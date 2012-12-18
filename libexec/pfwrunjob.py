#!/usr/bin/env python

import re
import subprocess
import argparse
import sys
import os
import time
import processingfw.pfwutils as pfwutils
import processingfw.pfwdb as pfwdb
import filemgmt.cache as cache
import intgutils.wclutils as wclutils


VERSION = '$Rev$'

def setupwrapper(inputwcl, iwfilename, logfilename, useDB=False):
    """ Create output directories, get files from cache, and other setup work """

    pfwutils.debug(3, "PFWRUNJOB_DEBUG", "BEG")

    # make directory for log file
    logdir = os.path.dirname(logfilename)
    if not os.path.exists(logdir):
        os.makedirs(logdir)

    # make directory for outputwcl
    outputwclfile = inputwcl['wrapper']['outputwcl']
    outputwcldir = os.path.dirname(outputwclfile)
    if not os.path.exists(outputwcldir):
        os.makedirs(outputwcldir)

    dbh = None
    if useDB:
        dbh = pfwdb.PFWDB()
        inputwcl['wrapperid'] = dbh.insert_wrapper(inputwcl, iwfilename)
        print "wrapperid =", inputwcl['wrapperid']


    # make directories for output files, cache input files
    pfwutils.debug(3, "PFWRUNJOB_DEBUG", "section loop beg")
    execnamesarr = [inputwcl['wrapname']]
    for sect in inputwcl.keys():
        pfwutils.debug(3, "PFWRUNJOB_DEBUG", "section %s" % sect)
        if sect.startswith('exec_'):
            execnamesarr.append(inputwcl[sect]['execname'])
            if 'children' in inputwcl[sect]:
                for child in pfwutils.pfwsplit(inputwcl[sect]['children']):
                    childnames = pfwutils.get_wcl_value(child+'.fullname', inputwcl)
                    outfile_names = pfwutils.pfwsplit(childnames)
                    for outfile in outfile_names:
                        outfile_dir = os.path.dirname(outfile)
                    if not os.path.exists(outfile_dir):
                            os.makedirs(outfile_dir)
            else:
                print "Note: 0 children in exec section", sect

            if 'parents' in inputwcl[sect]:
                files2get = {}
                for parent in pfwutils.pfwsplit(inputwcl[sect]['parents']):
                    infile_names = pfwutils.get_wcl_value(parent+'.fullname', inputwcl)
                    infile_names = pfwutils.pfwsplit(infile_names)
                    for inname in infile_names:
                        if not os.path.exists(inname):
                            files2get[inname] = True
                problemfiles = cache.get_from_cache(files2get.keys())
                if len(problemfiles) != 0:
                    print "Error: had problems getting input files from cache"
                    print "\t", problemfiles
                    return(len(problemfiles))
            else:
                print "Note: 0 parents in exec section", sect

            if useDB: 
                if 'execnum' not in inputwcl[sect]:
                    result = re.match('exec_(\d+)', sect)
                    execnum = result.group(1)
                    inputwcl[sect]['execnum'] = execnum
                inputwcl[sect]['execid'] = dbh.insert_exec(inputwcl, sect) 

    inputwcl['execnames'] = ','.join(execnamesarr)
    pfwutils.debug(3, "PFWRUNJOB_DEBUG", "section loop end")

    pfwutils.debug(3, "PFWRUNJOB_DEBUG", "END")

    return(0)



def runwrapper(wrappercmd, logfilename, wrapperid, execnames, bufsize=5000, useQCF=False):
    pfwutils.debug(3, "PFWRUNJOB_DEBUG", "BEG")
    print "wrappercmd = ", wrappercmd
    print "logfilename = ", logfilename
    print "useQCF = ", useQCF

    logfh = open(logfilename, 'w', 0)

    processWrap = subprocess.Popen(wrappercmd.split(),
                                   shell=False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
    if useQCF:
        cmdQCF = "qcf_controller.pl -wrapperInstanceId %s -execnames %s" % (wrapperid, execnames)
        processQCF = subprocess.Popen(cmdQCF.split(),
                                      shell=False,
                                      stdin=subprocess.PIPE,
                                      stderr=subprocess.STDOUT)

    try:
        buf = os.read(processWrap.stdout.fileno(), bufsize)
        while processWrap.poll() == None or len(buf) != 0:
            logfh.write(buf)
            if useQCF:
                processQCF.stdin.write(buf)
            buf = os.read(processWrap.stdout.fileno(), bufsize)

        logfh.close()
        if useQCF:
            processQCF.stdin.close()
            while processQCF.poll() == None:
                time.sleep(1)
            if processQCF.returncode != 0:
                print "QCF returned non-zero exit code"
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
        if useQCF:
            qcfpoll = processQCF.poll()
            if qcfpoll != None and qcfpoll != 0:
                if processWrap.poll() == None:
                    buf = os.read(processWrap.stdout.fileno(), bufsize)
                    while processWrap.poll() == None or len(buf) != 0:
                        logfh.write(buf)
                        buf = os.read(processWrap.stdout.fileno(), bufsize)

                    logfh.close()
            else:
                print "Unexpected error:", sys.exc_info()[0]
                raise
    except:
        print "Unexpected error:", sys.exc_info()[0]
        raise

    if processWrap.returncode != 0:
        print "wrapper returned non-zero exit code"

    pfwutils.debug(3, "PFWRUNJOB_DEBUG", "END")
    return processWrap.returncode


def postwrapper(inputwcl, jobwcl, logfile, exitcode, useDB=False):
    pfwutils.debug(3, "PFWRUNJOB_DEBUG", "BEG")

    if not os.path.isfile(logfile):
        logfile = None

    outputwclfile = inputwcl['wrapper']['outputwcl']
    outputwcl = None
    if not os.path.isfile(outputwclfile):
        outputwclfile = None
    else:
        outwclfh = open(outputwclfile, 'r')
        outputwcl = wclutils.read_wcl(outwclfh)

    # make 
    if useDB:
        dbh = pfwdb.PFWDB()
        dbh.update_wrapper_end(inputwcl, outputwclfile, logfile, exitcode)
        if outputwcl is not None:
            for sect in outputwcl.keys():
                if sect.startswith('exec_'):
                    dbh.update_exec_end(outputwcl[sect], inputwcl[sect]['execid'], exitcode)
            if 'outputfiles' in outputwcl:
                dbh.ingest_file_metadata(outputwcl['outputfiles'], jobwcl['filetype_metadata'])

    pfwutils.debug(3, "PFWRUNJOB_DEBUG", "END")
    



def runtasks(taskfile, useDB=False, jobwcl={}, useQCF=False):
    # run each wrapper execution sequentially
    with open(taskfile, 'r') as tasksfh:
        # for each task
        line = tasksfh.readline()
        while line:
            (wrapnum, wrapname, wclfile, logfile) = pfwutils.pfwsplit(line.strip())
            wrappercmd = "%s --input=%s" % (wrapname, wclfile)
            print "%04d: wrappercmd: %s" % (int(wrapnum), wrappercmd)

            if not os.path.exists(wclfile):
                print "Error: input wcl file does not exist (%s)" % wclfile
                return(1)

            with open(wclfile, 'r') as wclfh:
                inputwcl = wclutils.read_wcl(wclfh)
            inputwcl.update(jobwcl)

            if setupwrapper(inputwcl, wclfile, logfile, useDB) == 0:
                exitcode = runwrapper(wrappercmd, 
                                      logfile,
                                      inputwcl['wrapperid'], 
                                      inputwcl['execnames'],
                                      5000,
                                      useQCF)
                postwrapper(inputwcl, jobwcl, logfile, exitcode, useDB) 
                sys.stdout.flush()
                sys.stderr.flush()
                if exitcode:
                    print "Aborting due to non-zero exit code"
                    return(exitcode)
            line = tasksfh.readline()
    return(0)



def runjob(args): 
    """Run tasks inside single job"""

    useDB = False
    useQCF = False
    wcl = {}

    if args.config:
        with open(args.config, 'r') as wclfh:
            wcl = wclutils.read_wcl(wclfh) 
        if 'usedb' in wcl:
            if wcl['usedb'].lower() == 'true':
                useDB = True

        if 'useqcf' in wcl:
            if wcl['useqcf'].lower() == 'true':
                useQCF = True

    if useDB:
        dbh = pfwdb.PFWDB()
        dbh.insert_job(wcl)

    exitcode = runtasks(args.taskfile[0], useDB, wcl, useQCF)

    if useDB:
        dbh.update_job_end(wcl, exitcode)
        

def parseArgs(argv):
    parser = argparse.ArgumentParser(description='pfwrunjob.py')
    parser.add_argument('--version', action='store_true', default=False)
    parser.add_argument('--config', action='store')
    parser.add_argument('taskfile', nargs=1, action='store')

    args = parser.parse_args()

    if args.version:
        print VERSION
        sys.exit(0)

    return args

if __name__ == '__main__':
    print ' '.join(sys.argv)
    sys.exit(runjob(parseArgs(sys.argv)))
