#!/usr/bin/env python

import re
import subprocess
import argparse
import sys
import os
import time

from processingfw.fwutils import *
from processingfw.pfwdefs import *

import processingfw.pfwutils as pfwutils
import processingfw.pfwdb as pfwdb
import filemgmt.cache as cache
import intgutils.wclutils as wclutils


VERSION = '$Rev$'

# assumes exit code for version is 0
def getVersion(execname, verflag, verpat):
    """run command with version flag and parse output for version"""

    ver = None
    print "getVersion", execname, verflag, verpat
    cmd = "%s %s" % (execname, verflag)
    print "cmd> ", cmd
    process = subprocess.Popen(cmd.split(),
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
    process.wait()
    out = process.communicate()[0]
    print "output = ", out
    if process.returncode != 0:
        print "Warning:  problem when trying to get version"
        print "\tcmd> ",cmd
        print out
        ver = None
    else:
        # parse output with verpat
        try:
            print "before match verpat=",verpat
            print "before match out=",out
            m = re.search(verpat, out)
            if m:
                ver = m.group(1)
                print "Found version: ", ver
            else:
                print "Didn't find version"
        except Exception as err:
            #print type(err)
            ver = None 
            fwdie("Exception from re.match.  Didn't find version: %s" % err)

    return ver


def setupwrapper(inwcl, iwfilename, logfilename, useDB=False):
    """ Create output directories, get files from cache, and other setup work """

    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    # make directory for log file
    logdir = os.path.dirname(logfilename)
    if not os.path.exists(logdir):
        os.makedirs(logdir)

    # make directory for outputwcl
    outputwclfile = inwcl[IW_WRAPSECT]['outputwcl']
    outputwcldir = os.path.dirname(outputwclfile)
    if len(outputwcldir) > 0:
        if not os.path.exists(outputwcldir):
            os.makedirs(outputwcldir)
    else:
        print "0 length directory for outputwcl"

    dbh = None
    if useDB:
        dbh = pfwdb.PFWDB()
        inwcl['dbids'] = {}
        inwcl['wrapperid'] = dbh.insert_wrapper(inwcl, iwfilename)
        pfw_file_metadata = {}
        pfw_file_metadata['file_1'] = {'filename' : iwfilename, 
                                       'filetype' : 'wcl'}
        dbh.ingest_file_metadata(pfw_file_metadata, inwcl['filetype_metadata'])

    else:
        inwcl['wrapperid'] = -1
        inwcl['dbids'] = {}


    # make directories for output files, cache input files
    fwdebug(3, "PFWRUNJOB_DEBUG", "section loop beg")
    execnamesarr = [inwcl['wrapname']]
    outfiles = {}
    for sect in sorted(inwcl.keys()):
        fwdebug(3, "PFWRUNJOB_DEBUG", "section %s" % sect)
        if re.search("^%s\d+$" % IW_EXECPREFIX, sect):
            if 'execname' not in inwcl[sect]:
                print "Missing execname in input wcl.  sect =", sect
                print "inwcl[sect] = ", wclutils.write_wcl(inwcl[sect])
                
                
            execname = inwcl[sect]['execname']
            execnamesarr.append(execname)
            if IW_OUTPUTS in inwcl[sect]:
                for outfile in fwsplit(inwcl[sect][IW_OUTPUTS]):
                    outfiles[outfile] = True
                    fullnames = pfwutils.get_wcl_value(outfile+'.fullname', inwcl)
                    print "fullnames = ", fullnames
                    if '$RNMLST{' in fullnames:
                        m = re.search("\$RNMLST{\${(.+)},(.+)}", fullnames)
                        if m:
                            print "Found rnmlst"
                            print m.group(1)
                        pattern = pfwutils.get_wcl_value(m.group(1), inwcl)
                        print pattern
                    else:
                        outfile_names = fwsplit(fullnames)
                        for outfile in outfile_names:
                            outfile_dir = os.path.dirname(outfile)
                            if len(outfile_dir) > 0:
                                if not os.path.exists(outfile_dir):
                                    os.makedirs(outfile_dir)
                            else:
                                print "0 length directory for output file:", outfile
            else:
                print "Note: 0 output files (%s) in exec section %s" % (IW_OUTPUTS, sect)

            if IW_INPUTS in inwcl[sect]:
                files2get = {}
                for infile in fwsplit(inwcl[sect][IW_INPUTS]):
                    infile_names = pfwutils.get_wcl_value(infile+'.fullname', inwcl)
                    infile_names = fwsplit(infile_names)
                    for inname in infile_names:
                        if not os.path.exists(inname) and not infile in outfiles:
                            files2get[inname] = True
                problemfiles = cache.get_from_cache(files2get.keys())
                if len(problemfiles) != 0:
                    print "Error: had problems getting input files from cache"
                    print "\t", problemfiles
                    return(len(problemfiles))
            else:
                print "Note: 0 inputs (%s) in exec section %s" % (IW_INPUTS, sect)

            if IW_EXEC_DEF in inwcl:
                if execname.lower() in inwcl[IW_EXEC_DEF]:    # might be a function or just missing
                    if ( 'version_flag' in inwcl[IW_EXEC_DEF][execname.lower()]
                       and 'version_pattern' in inwcl[IW_EXEC_DEF][execname.lower()] ):
                        verflag = inwcl[IW_EXEC_DEF][execname.lower()]['version_flag']
                        verpat = inwcl[IW_EXEC_DEF][execname.lower()]['version_pattern']

                        inwcl[sect]['version'] = getVersion(execname, verflag, verpat)
                        print "inwcl[sect]['version']", sect, inwcl[sect]['version']

            if useDB: 
                if 'execnum' not in inwcl[sect]:
                    result = re.match('%s(\d+)' % IW_EXECPREFIX, sect)
                    execnum = result.group(1)
                    inwcl[sect]['execnum'] = execnum
                inwcl['dbids'][sect] = dbh.insert_exec(inwcl, sect) 

    inwcl['execnames'] = ','.join(execnamesarr)
    fwdebug(3, "PFWRUNJOB_DEBUG", "section loop end")

    fwdebug(3, "PFWRUNJOB_DEBUG", "END")

    return(0)



def runwrapper(wrappercmd, logfilename, wrapperid, execnames, bufsize=5000, useQCF=False):
    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")
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
                fwdie("Unexpected error: %s" % sys.exc_info()[0], FW_EXIT_FAILURE)
                
    except:
        fwdie("Unexpected error: %s" % sys.exc_info()[0], FW_EXIT_FAILURE)

    if processWrap.returncode != 0:
        print "wrapper returned non-zero exit code"

    fwdebug(3, "PFWRUNJOB_DEBUG", "END")
    return processWrap.returncode


def postwrapper(inwcl, logfile, exitcode, useDB=False):
    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    if not os.path.isfile(logfile):
        logfile = None

    outputwclfile = inwcl[IW_WRAPSECT]['outputwcl']
    outputwcl = None
    if not os.path.isfile(outputwclfile):
        outputwclfile = None
    else:
        outwclfh = open(outputwclfile, 'r')
        outputwcl = wclutils.read_wcl(outwclfh)

    # make 
    if useDB:
        dbh = pfwdb.PFWDB()
        dbh.update_wrapper_end(inwcl, outputwclfile, logfile, exitcode)
        if outputwcl is not None:
            for sect in outputwcl.keys():
                if re.search("^%s\d+$" % OW_EXECPREFIX, sect):
                    dbh.update_exec_end(outputwcl[sect], inwcl['dbids'][sect], exitcode)
            if OW_METASECT in outputwcl:
                dbh.ingest_file_metadata(outputwcl[OW_METASECT], inwcl['filetype_metadata'])
            pfw_file_metadata = {}
            pfw_file_metadata['file_1'] = {'filename' : outputwclfile, 
                                           'filetype' : 'wcl'}
            pfw_file_metadata['file_2'] = {'filename' : logfile, 
                                           'filetype' : 'log'}
            dbh.ingest_file_metadata(pfw_file_metadata, inwcl['filetype_metadata'])

            if OW_PROVSECT in outputwcl and len(outputwcl[OW_PROVSECT].keys()) > 0:
                dbh.ingest_provenance(outputwcl[OW_PROVSECT], inwcl['dbids'])

    fwdebug(3, "PFWRUNJOB_DEBUG", "END")
    



def runtasks(taskfile, useDB=False, jobwcl={}, useQCF=False):
    # run each wrapper execution sequentially
    with open(taskfile, 'r') as tasksfh:
        # for each task
        line = tasksfh.readline()
        while line:
            (wrapnum, wrapname, wclfile, logfile) = fwsplit(line.strip())
            wrappercmd = "%s --input=%s" % (wrapname, wclfile)
            print "%04d: wrappercmd: %s" % (int(wrapnum), wrappercmd)

            if not os.path.exists(wclfile):
                print "Error: input wcl file does not exist (%s)" % wclfile
                return(1)

            with open(wclfile, 'r') as wclfh:
                inwcl = wclutils.read_wcl(wclfh)
            inwcl.update(jobwcl)

            exitcode = setupwrapper(inwcl, wclfile, logfile, useDB)
            if exitcode == 0:
                exitcode = runwrapper(wrappercmd, 
                                      logfile,
                                      inwcl['wrapperid'], 
                                      inwcl['execnames'],
                                      5000,
                                      useQCF)
                postwrapper(inwcl, logfile, exitcode, useDB) 
 
                # to give me full wcl (input + job + values created
                # at runtime to run against dummy output wcl
                with open(wclfile+'.mmg', 'w') as wclfh:
                    wclutils.write_wcl(inwcl, wclfh, True, 4)

                sys.stdout.flush()
                sys.stderr.flush()
                if exitcode:
                    print "Aborting due to non-zero exit code"
                    return(exitcode)
            else:
                print "Aborting due to problems in setup wrapper"
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
            useDB = convertBool(wcl['usedb'])

        if 'useqcf' in wcl:
            useQCF = convertBool(wcl['useqcf'])

    if useDB:
        if 'des_services' in wcl:
            os.environ['DES_SERVICES'] = wcl['des_services']
        if 'des_db_section' in wcl:
            os.environ['DES_DB_SECTION'] = wcl['des_db_section']

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
