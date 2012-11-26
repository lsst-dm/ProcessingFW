#!/usr/bin/env python

import subprocess
import argparse
import sys
import os
import time
import processingfw.pfwutils as pfwutils
import filemgmt.cache as cache
import intgutils.wclutils as wclutils


VERSION = '$Rev$'

def setupwrapper(inputwcl, logfilename, useDB=False):
    """ Create output directories, get files from cache, and other setup work """

    # make directory for log file
    logdir = os.path.dirname(logfilename)
    if not os.path.exists(logdir):
        os.makedirs(logdir)

    # make directory for outputwcl
    outputwclfile = inputwcl['wrapper']['outputfile']
    outputwcldir = os.path.dirname(outputwclfile)
    if not os.path.exists(outputwcldir):
        os.makedirs(outputwcldir)

    # make directories for output files, cache input files
    for sect in inputwcl.keys():
        if sect.startswith('exec_'):
            if 'children' in inputwcl[sect]:
                for child in pfwutils.pfwsplit(inputwcl[sect]['children']):
                    childnames = pfwutils.get_wcl_value(child+'.filename', inputwcl)
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
                    infile_names = pfwutils.get_wcl_value(parent+'.filename', inputwcl)
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


    # create wrapper db entry
    if useDB: 
        import processingfw.pfwdb as pfwdb
        dbh = pfwdb.PFWDB()
        inputwcl['wrapperid'] = dbh.insert_wrapper(inputwcl)
        print "wrapperid =", inputwcl['wrapperid']

    return(0)



def runwrapper(wrappercmd, logfilename, useQCF=False, bufsize = 5000):
    print "wrappercmd = ", wrappercmd
    print "logfilename = ", logfilename
    print "useQCF = ", useQCF

    logfh = open(logfilename, 'w', 0)

    processWrap = subprocess.Popen(wrappercmd.split(),
                                   shell=False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
    if useQCF:
        cmdQCF = "qcf_controller.pl -wrapperInstanceId %s -execnames %s"
        processQCF = subprocess.Popen(cmdQCF.split(),
                                      shell=False,
                                      stdin=subprocess.PIPE,
                                      stderr=subprocess.STDOUT)

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

    if processWrap.returncode != 0:
        print "wrapper returned non-zero exit code"

    return processWrap.returncode

#def makepaths(pathsfile):
#    with open(pathsfile, 'r') as pathsfh:
#        line = pathsfh.readline()
#        while line:
#            thedir = line.strip()
#            if not os.path.exists(thedir):
#                os.makedirs(thedir)
#            line = pathsfh.readline()

def postwrapper(inputwcl, exitcode, useDB=False):
    if useDB: 
        import processingfw.pfwdb as pfwdb
        dbh = pfwdb.PFWDB()
        dbh.update_wrapper_end(inputwcl, exitcode)
    


def runtasks(taskfile, useDB=False, useQCF=False):
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

            if setupwrapper(inputwcl, logfile, useDB) == 0:
                exitcode = runwrapper(wrappercmd, logfile, useQCF)
                postwrapper(inputwcl, exitcode, useDB) 
                if exitcode:
                    print "Aborting due to non-zero exit code"
                    sys.exit(exitcode)
            line = tasksfh.readline()



def runjob(args): 
    """Run tasks inside single job"""
    runtasks(args.taskfile[0], args.useDB, args.useQCF)
        

def parseArgs(argv):
    parser = argparse.ArgumentParser(description='pfwrunjob.py')
    parser.add_argument('--useDB', action='store_true', default=False)
    parser.add_argument('--useQCF', action='store_true', default=False)
    parser.add_argument('--version', action='store_true', default=False)
    parser.add_argument('taskfile', nargs=1, action='store')

    args = parser.parse_args()

    if args.version:
        print VERSION
        sys.exit(0)

    return args

if __name__ == '__main__':
    sys.exit(runjob(parseArgs(sys.argv)))
