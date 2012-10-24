#!/usr/bin/env python

import subprocess
import sys
import os
import time
import processingfw.pfwutils as pfwutils
import filemgmt.cache as cache
import intgutils.wclutils as wclutils


def setupwrapper(inputwclfile, logfilename):
    """ Create output directories, get files from cache, and other setup work """

    # make directory for log file
    logdir = os.path.dirname(logfilename)
    if not os.path.exists(logdir):
        os.makedirs(logdir)

    if not os.path.exists(inputwclfile):
        print "Error: input wcl file does not exist (%s)" % inputwclfile
        return(1)

    with open(inputwclfile, 'r') as wclfh:
        inputwcl = wclutils.read_wcl(wclfh)

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
        cmdQCF = "./myQCF"
        processQCF = subprocess.Popen(cmdQCF.split(),
                                      shell=False,
                                      stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE,
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

def runtasks(taskfile):
    # run each wrapper execution sequentially
    with open(taskfile, 'r') as tasksfh:
        line = tasksfh.readline()
        while line:
            task = pfwutils.pfwsplit(line.strip())
            wrappercmd = task[0] + " --input=" + task[1]
            print "Wrappercmd:", wrappercmd

            if setupwrapper(task[1], task[2]) == 0:
                exitcode = runwrapper(wrappercmd, task[2])
                if exitcode:
                    print "Aborting due to non-zero exit code"
                    sys.exit(exitcode)
            line = tasksfh.readline()


def runjob(taskfile): 
    """Run tasks inside single job"""

    # untar wcltar
#    pfwutils.untar_dir(wcltar, '.')
    runtasks(taskfile)
        

if __name__ == '__main__':
#    print runwrapper("/usr/bin/env", "myfile.log", True)
    if len(sys.argv) != 2: 
        raise Exception("Usage: pfwrunjob.py taskslist")

    runjob(sys.argv[1])
