#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import subprocess
import os
import time
from coreutils.miscutils import *

def runwrapper(wrappercmd, logfilename, useQCF=False, bufsize = 5000):
    print "wrappercmd = ", wrappercmd
    print "logfilename = ", logfilename
    print "useQCF = ", useQCF

    logpath = os.path.dirname(logfilename)
    coremakedirs(logpath)

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

if __name__ == '__main__':
    print runwrapper("/usr/bin/env", "myfile.log", True)
