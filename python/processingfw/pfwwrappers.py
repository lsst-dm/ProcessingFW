#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

import os
import errno
import stat

from intgutils.wclutils import write_wcl
from processingfw.pfwdefs import *

def write_wrapper_wcl(config, filename, wrapperwcl):
    wcldir = os.path.dirname(filename)
    if not os.path.exists(wcldir):  # some parallel filesystems really don't like 
                                    # trying to make directory if it already exists
        try:
            os.makedirs(wcldir)
        except OSError as exc:      # go ahead and check for race condition
            if exc.errno == errno.EEXIST:
                pass
            else: 
                fwdie("Error making directory wcldir: %s" % exc, PFW_EXIT_FAILURE)

    with open(filename, 'w', 0) as wclfh:
        write_wcl(wrapperwcl, wclfh, True, 4)


def write_workflow_taskfile(config, tasks):
    taskfile = config.get_filename('jobtasklist', {'required': True, 'interpolate': True})
    with open(taskfile, 'w', 0) as tasksfh:
        for task in sorted(tasks, key=lambda singletask: int(singletask[0])):
            tasksfh.write("%s, %s, %s, %s\n" % (task[0], task[1], task[2], task[3]))
    return taskfile


#def write_workflow_script(config, tasks):
#    (found, scriptfilename) = config.search('runscript', {'required': True, 'interpolate': True})
#    pipeline = config['pipeline']
#    pipever = config['pipever']
#
#    scriptstr = """#!/usr/bin/env python
#
#import sys
#from processingfw.pfwrunwrapper import runwrapper
#from processingfw.pfwutils import pfwsplit
##import eups.setupcmd
#
#
#def runTasks(taskfile, useQCF=False):
#    with open(taskfile, 'r', 0) as tasksfh
#    tasks = %s
#    for task in tasks:
#        wrappercmd = task[0] + " --input=" + task[1]
#        exitcode = pfwrunwrapper.runwrapper(wrappercmd, task[2])
#        if exitcode:
#            print "Aborting due to non-zero exit code"
#            sys.exit(exitcode)
#
#if __name__ == '__main__':
#    #setup = eups.setupcmd.EupsSetup(['%s', '%s', '-q'], 'setup')
#    #status = setup.run()
#    #if status:
#    #    print "Aborting due to non-zero eups setup exit code"
#    #    sys.exit(status)
#    runTasks()
#""" % (str(tasks), pipeline, pipever)
#    with open(scriptfilename, 'w', 0) as scriptfh:
#        scriptfh.write(scriptstr)
#    os.chmod(scriptfilename, stat.S_IRWXU | stat.S_IRWXG)

