#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

import os
import errno
import stat

from intgutils.wclutils import write_wcl
from coreutils.miscutils import *
from processingfw.pfwdefs import *

def write_wrapper_wcl(config, filename, wrapperwcl):
    wcldir = os.path.dirname(filename)
    coremakedirs(wcldir)
    with open(filename, 'w', 0) as wclfh:
        write_wcl(wrapperwcl, wclfh, True, 4)


def write_workflow_taskfile(config, jobnum, tasks):
    taskfile = config.get_filename('jobtasklist', {PF_CURRVALS:{'jobnum':jobnum},'required': True, 'interpolate': True})
    with open(taskfile, 'w', 0) as tasksfh:
        for task in sorted(tasks, key=lambda singletask: int(singletask[0])):
            tasksfh.write("%s, %s, %s, %s, %s\n" % (task[0], task[1], task[2], task[3], task[4]))
    return taskfile

