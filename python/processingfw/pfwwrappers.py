#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

import os
import errno
import stat

from intgutils.wclutils import write_wcl
import processingfw.pfwutils as pfwutils
import coreutils.miscutils as coremisc
import processingfw.pfwdefs as pfwdefs

def write_wrapper_wcl(config, filename, wrapperwcl):
    wcldir = os.path.dirname(filename)
    coremisc.coremakedirs(wcldir)
    with open(filename, 'w', 0) as wclfh:
        write_wcl(wrapperwcl, wclfh, True, 4)


def write_workflow_taskfile(config, jobnum, tasks):
    taskfile = config.get_filename('jobtasklist', {pfwdefs.PF_CURRVALS:{'jobnum':jobnum},'required': True, 'interpolate': True})
    tjpad = pfwutils.pad_jobnum(jobnum)
    coremisc.coremakedirs(tjpad)
    with open("%s/%s" % (tjpad, taskfile), 'w') as tasksfh:
        for task in sorted(tasks, key=lambda singletask: int(singletask[0])):
            tasksfh.write("%s, %s, %s, %s, %s\n" % (task[0], task[1], task[2], task[3], task[4]))
    return taskfile

