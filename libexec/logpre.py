#!/usr/bin/env python
# $Id: logpre.py 41004 2015-12-11 15:49:41Z mgower $
# $Rev:: 41004                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2015-12-11 09:49:41 #$:  # Date of last commit.

""" Bookkeeping steps executed submit-side prior to certain submit-side tasks """

import sys
import os
import processingfw.pfwdefs as pfwdefs
import despymisc.miscutils as miscutils
from processingfw.pfwlog import log_pfw_event
import processingfw.pfwconfig as pfwconfig

def logpre(argv=None):
    """ Program entry point """
    if argv is None:
        argv = sys.argv

    default_log = 'logpre.out'
    debugfh = open(default_log, 'w', 0)
    sys.stdout = debugfh
    sys.stderr = debugfh

    print ' '.join(sys.argv) # command line for debugging

    if len(argv) < 5:
        print 'Usage: logpre configfile block subblocktype subblock'
        debugfh.close()
        return pfwdefs.PF_EXIT_FAILURE

    configfile = sys.argv[1]
    blockname = sys.argv[2]    # could also be uberctrl
    subblocktype = sys.argv[3]
    subblock = sys.argv[4]

    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})

    # now that have more information, can rename output file
    miscutils.fwdebug_print("getting new_log_name")
    blockname = config.getfull('blockname')
    blkdir = config.getfull('block_dir')
    new_log_name = config.get_filename('block',
                                       {pfwdefs.PF_CURRVALS: {'subblock': subblock,
                                                              'flabel': '${subblock}_logpre',
                                                              'fsuffix':'out'}})
    new_log_name = "%s/%s" % (blkdir, new_log_name)
    miscutils.fwdebug_print("new_log_name = %s" % new_log_name)
    debugfh.close()

    os.chmod(default_log, 0666)
    os.rename(default_log, new_log_name)

    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh


    log_pfw_event(config, blockname, subblock, subblocktype, ['pretask'])

    print "logpre done"
    debugfh.close()
    return pfwdefs.PF_EXIT_SUCCESS

if __name__ == "__main__":
    sys.exit(logpre(sys.argv))
