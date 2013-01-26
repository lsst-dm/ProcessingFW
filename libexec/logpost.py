#!/usr/bin/env python

import sys
import os
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwdb as pfwdb
from processingfw.pfwdefs import *
from processingfw.pfwlog import log_pfw_event
from processingfw.pfwemail import send_subblock_email
from processingfw.pfwutils import debug
    
    
def logpost(argv = None):
    if argv is None:
        argv = sys.argv

    # open file to catch error messages about command line
    debugfh = open('logpost.out', 'w', 0)
    sys.stdout = debugfh
    sys.stderr = debugfh
    
    print ' '.join(argv)  # print command line for debugging
    
    if len(argv) < 5:
        print 'Usage: logpost configfile block subblocktype subblock retval'
        debugfh.close()
        return(PF_FAILURE)

    configfile = argv[1]
    block = argv[2]
    subblocktype = argv[3]
    subblock = argv[4]
    retval = ''
    if len(argv) == 6:
        retval = sys.argv[5]

    debug(3, 'PFWPOST_DEBUG', "configfile = %s" % configfile)
    debug(3, 'PFWPOST_DEBUG', "block = %s" % block)
    debug(3, 'PFWPOST_DEBUG', "subblock = %s" % subblock)
    debug(3, 'PFWPOST_DEBUG', "retval = %s" % retval)

    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    debug(3, 'PFWPOST_DEBUG', "done reading config file")
    
    dbh = pfwdb.PFWDB()
    dbh.update_blktask_end(config, "", subblock, retval)

    # now that have more information, rename output file
    debug(3, 'PFWPOST_DEBUG', "before get_filename")
    new_log_name = config.get_filename('block', {PF_CURRVALS: 
                                                  {'flabel': 'logpost_${subblock}', 
                                                   'subblock': subblock,
                                                   'fsuffix':'out'}})
    debug(3, 'PFWPOST_DEBUG', "new_log_name = %s" % new_log_name)
    debugfh.close()
    os.rename('logpost.out', new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh
    
    
    log_pfw_event(config, block, subblock, subblocktype, ['posttask', retval])
    
    # In order to continue, make pipelines dagman jobs exit with success status
    #if 'pipelinesmngr' not in subblock:
    #    retval = PF_SUCCESS
    
#    # If error at non-manager level, send failure email
#    if retval != PF_SUCCESS and \
#        'mngr' not in subblock:
#        send_subblock_email(config, block, subblock, retval)
    
    
    debug(3, 'PFWPOST_DEBUG', "Returning retval = %s" % retval)
    print type(retval)
    debugfh.close()
    return(int(retval))

if __name__ == "__main__":
    realstdout = sys.stdout
    realstderr = sys.stderr
    exitcode = logpost(sys.argv)
    sys.stdout = realstdout
    sys.stderr = realstderr
    debug(3, 'PFWPOST_DEBUG', "Exiting with = %s" % exitcode)
    sys.exit(exitcode)
