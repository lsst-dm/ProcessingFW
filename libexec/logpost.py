#!/usr/bin/env python

import sys
import os
import processingfw.pfwconfig as pfwconfig
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
        return(pfwconfig.PfwConfig.FAILURE)

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

    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    debug(3, 'PFWPOST_DEBUG', "done reading config file")
    

    # now that have more information, rename output file
    debug(3, 'PFWPOST_DEBUG', "before get_filename")
    new_log_name = config.get_filename('block', {'currentvals': 
                                                  {'filetype': 'logpost_${subblock}', 
                                                   'subblock': subblock,
                                                   'suffix':'out'}})
    debug(3, 'PFWPOST_DEBUG', "new_log_name = %s" % new_log_name)
    debugfh.close()
    os.rename('logpost.out', new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh
    
    
    log_pfw_event(config, block, subblock, subblocktype, ['posttask', retval])
    
    # In order to continue, make pipelines dagman jobs exit with success status
    if 'pipelinesmngr' not in subblock:
        retval = pfwconfig.PfwConfig.SUCCESS
    
    # If error at non-manager level, send failure email
    if retval != pfwconfig.PfwConfig.SUCCESS and \
        'mngr' not in subblock:
        send_subblock_email(config, block, subblock, retval)
    
    debugfh.close()
    return(retval)

if __name__ == "__main__":
    sys.exit(logpost(sys.argv))
