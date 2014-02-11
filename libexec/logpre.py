#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import os
from processingfw.pfwdefs import *
from coreutils.miscutils import *
from processingfw.pfwlog import log_pfw_event
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwdb as pfwdb

def logpre(argv = None):
    if argv is None:
        argv = sys.argv

    debugfh = open('logpre.out', 'w', 0) 
    sys.stdout = debugfh
    sys.stderr = debugfh
    
    print ' '.join(sys.argv) # command line for debugging
    
    if len(argv) < 5:
        print 'Usage: logpre configfile block subblocktype subblock'
        debugfh.close()
        return(PF_EXIT_FAILURE)

    configfile = sys.argv[1]
    blockname = sys.argv[2]    # could also be uberctrl
    subblocktype = sys.argv[3]
    subblock = sys.argv[4]
    
    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})

    if convertBool(config[PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['submit_des_services'], config['submit_des_db_section'])
        #dbh.insert_blktask(config, "", subblock)

    # now that have more information, can rename output file
    fwdebug(0, 'PFWPOST_DEBUG', "getting new_log_name")
    blockname = config['blockname']
    new_log_name = config.get_filename('block', {PF_CURRVALS:
                                                    {'flabel': '${subblock}_logpre',
                                                     'subblock': subblock,
                                                     'fsuffix':'out'}})
    new_log_name = "../%s/%s" % (blockname, new_log_name)
    fwdebug(0, 'PFWPOST_DEBUG', "new_log_name = %s" % new_log_name)
    debugfh.close()

    os.rename('logpre.out', new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh
    
    
    log_pfw_event(config, blockname, subblock, subblocktype, ['pretask'])
    
    print "logpre done"
    debugfh.close()
    return(PF_EXIT_SUCCESS)

if __name__ == "__main__":
    sys.exit(logpre(sys.argv))
