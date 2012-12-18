#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import os
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
        return(pfwconfig.PfwConfig.FAILURE)

    configfile = sys.argv[1]
    block = sys.argv[2]
    subblocktype = sys.argv[3]
    subblock = sys.argv[4]
    
    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    print config['reqnum']

    dbh = pfwdb.PFWDB()
    dbh.insert_blktask(config, "", subblock)

    # now that have more information, can rename output file
    new_log_name = config.get_filename('block', {'currentvals':
                                                    {'filetype': 'logpre_${subblock}',
                                                     'subblock': subblock,
                                                     'suffix':'out'}})
    print "new_log_name=",new_log_name
    debugfh.close()
    os.rename('logpre.out', new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh
    
    
    log_pfw_event(config, block, subblock, subblocktype, ['pretask'])
    
    debugfh.close()
    return(int(pfwconfig.PfwConfig.SUCCESS))

if __name__ == "__main__":
    sys.exit(logpre(sys.argv))
