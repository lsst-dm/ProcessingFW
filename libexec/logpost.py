#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import os
from processingfw.pfwdefs import *
from coreutils.miscutils import *

import processingfw.pfwconfig as pfwconfig
import processingfw.pfwdb as pfwdb
from processingfw.pfwlog import log_pfw_event
from processingfw.pfwemail import send_subblock_email


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
        return(PF_EXIT_FAILURE)

    configfile = argv[1]
    blockname = argv[2]
    subblocktype = argv[3]
    subblock = argv[4]
    retval = PF_EXIT_FAILURE
    if len(argv) == 6:
        retval = int(sys.argv[5])

    fwdebug(3, 'PFWPOST_DEBUG', "configfile = %s" % configfile)
    fwdebug(3, 'PFWPOST_DEBUG', "block = %s" % blockname)
    fwdebug(3, 'PFWPOST_DEBUG', "subblock = %s" % subblock)
    fwdebug(3, 'PFWPOST_DEBUG', "retval = %s" % retval)

    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    fwdebug(3, 'PFWPOST_DEBUG', "done reading config file")
    if convertBool(config[PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['des_services'], config['des_db_section'])
        #dbh.update_blktask_end(config, "", subblock, retval)

    # now that have more information, rename output file
    fwdebug(3, 'PFWPOST_DEBUG', "before get_filename")
    blockname = config['blockname']
    new_log_name = config.get_filename('block', {PF_CURRVALS: 
                                                  {'flabel': '${subblock}_logpost', 
                                                   'subblock': subblock,
                                                   'fsuffix':'out'}})
    new_log_name = "../%s/%s" % (blockname, new_log_name)
    fwdebug(0, 'PFWPOST_DEBUG', "new_log_name = %s" % new_log_name)
     
    debugfh.close()
    os.rename('logpost.out', new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh
    
    
    log_pfw_event(config, blockname, subblock, subblocktype, ['posttask', retval])
    
    # In order to continue, make pipelines dagman jobs exit with success status
    #if 'pipelinesmngr' not in subblock:
    #    retval = PF_EXIT_SUCCESS
    
#    # If error at non-manager level, send failure email
#    if retval != PF_EXIT_SUCCESS and \
#        'mngr' not in subblock:
#        send_subblock_email(config, blockname, subblock, retval)
    
    if retval != PF_EXIT_SUCCESS:
        fwdebug(0, 'PFWPOST_DEBUG', "Setting failure retval")
        retval = PF_EXIT_FAILURE     
    
    fwdebug(0, 'PFWPOST_DEBUG', "Returning retval = %s" % retval)
    fwdebug(0, 'PFWPOST_DEBUG', "logpost done")
    debugfh.close()
    return(int(retval))

if __name__ == "__main__":
    realstdout = sys.stdout
    realstderr = sys.stderr
    exitcode = logpost(sys.argv)
    sys.stdout = realstdout
    sys.stderr = realstderr
    fwdebug(3, 'PFWPOST_DEBUG', "Exiting with = %s" % exitcode)
    sys.exit(exitcode)
