#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import os

import processingfw.pfwdefs as pfwdefs
import despymisc.miscutils as miscutils
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
        return(pfwdefs.PF_EXIT_FAILURE)

    configfile = argv[1]
    blockname = argv[2]
    subblocktype = argv[3]
    subblock = argv[4]
    retval = pfwdefs.PF_EXIT_FAILURE
    if len(argv) == 6:
        retval = int(sys.argv[5])

    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "configfile = %s" % configfile)
    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "block = %s" % blockname)
    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "subblock = %s" % subblock)
    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "retval = %s" % retval)

    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "done reading config file")
    if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['submit_des_services'], config['submit_des_db_section'])
        #dbh.update_blktask_end(config, "", subblock, retval)

    # now that have more information, rename output file
    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "before get_filename")
    blockname = config['blockname']
    blkdir = config['block_dir']
    new_log_name = config.get_filename('block', {pfwdefs.PF_CURRVALS: 
                                                  {'flabel': '${subblock}_logpost', 
                                                   'subblock': subblock,
                                                   'fsuffix':'out'}})
    new_log_name = "%s/%s" % (blkdir, new_log_name)
    miscutils.fwdebug(0, 'PFWPOST_DEBUG', "new_log_name = %s" % new_log_name)
     
    debugfh.close()
    os.chmod('logpost.out', 0666)
    os.rename('logpost.out', new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh
    
    
    log_pfw_event(config, blockname, subblock, subblocktype, ['posttask', retval])
    
    # In order to continue, make pipelines dagman jobs exit with success status
    #if 'pipelinesmngr' not in subblock:
    #    retval = pfwdefs.PF_EXIT_SUCCESS
    
#    # If error at non-manager level, send failure email
#    if retval != pfwdefs.PF_EXIT_SUCCESS and \
#        'mngr' not in subblock:
#        send_subblock_email(config, blockname, subblock, retval)
    
    if retval != pfwdefs.PF_EXIT_SUCCESS:
        miscutils.fwdebug(0, 'PFWPOST_DEBUG', "Setting failure retval")
        retval = pfwdefs.PF_EXIT_FAILURE     
    
    miscutils.fwdebug(0, 'PFWPOST_DEBUG', "Returning retval = %s" % retval)
    miscutils.fwdebug(0, 'PFWPOST_DEBUG', "logpost done")
    debugfh.close()
    return(int(retval))

if __name__ == "__main__":
    realstdout = sys.stdout
    realstderr = sys.stderr
    exitcode = logpost(sys.argv)
    sys.stdout = realstdout
    sys.stderr = realstderr
    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "Exiting with = %s" % exitcode)
    sys.exit(exitcode)
