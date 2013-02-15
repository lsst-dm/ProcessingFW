#!/usr/bin/env python

import sys
import os

from processingfw.pfwdefs import *
from processingfw.fwutils import *

import processingfw.pfwconfig as pfwconfig
import processingfw.pfwdb as pfwdb
from processingfw.pfwlog import log_pfw_event
from processingfw.pfwemail import send_email
    
def blockpost(argv = None):
    if argv is None:
        argv = sys.argv

    # open file to catch error messages about command line
    debugfh = open('blockpost.out', 'w', 0)
    sys.stdout = debugfh
    sys.stderr = debugfh
    
    print ' '.join(argv)  # print command line for debugging
    
    if len(argv) != 3:
        print 'Usage: blockpost.py configfile retval'
        debugfh.close()
        return(PF_EXIT_FAILURE)

    configfile = argv[1]
    retval = int(argv[2])

    fwdebug(3, 'PFWPOST_DEBUG', "configfile = %s" % configfile)
    fwdebug(3, 'PFWPOST_DEBUG', "retval = %s" % retval)

    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    fwdebug(3, 'PFWPOST_DEBUG', "done reading config file")
    blockname = config['blockname']
    

    # now that have more information, can rename output file
    fwdebug(0, 'PFWPOST_DEBUG', "getting new_log_name")
    new_log_name = config.get_filename('block', {PF_CURRVALS:
                                                  {'flabel': 'blockpost',
                                                   'fsuffix':'out'}})
#    new_log_name = "../%s/%s" % (blockname, new_log_name)
    fwdebug(0, 'PFWPOST_DEBUG', "new_log_name = %s" % new_log_name)

    debugfh.close()
    os.rename('blockpost.out', new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh
    
    log_pfw_event(config, blockname, 'blockpost', 'j', ['posttask', retval])

    dryrun = config[PF_DRYRUN]
    warningfile = config['warningfile']
    failedfile = config['failedfile']
    run = config['run']

    if retval:
        print "Block failed\nAlready sent email"
    elif convertBool(dryrun):
        print "dryrun = ", dryrun
        print "Sending dryrun email"
        msg1 = "%s:  In dryrun mode, block %s has finished successfully." % (run, blockname)
        msg2 = ""
        send_email(config, blockname, PF_EXIT_SUCCESS, "[DRYRUN]", msg1, msg2)
        retval = PF_EXIT_DRYRUN
    elif os.path.isfile(warningfile):
        print "JOBS_WARNINGS.txt exists.  Sending warning email"
        msg1 = "%s:  At least one job in block %s has status4 messages." % (run, blockname)
        msg2 = ""

        fh.open(warningfile, "r")
        lines = fh.readlines()
        fh.close()

        jobid = {}
        for j in lines:
            jobid[j.strip()] = True
        msg2 += "Check the following jobs:"
        for j in sorted(jobid.keys()):
            msg2 += "\t%s\n" % (j)
        fh.close()
        msg2 += "\n\n".getJobInfo(blockname)

        send_email(config, blockname, PF_WARNINGS, "[WARNINGS]", msg1, msg2)
        retval = PF_EXIT_SUCCESS
    elif retval == PF_EXIT_SUCCESS:
        print "Sending success email\n";
        msg1 = "%s:  block %s has finished successfully." % (run, blockname)
        #msg2 = "\n\n" + getJobInfo(blockname)
        msg2 = ""

        send_email(config, blockname, retval, "", msg1, msg2)
    else:
        print "Not sending block email"
        print "retval = ", retval
        if os.path.isfile(failedfile):
            print "%s exists, so email should have already been sent" % (failedfile)

    # Store values in DB and hist file 
    if convertBool(config[PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['des_services'], config['des_db_section'])
        dbh.update_block_end(config, retval)
    #logEvent(config, blockname, 'mngr', 'j', 'posttask', retval)

    if retval == PF_EXIT_SUCCESS:
        # Get ready for next block
        config.inc_blknum()
        config.inc_jobnum()
        config.save_file(configfile)
        print "new blknum = ", config[PF_BLKNUM]
        print "number of blocks = ", len(config.block_array)
        if int(config[PF_BLKNUM]) <= len(config.block_array):   # blknum is 1-based
            retval = PF_NEXTBLOCK
            print "modified retval to %s" % retval
    else:
        retval = PF_EXIT_FAILURE
    
    fwdebug(3, 'PFWPOST_DEBUG', "Returning retval = %s" % retval)
    print "type(retval) =",type(retval)
    print "blockpost done" 
    debugfh.close()
    return(int(retval))

if __name__ == "__main__":
    realstdout = sys.stdout
    realstderr = sys.stderr
    exitcode = blockpost(sys.argv)
    sys.stdout = realstdout
    sys.stderr = realstderr
    fwdebug(3, 'PFWPOST_DEBUG', "Exiting with = %s" % exitcode)
    fwdebug(3, 'PFWPOST_DEBUG', "type of exitcode = %s" % type(exitcode))
    sys.exit(exitcode)
