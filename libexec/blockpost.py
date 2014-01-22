#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import os

from processingfw.pfwdefs import *
from processingfw.pfwutils import *
from coreutils.miscutils import *

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
    new_log_name = "../%s/%s" % (blockname, new_log_name)
    fwdebug(0, 'PFWPOST_DEBUG', "new_log_name = %s" % new_log_name)

    debugfh.close()
    os.rename('blockpost.out', new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh

    os.chdir("../%s" % blockname)
    
    log_pfw_event(config, blockname, 'blockpost', 'j', ['posttask', retval])

    dryrun = config[PF_DRYRUN]
    warningfile = config['warningfile']
    failedfile = config['failedfile']
    run = config['run']
    reqnum = config.search(REQNUM, {'interpolate': True})[1]
    unitname = config.search(UNITNAME, {'interpolate': True})[1]
    attnum = config.search(ATTNUM, {'interpolate': True})[1]
    blknum = int(config.search(PF_BLKNUM, {'interpolate': True})[1])

    msg2 = ""
    dbh = None
    if convertBool(config[PF_USE_DB_OUT]): 
        try:
            print "\n\nChecking job status from pfw_job table in DB (%s is success)" % PF_EXIT_SUCCESS
            dbh = pfwdb.PFWDB(config['des_services'], config['des_db_section'])
            jobinfo = dbh.get_job_info({'reqnum':reqnum, 'unitname': unitname, 'attnum': attnum, 'blknum': blknum})
            wrapinfo = dbh.get_wrapper_info(reqnum, unitname, attnum, blknum)
            dbh.close()
    
            job_byblk = index_job_info(jobinfo)
            print job_byblk
            wrap_byjob, wrap_bymod = index_wrapper_info(wrapinfo)

    
            lastwraps = []
            for jobnum,jobdict in sorted(job_byblk[blknum].items()):
                if jobnum not in wrap_byjob:
                    print "\t%06d No wrapper instances" % jobnum
                #print "wrapnum in job =", wrap_byjob[jobnum].keys()
                maxwrap = max(wrap_byjob[jobnum].keys())
                modname = wrap_byjob[jobnum][maxwrap]['modname']
                jobkeys = ""
                if jobdict['jobkeys'] is not None:
                    jobkeys = jobdict['jobkeys']
                    print jobkeys, type(jobkeys)

                msg2 += "\t%04d %d/%d  %s (%s)" % (jobnum, len(wrap_byjob[jobnum]), jobdict['numexpwrap'], modname, jobkeys)
                if jobdict['status'] is None:
                    msg2 += " FAIL - NULL status"
                    lastwraps.append(wrap_byjob[jobnum][maxwrap]['id'])
                    retval = PF_EXIT_FAILURE
                elif jobdict['status'] != PF_EXIT_SUCCESS:
                    lastwraps.append(wrap_byjob[jobnum][maxwrap]['id'])
                    msg2 += " FAIL"
                    retval = PF_EXIT_FAILURE

                msg2 += '\n'
        except Exception, e:
            msg2 += "\n\nEncountered error trying to gather job/wrapper status for email.  Check output for blockpost for further details."
            print "\n\nEncountered error trying to gather job/wrapper status for email"
            print "%s: %s" % (e.__class__.__name__,str(e))
            retval = PF_EXIT_FAILURE

        if convertBool(config[PF_USE_QCF]): 
            try:
                import qcframework.qcfdb as qcfdb
                dbh = qcfdb.QCFDB(config['des_services'], config['des_db_section'])
                print "lastwraps = ", lastwraps
                wrapmsg = dbh.get_qcf_messages_for_wrappers(lastwraps)
                print "wrapmsg = ", wrapmsg
                dbh.close() 

                MAXMESG = 3
                msg2 += "\n\n\nDetails\n"
                for jobnum in sorted(job_byblk[blknum].keys()):
                    maxwrap = max(wrap_byjob[jobnum].keys())
                    maxwrapid = wrap_byjob[jobnum][maxwrap]['id']
                    modname = wrap_byjob[jobnum][maxwrap]['modname']
                    if jobdict['status'] != PF_EXIT_SUCCESS:
                        msg2 += "\t%04d %s\n" % (jobnum, modname)
                        if maxwrapid in wrapmsg:
                            if len(wrapmsg[maxwrapid]) > MAXMESG:
                                msg2 += "\t\tOnly printing last %d messages\n" % MAXMESG
                                for mesgrow in wrapmsg[maxwrapid][-MAXMESG:]:
                                    msg2 += "\t\t%s\n" % mesgrow['message']
                            else:
                                for mesgrow in wrapmsg[maxwrapid]:
                                    msg2 += "\t\t%s\n" % mesgrow['message']
                        else:
                            msg2 += "\t\tNo QCF messages\n"
            except Exception, e:
                msg2 += "\n\nEncountered error trying to gather QCF info for email.  Check output for blockpost for further details."
                print "\n\nEncountered error trying to gather QCF info status for email"
                print "%s: %s" % (e.__class__.__name__,str(e))

    if retval:
        #print "Block failed\nAlready sent email"
        print "Sending block failed email\n";
        msg1 = "%s:  block %s has failed." % (run, blockname)

        send_email(config, blockname, retval, "", msg1, msg2)
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
    dbh = None
    if convertBool(config[PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['des_services'], config['des_db_section'])
        dbh.update_block_end(config, retval)
    #logEvent(config, blockname, 'mngr', 'j', 'posttask', retval)

    if retval == PF_EXIT_SUCCESS:
        # Get ready for next block
        config.inc_blknum()
        config.save_file(configfile)
        print "new blknum = ", config[PF_BLKNUM]
        print "number of blocks = ", len(config.block_array)
        if int(config[PF_BLKNUM]) <= len(config.block_array):   # blknum is 1-based
            retval = PF_EXIT_NEXTBLOCK
            print "modified retval to %s" % retval
    else:
        retval = PF_EXIT_FAILURE

    if convertBool(config[PF_USE_DB_OUT]): 
        if retval != PF_EXIT_NEXTBLOCK:
            fwdebug(0, 'PFWPOST_DEBUG', "Calling update_attempt_end: retval = %s" % retval)
            dbh.update_attempt_end(config, retval)
        else:
            fwdebug(0, 'PFWPOST_DEBUG', "Not calling update_attempt_end: use_db_out = %s, retval = %s" % (config[PF_USE_DB_OUT], retval))

        dbh.commit()
        dbh.close()
    
    fwdebug(3, 'PFWPOST_DEBUG', "Returning retval = %s (%s)" % (retval, type(retval)))
    fwdebug(0, 'PFWPOST_DEBUG', "END")
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
