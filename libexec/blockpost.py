#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import os
import traceback

import processingfw.pfwdefs as pfwdefs
import processingfw.pfwutils as pfwutils
import coreutils.miscutils as coremisc

import processingfw.pfwconfig as pfwconfig
import processingfw.pfwcondor as pfwcondor
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
        return(pfwdefs.PF_EXIT_FAILURE)

    configfile = argv[1]
    retval = int(argv[2])

    coremisc.fwdebug(3, 'PFWPOST_DEBUG', "configfile = %s" % configfile)
    coremisc.fwdebug(0, 'PFWPOST_DEBUG', "retval = %s" % retval)

    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    coremisc.fwdebug(3, 'PFWPOST_DEBUG', "done reading config file")
    blockname = config['blockname']
    blkdir = config['block_dir']
    

    # now that have more information, can rename output file
    coremisc.fwdebug(0, 'PFWPOST_DEBUG', "getting new_log_name")
    new_log_name = config.get_filename('block', {pfwdefs.PF_CURRVALS:
                                                  {'flabel': 'blockpost',
                                                   'fsuffix':'out'}})
    new_log_name = "%s/%s" % (blkdir, new_log_name)
    coremisc.fwdebug(0, 'PFWPOST_DEBUG', "new_log_name = %s" % new_log_name)

    debugfh.close()
    os.chmod('blockpost.out', 0666)
    os.rename('blockpost.out', new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh

    os.chdir(blkdir)
    
    log_pfw_event(config, blockname, 'blockpost', 'j', ['posttask', retval])

    dryrun = config[pfwdefs.PF_DRYRUN]
    warningfile = config['warningfile']
    failedfile = config['failedfile']
    run = config['run']
    reqnum = config.search(pfwdefs.REQNUM, {'interpolate': True})[1]
    unitname = config.search(pfwdefs.UNITNAME, {'interpolate': True})[1]
    attnum = config.search(pfwdefs.ATTNUM, {'interpolate': True})[1]
    blknum = int(config.search(pfwdefs.PF_BLKNUM, {'interpolate': True})[1])
    blktid = None

    msg2 = ""
    dbh = None
    lastwraps = []
    job_byblk = {}
    wrap_byjob = {}
    wrap_bymod = {}
    if coremisc.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
        try:
            dbh = pfwdb.PFWDB(config['submit_des_services'], config['submit_des_db_section'])

            print "\n\nChecking non-job block task status from task table in DB (%s is success)" % pfwdefs.PF_EXIT_SUCCESS
            num_bltasks_failed = 0
            bltasks = {}
            if ('block' in config['task_id'] and 
                str(blknum) in config['task_id']['block']):
                blktid = config['task_id']['block'][str(blknum)]
                bltasks = dbh.get_block_task_info(blktid)
            else:
                msg = "Could not find task id for block %s in config.des" % blockname
                print "Error:", msg
                if 'attempt' in config['task_id']:
                    dbh.insert_message(config['task_id']['attempt'], pfwdb.PFW_MSG_WARN, msg)
                print "all the task ids:", config['task_id']

            for bltdict in bltasks.values():
                if bltdict['status'] != pfwdefs.PF_EXIT_SUCCESS:
                    num_bltasks_failed += 1
                    msg2 += "\t%s" % (bltdict['name'])
                    if bltdict['label'] is not None:
                        msg2 += " - %s" % (bltdict['label'])
                    msg2 += " failed"
                    retval = pfwdefs.PF_EXIT_FAILURE


            print "\n\nChecking job status from pfw_job table in DB (%s is success)" % pfwdefs.PF_EXIT_SUCCESS

            jobinfo = dbh.get_job_info({'reqnum':reqnum, 'unitname': unitname, 'attnum': attnum, 'blknum': blknum})

            wrapinfo = dbh.get_wrapper_info(reqnum, unitname, attnum, blknum)
            dbh.close()

            print "len(jobinfo) = ", len(jobinfo)
            print "len(wrapinfo) = ", len(wrapinfo)
            job_byblk = pfwutils.index_job_info(jobinfo)
            #print "job_byblk:", job_byblk
            wrap_byjob, wrap_bymod = pfwutils.index_wrapper_info(wrapinfo)
            #print "wrap_byjob:", wrap_byjob
            #print "wrap_bymod:", wrap_bymod

            if blknum not in job_byblk:
                print "Warn: could not find jobs for block %s" % blknum
                print "      This is ok if attempt died before jobs ran"
                print "      blknums in job_byblk:" % job_byblk.keys()
            else:
                for jobnum,jobdict in sorted(job_byblk[blknum].items()):
                    jobkeys = ""

                    if jobdict['jobkeys'] is not None:
                        jobkeys = jobdict['jobkeys']
                        #print "jobkeys = ",jobkeys, type(jobkeys)

                    msg2 += "\n\t%s (%s) " % (pfwutils.pad_jobnum(jobnum), jobkeys)

                    if jobnum not in wrap_byjob:
                        msg2 += "\tNo wrapper instances"
                    else:
                        #print "wrapnum in job =", wrap_byjob[jobnum].keys()
                        maxwrap = max(wrap_byjob[jobnum].keys())
                        #print "maxwrap =", maxwrap
                        modname = wrap_byjob[jobnum][maxwrap]['modname']
                        #print "modname =", modname

                        #print "wrap_byjob[jobnum][maxwrap]['task_id']=",wrap_byjob[jobnum][maxwrap]['task_id']
                        msg2 += "%d/%s  %s" % (len(wrap_byjob[jobnum]), jobdict['expect_num_wrap'], modname)
                
                    if jobdict['status'] == pfwdefs.PF_EXIT_EUPS_FAILURE:
                        msg2 += " - FAIL - EUPS setup failure"
                        retval = pfwdefs.PF_EXIT_FAILURE
                    elif jobdict['status'] == pfwdefs.PF_EXIT_CONDOR:
                        msg2 += " - FAIL - Condor/Globus failure"
                        retval = pfwdefs.PF_EXIT_FAILURE
                    elif jobdict['status'] is None:
                        msg2 += " - FAIL - NULL status"
                        if jobnum in wrap_byjob:
                            lastwraps.append(wrap_byjob[jobnum][maxwrap]['task_id'])
                        retval = pfwdefs.PF_EXIT_FAILURE
                    elif jobdict['status'] != pfwdefs.PF_EXIT_SUCCESS:
                        if jobnum in wrap_byjob:
                            lastwraps.append(wrap_byjob[jobnum][maxwrap]['task_id'])
                        msg2 += " - FAIL - Non-zero status"
                        retval = pfwdefs.PF_EXIT_FAILURE

                    msg2 += '\n'

                    if 'message' in jobdict:
                        for msgdict in sorted(jobdict['message'], key=lambda k: k['msgtime']):
                            level = int(msgdict['msglevel'])
                            print level, msgdict['msg'], type(level)
                            print "PFW_MSG_WARN = ", pfwdb.PFW_MSG_WARN, type(pfwdb.PFW_MSG_WARN) 
                            print "PFW_MSG_ERROR = ", pfwdb.PFW_MSG_ERROR 
                            levelstr = 'info'
                            if level == pfwdb.PFW_MSG_WARN:
                                levelstr = 'WARN'
                            elif level == pfwdb.PFW_MSG_ERROR:
                                levelstr = 'ERROR'
        
                            msg2 += "\t\t%s - %s\n" % (levelstr, msgdict['msg'])

        except Exception, e:
            msg2 += "\n\nEncountered error trying to gather status information for email.  Check output for blockpost for further details."
            print "\n\nEncountered error trying to gather status information for email"
            print "%s: %s" % (e.__class__.__name__,str(e))
            (extype, value, trback) = sys.exc_info()
            traceback.print_exception(extype, value, trback, file=sys.stdout)
            retval = pfwdefs.PF_EXIT_FAILURE

        print "lastwraps = ", lastwraps
        if coremisc.convertBool(config[pfwdefs.PF_USE_QCF]) and len(lastwraps) > 0: 
            try:
                import qcframework.qcfdb as qcfdb
                dbh = qcfdb.QCFDB(config['submit_des_services'], config['submit_des_db_section'])
                wrapmsg = dbh.get_qcf_messages_for_wrappers(lastwraps)
                print "wrapmsg = ", wrapmsg
                dbh.close() 

                MAXMESG = 3
                msg2 += "\n\n\nDetails\n"
                for jobnum,jobdict in sorted(job_byblk[blknum].items()):
                    maxwrap = max(wrap_byjob[jobnum].keys())
                    maxwrapid = wrap_byjob[jobnum][maxwrap]['task_id']
                    modname = wrap_byjob[jobnum][maxwrap]['modname']
                    if jobdict['status'] != pfwdefs.PF_EXIT_SUCCESS:
                        msg2 += "\t%s %s\n" % (pfwutils.pad_jobnum(jobnum), modname)
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
                (extype, value, trback) = sys.exc_info()
                traceback.print_exception(extype, value, trback, file=sys.stdout)

    print "before email retval =", retval
    
    if retval:
        if 'when_to_email' in config and config['when_to_email'].lower() != 'never':
            print "Sending block failed email\n";
            msg1 = "%s:  block %s has failed." % (run, blockname)

            send_email(config, blockname, retval, "", msg1, msg2)
    elif coremisc.convertBool(dryrun):
        if 'when_to_email' in config and config['when_to_email'].lower() != 'never':
            print "dryrun = ", dryrun
            print "Sending dryrun email"
            msg1 = "%s:  In dryrun mode, block %s has finished successfully." % (run, blockname)
            msg2 = ""
            send_email(config, blockname, pfwdefs.PF_EXIT_SUCCESS, "[DRYRUN]", msg1, msg2)
        retval = pfwdefs.PF_EXIT_DRYRUN
    elif retval == pfwdefs.PF_EXIT_SUCCESS:
        if ('when_to_email' in config and 
           (config['when_to_email'].lower() == 'block' or
           (config['when_to_email'].lower() == 'run' and int(config[pfwdefs.PF_BLKNUM]) == int(config['num_blocks'])))):
            print "Sending success email\n";
            if config['when_to_email'].lower() == 'run':
                msg1 = "%s:  run has finished successfully." % (run)
            else:
                msg1 = "%s:  block %s has finished successfully." % (run, blockname)
            msg2 = ""

            send_email(config, blockname, retval, "", msg1, msg2)
    else:
        print "Not sending email"
        print "retval = ", retval

    # Store values in DB and hist file 
    dbh = None
    if coremisc.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['submit_des_services'], config['submit_des_db_section'])
        if blktid is not None:
            print "Updating end of block task", blktid 
            dbh.end_task(blktid, retval, True)
        else:
            print "Could not update end of block task without block task id"
        if retval != pfwdefs.PF_EXIT_SUCCESS:
            print "Updating end of attempt", config['task_id']['attempt']
            dbh.end_task(config['task_id']['attempt'], retval, True)
        dbh.commit()
        dbh.close()

    print "before next block retval = ", retval
    if retval == pfwdefs.PF_EXIT_SUCCESS:
        # Get ready for next block
        config.inc_blknum()
        config.save_file(configfile)
        print "new blknum = ", config[pfwdefs.PF_BLKNUM]
        print "number of blocks = ", len(config.block_array)
    else:
        retval = pfwdefs.PF_EXIT_FAILURE

    coremisc.fwdebug(3, 'PFWPOST_DEBUG', "Returning retval = %s (%s)" % (retval, type(retval)))
    coremisc.fwdebug(0, 'PFWPOST_DEBUG', "END")
    debugfh.close()
    return(int(retval))

if __name__ == "__main__":
    realstdout = sys.stdout
    realstderr = sys.stderr
    exitcode = blockpost(sys.argv)
    sys.stdout = realstdout
    sys.stderr = realstderr
    coremisc.fwdebug(3, 'PFWPOST_DEBUG', "Exiting with = %s" % exitcode)
    coremisc.fwdebug(3, 'PFWPOST_DEBUG', "type of exitcode = %s" % type(exitcode))
    sys.exit(exitcode)
