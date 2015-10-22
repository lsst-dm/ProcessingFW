#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

""" Perform end of block tasks whether block success or failure """

import sys
import os
import traceback

import processingfw.pfwdefs as pfwdefs
import processingfw.pfwutils as pfwutils
import despymisc.miscutils as miscutils

import processingfw.pfwconfig as pfwconfig
import processingfw.pfwdb as pfwdb
from processingfw.pfwlog import log_pfw_event
from processingfw.pfwemail import send_email

def blockpost(argv=None):
    """ Program entry point """
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
        return pfwdefs.PF_EXIT_FAILURE

    configfile = argv[1]
    retval = int(argv[2])

    if miscutils.fwdebug_check(3, 'PFWPOST_DEBUG'):
        miscutils.fwdebug_print("configfile = %s" % configfile)
    miscutils.fwdebug_print("retval = %s" % retval)

    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    if miscutils.fwdebug_check(3, 'PFWPOST_DEBUG'):
        miscutils.fwdebug_print("done reading config file")
    blockname = config.getfull('blockname')
    blkdir = config.getfull('block_dir')


    # now that have more information, can rename output file
    miscutils.fwdebug_print("getting new_log_name")
    new_log_name = config.get_filename('block',
                                       {pfwdefs.PF_CURRVALS: {'flabel': 'blockpost',
                                                              'fsuffix':'out'}})
    new_log_name = "%s/%s" % (blkdir, new_log_name)
    miscutils.fwdebug_print("new_log_name = %s" % new_log_name)

    debugfh.close()
    os.chmod('blockpost.out', 0666)
    os.rename('blockpost.out', new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh

    os.chdir(blkdir)

    log_pfw_event(config, blockname, 'blockpost', 'j', ['posttask', retval])

    dryrun = config.getfull(pfwdefs.PF_DRYRUN)
    run = config.getfull('run')
    reqnum = config.getfull(pfwdefs.REQNUM)
    unitname = config.getfull(pfwdefs.UNITNAME)
    attnum = config.getfull(pfwdefs.ATTNUM)
    blknum = int(config.getfull(pfwdefs.PF_BLKNUM))
    blktid = None

    msg2 = ""
    dbh = None
    lastwraps = []
    job_byblk = {}
    wrap_byjob = {}
    wrap_bymod = {}
    if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_DB_OUT)):
        try:
            dbh = pfwdb.PFWDB(config.getfull('submit_des_services'), 
                              config.getfull('submit_des_db_section'))

            print "\n\nChecking non-job block task status from task table in DB (%s is success)" % \
                  pfwdefs.PF_EXIT_SUCCESS
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
                    dbh.insert_message(config['pfw_attempt_id'], 
                                       config['task_id']['attempt'], 
                                       pfwdefs.PFWDB_MSG_WARN, msg)
                print "all the task ids:", config['task_id']

            for bltdict in bltasks.values():
                if bltdict['status'] != pfwdefs.PF_EXIT_SUCCESS:
                    num_bltasks_failed += 1
                    msg2 += "\t%s" % (bltdict['name'])
                    if bltdict['label'] is not None:
                        msg2 += " - %s" % (bltdict['label'])
                    msg2 += " failed"
                    retval = pfwdefs.PF_EXIT_FAILURE


            print "\n\nChecking job status from pfw_job table in DB (%s is success)" % \
                  pfwdefs.PF_EXIT_SUCCESS

            jobinfo = dbh.get_job_info({'reqnum':reqnum, 'unitname': unitname,
                                        'attnum': attnum, 'blknum': blknum})

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
                for jobnum, jobdict in sorted(job_byblk[blknum].items()):
                    jobkeys = ""

                    if jobdict['jobkeys'] is not None:
                        jobkeys = jobdict['jobkeys']
                        #print "jobkeys = ", jobkeys, type(jobkeys)

                    msg2 += "\n\t%s (%s) " % (pfwutils.pad_jobnum(jobnum), jobkeys)

                    if jobnum not in wrap_byjob:
                        msg2 += "\tNo wrapper instances"
                    else:
                        #print "wrapnum in job =", wrap_byjob[jobnum].keys()
                        maxwrap = max(wrap_byjob[jobnum].keys())
                        #print "maxwrap =", maxwrap
                        modname = wrap_byjob[jobnum][maxwrap]['modname']
                        #print "modname =", modname

                        msg2 += "%d/%s  %s" % (len(wrap_byjob[jobnum]),
                                               jobdict['expect_num_wrap'], modname)

                    if jobdict['status'] == pfwdefs.PF_EXIT_EUPS_FAILURE:
                        msg2 += " - FAIL - EUPS setup failure"
                        retval = jobdict['status']
                    elif jobdict['status'] == pfwdefs.PF_EXIT_CONDOR:
                        msg2 += " - FAIL - Condor/Globus failure"
                        retval = jobdict['status']
                    elif jobdict['status'] is None:
                        msg2 += " - FAIL - NULL status"
                        if jobnum in wrap_byjob:
                            lastwraps.append(wrap_byjob[jobnum][maxwrap]['task_id'])
                        retval = pfwdefs.PF_EXIT_FAILURE
                    elif jobdict['status'] != pfwdefs.PF_EXIT_SUCCESS:
                        if jobnum in wrap_byjob:
                            lastwraps.append(wrap_byjob[jobnum][maxwrap]['task_id'])
                        msg2 += " - FAIL - Non-zero status"
                        retval = jobdict['status']

                    msg2 += '\n'

                    if 'message' in jobdict:
                        for msgdict in sorted(jobdict['message'], key=lambda k: k['msgtime']):
                            level = int(msgdict['msglevel'])
                            print level, msgdict['msg'], type(level)
                            print "PFWDB_MSG_WARN = ", pfwdefs.PFWDB_MSG_WARN, \
                                  type(pfwdefs.PFWDB_MSG_WARN)
                            print "PFWDB_MSG_ERROR = ", pfwdefs.PFWDB_MSG_ERROR
                            levelstr = 'info'
                            if level == pfwdefs.PFWDB_MSG_WARN:
                                levelstr = 'WARN'
                            elif level == pfwdefs.PFWDB_MSG_ERROR:
                                levelstr = 'ERROR'

                            msg2 += "\t\t%s - %s\n" % (levelstr, msgdict['msg'])

        except Exception as exc:
            msg2 += "\n\nEncountered error trying to gather status information for email."
            msg2 += "\nCheck output for blockpost for further details."
            print "\n\nEncountered error trying to gather status information for email"
            print "%s: %s" % (exc.__class__.__name__, str(exc))
            (extype, exvalue, trback) = sys.exc_info()
            traceback.print_exception(extype, exvalue, trback, file=sys.stdout)
            retval = pfwdefs.PF_EXIT_FAILURE

        print "lastwraps = ", lastwraps
        if miscutils.convertBool(config.getfull(pfwdefs.PF_USE_QCF)) and len(lastwraps) > 0:
            try:
                import qcframework.qcfdb as qcfdb
                dbh = qcfdb.QCFDB(config.getfull('submit_des_services'), 
                                  config.getfull('submit_des_db_section'))
                wrapmsg = dbh.get_qcf_messages_for_wrappers(lastwraps)
                print "wrapmsg = ", wrapmsg
                dbh.close()

                MAXMESG = 3
                msg2 += "\n\n\nDetails\n"
                for jobnum, jobdict in sorted(job_byblk[blknum].items()):
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
            except Exception as exc:
                msg2 += "\n\nEncountered error trying to gather QCF info for email."
                msg2 += "\nCheck output for blockpost for further details."
                print "\n\nEncountered error trying to gather QCF info status for email"
                print "%s: %s" % (exc.__class__.__name__, str(exc))
                (extype, exvalue, trback) = sys.exc_info()
                traceback.print_exception(extype, exvalue, trback, file=sys.stdout)

    print "before email retval =", retval

    when_to_email = 'run'
    if 'when_to_email' in config:
        when_to_email = config.getfull('when_to_email').lower()

    if retval:
        if when_to_email != 'never':
            print "Sending block failed email\n"
            msg1 = "%s:  block %s has failed." % (run, blockname)

            send_email(config, blockname, retval, "", msg1, msg2)
        else:
            print "Not sending failed email"
            print "retval = ", retval
    elif miscutils.convertBool(dryrun):
        if when_to_email != 'never':
            print "dryrun = ", dryrun
            print "Sending dryrun email"
            msg1 = "%s:  In dryrun mode, block %s has finished successfully." % (run, blockname)
            msg2 = ""
            send_email(config, blockname, pfwdefs.PF_EXIT_SUCCESS, "[DRYRUN]", msg1, msg2)
        else:
            print "Not sending dryrun email"
            print "retval = ", retval
        retval = pfwdefs.PF_EXIT_DRYRUN
    elif retval == pfwdefs.PF_EXIT_SUCCESS:
        if when_to_email == 'block':
            msg1 = "%s:  block %s has finished successfully." % (run, blockname)
            msg2 = ""
            print "Sending success email\n"
            send_email(config, blockname, retval, "", msg1, msg2)
        elif when_to_email == 'run':
            numblocks = len(miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST], ','))
            if int(config[pfwdefs.PF_BLKNUM]) == numblocks:
                msg1 = "%s:  run has finished successfully." % (run)
                msg2 = ""
                print "Sending success email\n"
                send_email(config, blockname, retval, "", msg1, msg2)
            else:
                print "Not sending run email because not last block"
                print "retval = ", retval
        else:
            print "Not sending success email"
            print "retval = ", retval
    else:
        print "Not sending email"
        print "retval = ", retval

    # Store values in DB and hist file
    dbh = None
    if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]):
        dbh = pfwdb.PFWDB(config.getfull('submit_des_services'), config.getfull('submit_des_db_section'))
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
        with open(configfile, 'w') as cfgfh:
            config.write(cfgfh)
        print "new blknum = ", config[pfwdefs.PF_BLKNUM]
        print "number of blocks = ", len(miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST], ','))

    miscutils.fwdebug_print("Returning retval = %s (%s)" % (retval, type(retval)))
    miscutils.fwdebug_print("END")
    debugfh.close()
    return int(retval)

if __name__ == "__main__":
    realstdout = sys.stdout
    realstderr = sys.stderr

    exitcode = blockpost(sys.argv)

    sys.stdout = realstdout
    sys.stderr = realstderr

    if miscutils.fwdebug_check(3, 'PFWPOST_DEBUG'):
        miscutils.fwdebug_print("Exiting with = %s" % exitcode)
        miscutils.fwdebug_print("type of exitcode = %s" % type(exitcode))

    sys.exit(exitcode)
