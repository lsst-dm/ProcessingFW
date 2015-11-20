#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import re
import os
import tempfile
import traceback
import datetime

import processingfw.pfwdefs as pfwdefs
import despymisc.miscutils as miscutils

import processingfw.pfwconfig as pfwconfig
import processingfw.pfwcondor as pfwcondor
import processingfw.pfwutils as pfwutils
import processingfw.pfwdb as pfwdb
from processingfw.pfwlog import log_pfw_event
from processingfw.pfwemail import send_subblock_email



def parse_job_output(config, jobnum, dbh=None, retval=None):
    """ Search stdout/stderr for timing stats as well as eups setup or DB connection error messages and insert them into db """
    jobbase = config.get_filename('job', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM:jobnum, 
                                                        'flabel': 'runjob', 
                                                        'fsuffix':''}})

    tjobinfo = {}
    tjobinfo_task = {}
    for f in ['%sout'%jobbase, '%serr'%jobbase]:
        if os.path.exists(f):
            with open(f, 'r') as jobfh:
                for line in jobfh:
                    line = line.strip()
                    if line.startswith('PFW:'): 
                        parts = line.split()
                        if parts[1] == 'batchid':
                            if parts[2] == '=':   # older pfwrunjob.py
                                tjobinfo['target_job_id'] = parts[3]
                            else:
                                tjobinfo['target_job_id'] = parts[2]
                        elif parts[1] == 'condorid':
                            tjobinfo['condor_job_id'] = parts[2]
                        elif parts[1] == 'job_shell_script':
                            print "parts[2]", parts[2]
                            print "parts[3]", parts[3]
                            if parts[2] == 'exechost:':
                                #tjobinfo['target_exec_host']= parts[3]
                                tjobinfo_task['exec_host']= parts[3]
                            elif parts[2] == 'starttime:':
                                #tjobinfo['target_start_time'] = datetime.datetime.fromtimestamp(float(parts[3]))
                                tjobinfo_task['start_time']= datetime.datetime.fromtimestamp(float(parts[3]))
                            elif parts[2] == 'endtime:':
                                #tjobinfo['target_end_time'] = datetime.datetime.fromtimestamp(float(parts[3]))
                                tjobinfo_task['end_time']= datetime.datetime.fromtimestamp(float(parts[3]))
                            elif parts[2] == 'exit_status:':
                                tjobinfo_task['status'] = parts[3]
                    elif 'ORA-' in line:
                        print "Found:", line
                        print "Setting retval to failure"
                        tjobinfo_task['status'] = pfwdefs.PF_EXIT_FAILURE
                        if dbh:
                            dbh.insert_message(config['task_id']['job'][jobnum], pfwdb.PFW_MSG_ERROR, line)
                    elif 'Error: eups setup' in line:
                        print "Found:", line
                        print "Setting retval to failure"
                        tjobinfo_task['status'] = pfwdefs.PF_EXIT_EUPS_FAILURE
                        if dbh:
                            dbh.insert_message(config['task_id']['job'][jobnum], pfwdb.PFW_MSG_ERROR, line)
                    elif 'Exiting with status' in line:
                        m = re.search('Exiting with status (\d+)', line)
                        if m:
                            if int(m.group(1)) != 0 and retval == 0:
                                print "Found:", line
                                msg = "Info:  Job exit status was %s, but retval was %s.   Setting retval to failure." % (m.group(1), retval)
                                print msg
                                tjobinfo['status'] = pfwdefs.PF_EXIT_FAILURE
                                if dbh:
                                    dbh.insert_message(config['task_id']['job'][jobnum], pfwdb.PFW_MSG_ERROR, msg)
    return (tjobinfo, tjobinfo_task)



def jobpost(argv = None):
    """ Performs steps needed after a pipeline job """
    CONDOR2DB = { 'jobid': 'condor_job_id', 
                  'csubmittime': 'condor_submit_time', 
                  'gsubmittime': 'target_submit_time', 
                  'starttime'  : 'condor_start_time', 
                  'endtime'    : 'condor_end_time'}

    if argv is None:
        argv = sys.argv

    debugfh = tempfile.NamedTemporaryFile(prefix='jobpost_', dir='.', delete=False)
    tmpfn = debugfh.name
    sys.stdout = debugfh
    sys.stderr = debugfh

    miscutils.fwdebug(0, 'PFWPOST_DEBUG', "temp log name = %s" % tmpfn)
    print 'cmd>',' '.join(argv)  # print command line for debugging

    if len(argv) < 7:
        # open file to catch error messages about command line
        print 'Usage: jobpost.py configfile block jobnum inputtar outputtar retval'
        debugfh.close()
        return(pfwdefs.PF_EXIT_FAILURE)

    configfile = argv[1]
    blockname = argv[2]
    jobnum = argv[3]
    inputtar = argv[4]
    outputtar = argv[5]
    retval = pfwdefs.PF_EXIT_FAILURE
    if len(argv) == 7:
        retval = int(sys.argv[6])

    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "configfile = %s" % configfile)
    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "block = %s" % blockname)
    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "jobnum = %s" % jobnum)
    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "inputtar = %s" % inputtar)
    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "outputtar = %s" % outputtar)
    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "retval = %s" % retval)


    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "done reading config file")


    # now that have more information, rename output file
    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "before get_filename")
    blockname = config['blockname']
    blkdir = config['block_dir']
    tjpad = pfwutils.pad_jobnum(jobnum)

    os.chdir("%s/%s" % (blkdir,tjpad))
    new_log_name = config.get_filename('job', {pfwdefs.PF_CURRVALS: 
                                              {'flabel': 'jobpost', 
                                                pfwdefs.PF_JOBNUM: jobnum,
                                               'fsuffix':'out'}})
    new_log_name = "%s" % (new_log_name)
    miscutils.fwdebug(0, 'PFWPOST_DEBUG', "new_log_name = %s" % new_log_name)
     
    debugfh.close()
    os.chmod(tmpfn, 0666)
    os.rename(tmpfn, new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh
    

    dbh = None
    if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['submit_des_services'], config['submit_des_db_section'])

        # get job information from the job stdout if exists
        (tjobinfo, tjobinfo_task) = parse_job_output(config, jobnum, dbh, retval)

        if dbh and len(tjobinfo) > 0:
            print "tjobinfo: ", tjobinfo
            dbh.update_tjob_info(config, jobnum, tjobinfo)

        # get job information from the condor job log 
        logfilename = 'runjob.log'
        if os.path.exists(logfilename):   # if made it to submitting/running jobs
            try:
                # update job info in DB from condor log
                print "Updating job info in DB from condor log"
                condorjobinfo = pfwcondor.parse_condor_user_log(logfilename)
                if len(condorjobinfo.keys()) > 1:
                    print "More than single job in job log"
                j = condorjobinfo.keys()[0]
                cjobinfo = condorjobinfo[j]
                djobinfo = {}
                for ck, dk in CONDOR2DB.items():
                    if ck in cjobinfo:
                        djobinfo[dk] = cjobinfo[ck]
                print djobinfo
                dbh.update_job_info(config, cjobinfo['jobname'], djobinfo)

                if 'holdreason' in cjobinfo and cjobinfo['holdreason'] is not None:
                    msg = "Condor HoldReason: %s" % cjobinfo['holdreason']
                    print msg
                    if dbh:
                        dbh.insert_message(config['task_id']['job'][jobnum], pfwdb.PFW_MSG_WARN, msg)

                if 'abortreason' in cjobinfo and cjobinfo['abortreason'] is not None:
                    tjobinfo_task['start_time'] = cjobinfo['starttime']
                    tjobinfo_task['end_time'] = cjobinfo['endtime']
                    if 'condor_rm' in cjobinfo['abortreason']:
                        tjobinfo_task['status'] = pfwdefs.PF_EXIT_OPDELETE
                    else:
                        tjobinfo_task['status'] = pfwdefs.PF_EXIT_CONDOR
                    print "MMG", tjobinfo_task
                else:
                    print "NOTTHERE"
            except Exception as e:
                (extype, value, trback) = sys.exc_info()
                traceback.print_exception(extype, value, trback, file=sys.stdout)
        else:
            print "Warning:  no job condor log file"
        

        if dbh:
            # update job task
            if 'status' not in tjobinfo_task:
                tjobinfo_task['status'] = pfwdefs.PF_EXIT_CONDOR
            if 'end_time' not in tjobinfo_task:
                tjobinfo_task['end_time'] = datetime.datetime.now()
            wherevals = {'id': config['task_id']['job'][jobnum]}
            dbh.basic_update_row ('task', tjobinfo_task, wherevals)
            dbh.commit()

        
    log_pfw_event(config, blockname, jobnum, 'j', ['posttask', retval])


    # input wcl should already exist in untar form
    if os.path.exists(inputtar):
        print "found inputtar: %s" % inputtar
        os.unlink(inputtar)
    else:
        print "Could not find inputtar: %s" % inputtar

    # untar output wcl tar and delete tar
    if os.path.exists(outputtar): 
        print "Size of output wcl tar:", os.path.getsize(outputtar)
        if os.path.getsize(outputtar) > 0:
            print "found outputtar: %s" % outputtar
            pfwutils.untar_dir(outputtar, '..')
            os.unlink(outputtar)
        else:
            msg = "Warn: outputwcl tarball (%s) is 0 bytes." % outputtar
            print msg
            if dbh:
                dbh.insert_message(config['task_id']['job'][jobnum], pfwdb.PFW_MSG_WARN, msg)
    else:
        msg = "Warn: outputwcl tarball (%s) does not exist." % outputtar
        print msg
        if dbh:
            dbh.insert_message(config['task_id']['job'][jobnum], pfwdb.PFW_MSG_WARN, msg)


    if retval != pfwdefs.PF_EXIT_SUCCESS:
        miscutils.fwdebug(0, 'PFWPOST_DEBUG', "Setting failure retval")
        retval = pfwdefs.PF_EXIT_FAILURE     
        
    miscutils.fwdebug(0, 'PFWPOST_DEBUG', "Returning retval = %s" % retval)
    miscutils.fwdebug(0, 'PFWPOST_DEBUG', "jobpost done")
    debugfh.close()
    return(int(retval))


if __name__ == "__main__":
    realstdout = sys.stdout
    realstderr = sys.stderr
    exitcode = jobpost(sys.argv)
    sys.stdout = realstdout
    sys.stderr = realstderr
    miscutils.fwdebug(3, 'PFWPOST_DEBUG', "Exiting with = %s" % exitcode)
    sys.exit(exitcode)
