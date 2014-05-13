#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import os
import tempfile
import traceback
import datetime

import processingfw.pfwdefs as pfwdefs
import coreutils.miscutils as coremisc

import processingfw.pfwconfig as pfwconfig
import processingfw.pfwcondor as pfwcondor
import processingfw.pfwutils as pfwutils
import processingfw.pfwdb as pfwdb
from processingfw.pfwlog import log_pfw_event
from processingfw.pfwemail import send_subblock_email



def parse_job_output(config, jobnum, dbh=None):
    """ Search stdout/stderr for timing stats as well as eups setup or DB connection error messages and insert them into db """
    jobbase = config.get_filename('job', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM:jobnum, 
                                                        'flabel': 'runjob', 
                                                        'fsuffix':''}})

    tjobinfo = {}
    for f in ['%sout'%jobbase, '%serr'%jobbase]:
        if os.path.exists(f):
            with open(f, 'r') as jobfh:
                for line in jobfh:
                    line = line.strip()
                    if line.startswith('PFW:'): 
                        parts = line.split()
                        if parts[1] == 'batchid':
                            tjobinfo['target_job_id'] = parts[2]
                        elif parts[1] == 'condorid':
                            tjobinfo['condor_job_id'] = parts[2]
                        elif parts[1] == 'job_shell_script':
                            print "parts[2]", parts[2]
                            print "parts[3]", parts[3]
                            if parts[2] == 'exechost:':
                                tjobinfo['target_exec_host']= parts[3]
                            elif parts[2] == 'starttime:':
                                tjobinfo['target_start_time'] = datetime.datetime.fromtimestamp(float(parts[3]))
                            elif parts[2] == 'endtime:':
                                tjobinfo['target_end_time'] = datetime.datetime.fromtimestamp(float(parts[3]))
                            elif parts[2] == 'exit_status:':
                                tjobinfo['target_status'] = parts[3]
                    elif 'ORA-' in line:
                        print "Found:", line
                        print "Setting retval to failure"
                        tjobinfo['target_status'] = pfwdefs.PF_EXIT_FAILURE
                        if dbh:
                            dbh.insert_message(config, 'job', pfwdb.PFW_MSG_ERROR, line, config['blknum'], jobnum)
                    elif 'Error: eups setup had non-zero exit code' in line:
                        print "Found:", line
                        print "Setting retval to failure"
                        tjobinfo['target_status'] = pfwdefs.PF_EXIT_EUPS_FAILURE
                        if dbh:
                            dbh.insert_message(config, 'job', pfwdb.PFW_MSG_ERROR, line, config['blknum'], jobnum)
                    elif 'Exiting with status' in line:
                        m = re.search('Exiting with status (\d+)', line)
                        if m:
                            if int(m.group(1)) != 0 and retval == 0:
                                print "Found:", line
                                msg = "Info:  Job exit status was %s, but retval was %s.   Setting retval to failure." % (m.group(1), retval)
                                print msg
                        	tjobinfo['target_status'] = pfwdefs.PF_EXIT_FAILURE
                                if dbh:
                                    dbh.insert_message(config, 'job', pfwdb.PFW_MSG_ERROR, msg, config['blknum'], jobnum)
    return tjobinfo



def jobpost(argv = None):
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

    coremisc.fwdebug(0, 'PFWPOST_DEBUG', "temp log name = %s" % tmpfn)
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

    coremisc.fwdebug(3, 'PFWPOST_DEBUG', "configfile = %s" % configfile)
    coremisc.fwdebug(3, 'PFWPOST_DEBUG', "block = %s" % blockname)
    coremisc.fwdebug(3, 'PFWPOST_DEBUG', "jobnum = %s" % jobnum)
    coremisc.fwdebug(3, 'PFWPOST_DEBUG', "inputtar = %s" % inputtar)
    coremisc.fwdebug(3, 'PFWPOST_DEBUG', "outputtar = %s" % outputtar)
    coremisc.fwdebug(3, 'PFWPOST_DEBUG', "retval = %s" % retval)


    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    coremisc.fwdebug(3, 'PFWPOST_DEBUG', "done reading config file")


    # now that have more information, rename output file
    coremisc.fwdebug(3, 'PFWPOST_DEBUG', "before get_filename")
    blockname = config['blockname']
    blkdir = config['block_dir']
    tjpad = "%04d" % int(jobnum)

    os.chdir("%s/%s" % (blkdir,tjpad))
    new_log_name = config.get_filename('job', {pfwdefs.PF_CURRVALS: 
                                              {'flabel': 'jobpost', 
                                                pfwdefs.PF_JOBNUM: jobnum,
                                               'fsuffix':'out'}})
    new_log_name = "%s" % (new_log_name)
    coremisc.fwdebug(0, 'PFWPOST_DEBUG', "new_log_name = %s" % new_log_name)
     
    debugfh.close()
    os.chmod(tmpfn, 0666)
    os.rename(tmpfn, new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh
    

    dbh = None
    if coremisc.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['submit_des_services'], config['submit_des_db_section'])


    tjobinfo = parse_job_output(config, jobnum, dbh)

    if dbh and len(tjobinfo) > 0:
        print "tjobinfo: ", tjobinfo
        dbh.update_job_info(config, jobnum, tjobinfo)

        logfilename = 'runjob.log'
        if os.path.exists(logfilename):   # if made it to submitting/running jobs
            try:
                # update job info in DB from condor log
                print "Updating job info in DB from condor log"
                cjobinfo = pfwcondor.parse_condor_user_log(logfilename)
                for j in sorted(cjobinfo.keys()):
                    print cjobinfo[j]
                    djobinfo = {}
                    for ck, dk in CONDOR2DB.items():
                        if ck in cjobinfo[j]:
                            djobinfo[dk] = cjobinfo[j][ck]
                    print djobinfo
                    dbh.update_job_info(config, int(cjobinfo[j]['jobname']), djobinfo)
            except Exception as e:
                (extype, value, trback) = sys.exc_info()
                traceback.print_exception(extype, value, trback, file=sys.stdout)
    
    
    log_pfw_event(config, blockname, jobnum, 'j', ['posttask', retval])


    # input wcl should already exist in untar form
    if os.path.exists(inputtar):
        print "found inputtar: %s" % inputtar
        os.unlink(inputtar)
    else:
        print "Could not find inputtar: %s" % inputtar

    # untar output wcl tar and delete tar
    if os.path.exists(outputtar): 
        if os.path.getsize(outputtar) > 0:
            print "found outputtar: %s" % outputtar
            pfwutils.untar_dir(outputtar, '..')
            os.unlink(outputtar)
        else:
            msg = "Warn: outputwcl tarball (%s) is 0 bytes." % outputtar
            print msg
            if dbh:
                  dbh.insert_message(config, 'job', pfwdb.PFW_MSG_WARN, msg, config['blknum'], jobnum)
    else:
        msg = "Warn: outputwcl tarball (%s) does not exist." % outputtar
        print msg
        if dbh:
              dbh.insert_message(config, 'job', pfwdb.PFW_MSG_WARN, msg, config['blknum'], jobnum)


    if retval != pfwdefs.PF_EXIT_SUCCESS:
        coremisc.fwdebug(0, 'PFWPOST_DEBUG', "Setting failure retval")
        retval = pfwdefs.PF_EXIT_FAILURE     
    
    coremisc.fwdebug(0, 'PFWPOST_DEBUG', "Returning retval = %s" % retval)
    coremisc.fwdebug(0, 'PFWPOST_DEBUG', "jobpost done")
    debugfh.close()
    return(int(retval))


if __name__ == "__main__":
    realstdout = sys.stdout
    realstderr = sys.stderr
    exitcode = jobpost(sys.argv)
    sys.stdout = realstdout
    sys.stderr = realstderr
    coremisc.fwdebug(3, 'PFWPOST_DEBUG', "Exiting with = %s" % exitcode)
    sys.exit(exitcode)
