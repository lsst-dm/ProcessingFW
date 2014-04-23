#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import os
import tempfile
from processingfw.pfwdefs import *
from coreutils.miscutils import *

import processingfw.pfwconfig as pfwconfig
import processingfw.pfwutils as pfwutils
import processingfw.pfwdb as pfwdb
from processingfw.pfwlog import log_pfw_event
from processingfw.pfwemail import send_subblock_email


def jobpost(argv = None):
    if argv is None:
        argv = sys.argv

    debugfh = tempfile.NamedTemporaryFile(prefix='jobpost_', dir='.', delete=False)
    tmpfn = debugfh.name
    sys.stdout = debugfh
    sys.stderr = debugfh

    fwdebug(0, 'PFWPOST_DEBUG', "temp log name = %s" % tmpfn)
    print 'cmd>',' '.join(argv)  # print command line for debugging

    if len(argv) < 7:
        # open file to catch error messages about command line
        print 'Usage: jobpost.py configfile block jobnum inputtar outputtar retval'
        debugfh.close()
        return(PF_EXIT_FAILURE)

    configfile = argv[1]
    blockname = argv[2]
    jobnum = argv[3]
    inputtar = argv[4]
    outputtar = argv[5]
    retval = PF_EXIT_FAILURE
    if len(argv) == 7:
        retval = int(sys.argv[6])

    fwdebug(3, 'PFWPOST_DEBUG', "configfile = %s" % configfile)
    fwdebug(3, 'PFWPOST_DEBUG', "block = %s" % blockname)
    fwdebug(3, 'PFWPOST_DEBUG', "jobnum = %s" % jobnum)
    fwdebug(3, 'PFWPOST_DEBUG', "inputtar = %s" % inputtar)
    fwdebug(3, 'PFWPOST_DEBUG', "outputtar = %s" % outputtar)
    fwdebug(3, 'PFWPOST_DEBUG', "retval = %s" % retval)


    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    fwdebug(3, 'PFWPOST_DEBUG', "done reading config file")


    # now that have more information, rename output file
    fwdebug(3, 'PFWPOST_DEBUG', "before get_filename")
    blockname = config['blockname']
    blkdir = config['block_dir']
    tjpad = "%04d" % int(jobnum)

    os.chdir("%s/%s" % (blkdir,tjpad))
    new_log_name = config.get_filename('job', {PF_CURRVALS: 
                                              {'flabel': 'jobpost', 
                                                PF_JOBNUM: jobnum,
                                               'fsuffix':'out'}})
    new_log_name = "%s" % (new_log_name)
    fwdebug(0, 'PFWPOST_DEBUG', "new_log_name = %s" % new_log_name)
     
    debugfh.close()
    os.rename(tmpfn, new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh
    

    dbh = None
    if convertBool(config[PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['submit_des_services'], config['submit_des_db_section'])
    
    # Search for eups setup or DB connection error messages and insert them into db
    jobbase = config.get_filename('job', {PF_CURRVALS: {PF_JOBNUM:jobnum, 
                                                        'flabel': 'runjob', 
                                                        'fsuffix':''}})

    # grep job stdout|stderr files for failures not caught elsewhere
    submit_id = None
    target_id = None
    exechost = None
    for f in ['%sout'%jobbase, '%serr'%jobbase]:
        if os.path.exists(f):
            with open(f, 'r') as jobfh:
                for line in jobfh:
                    line = line.strip()
                    if line.startswith('PFW: batchid'):
                        target_id = line.replace('PFW: batchid','').strip()
                    elif line.startswith('PFW: condorid'):
                        submit_id = line.replace('PFW: condorid','').strip()
                    elif line.startswith('job_shell_script exechost'):
                        exechost = line.replace('job_shell_script exechost','').strip()
                    elif 'ORA-' in line:
                        print "Found:", line
                        print "Setting retval to failure"
                        retval = PF_EXIT_FAILURE
                        if dbh:
                            dbh.insert_message(config, 'job', pfwdb.PFW_MSG_ERROR, line, config['blknum'], jobnum)
                    elif 'Error: eups setup had non-zero exit code' in line:
                        print "Found:", line
                        print "Setting retval to failure"
                        retval = PF_EXIT_FAILURE
                        if dbh:
                            dbh.update_job_batchids(config, jobnum, submit_id, target_id, exechost)
                            dbh.insert_message(config, 'job', pfwdb.PFW_MSG_ERROR, line, config['blknum'], jobnum)
                    elif 'Exiting with status' in line:
                        m = re.search('Exiting with status (\d+)', line)
                        if m:
                            if int(m.group(1)) != 0 and retval == 0:
                                print "Found:", line
                                msg = "Info:  Job exit status was %s, but retval was %s.   Setting retval to failure." % (m.group(1), retval)
                                print msg
                                retval = PF_EXIT_FAILURE
                                if dbh:
                                    dbh.insert_message(config, 'job', pfwdb.PFW_MSG_ERROR, msg, config['blknum'], jobnum)
                        
            
    
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
    fwdebug(0, 'PFWPOST_DEBUG', "jobpost done")
    debugfh.close()
    return(int(retval))

if __name__ == "__main__":
    realstdout = sys.stdout
    realstderr = sys.stderr
    exitcode = jobpost(sys.argv)
    sys.stdout = realstdout
    sys.stderr = realstderr
    fwdebug(3, 'PFWPOST_DEBUG', "Exiting with = %s" % exitcode)
    sys.exit(exitcode)
