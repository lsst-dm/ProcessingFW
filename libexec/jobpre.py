#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import os
import tempfile
import processingfw.pfwdefs as pfwdefs
import coreutils.miscutils as coremisc
from processingfw.pfwlog import log_pfw_event
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwdb as pfwdb

def jobpre(argv = None):
    if argv is None:
        argv = sys.argv

    debugfh = tempfile.NamedTemporaryFile(prefix='jobpre_', dir='.', delete=False)
    tmpfn = debugfh.name
    sys.stdout = debugfh
    sys.stderr = debugfh
    
    print ' '.join(sys.argv) # command line for debugging
    print os.getcwd()
    
    if len(argv) < 3:
        print 'Usage: jobpre configfile jobnum'
        debugfh.close()
        return(pfwdefs.PF_EXIT_FAILURE)

    configfile = sys.argv[1]
    jobnum = sys.argv[2]    # could also be uberctrl
    
    # read wcl file
    config = pfwconfig.PfwConfig({'wclfile': configfile})
    blockname = config['blockname']
    blkdir = config['block_dir']
    tjpad = "%04d" % int(jobnum)

    # now that have more information, can rename output file
    coremisc.fwdebug(0, 'PFWJOBPRE_DEBUG', "getting new_log_name")
    new_log_name = config.get_filename('job', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM:jobnum,
                                                        'flabel': 'jobpre',
                                                        'fsuffix':'out'}})
    new_log_name = "%s/%s/%s" % (blkdir, tjpad, new_log_name)
    coremisc.fwdebug(0, 'PFWJOBPRE_DEBUG', "new_log_name = %s" % new_log_name)

    debugfh.close()
    os.chmod(tmpfn, 0666)
    os.rename(tmpfn, new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh
    
    if coremisc.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['submit_des_services'], config['submit_des_db_section'])
        ctstr = dbh.get_current_timestamp_str()
        dbh.update_job_info(config, jobnum, {'condor_submit_time': ctstr, 'target_submit_time': ctstr} )

    log_pfw_event(config, blockname, jobnum, 'j', ['pretask'])

    coremisc.fwdebug(0, 'PFWJOBPRE_DEBUG', "DONE")
    debugfh.close()
    return(pfwdefs.PF_EXIT_SUCCESS)

if __name__ == "__main__":
    sys.exit(jobpre(sys.argv))
