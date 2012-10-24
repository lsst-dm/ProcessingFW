#!/usr/bin/env python
# Id:
# Rev::                                  :  # Revision of last commit.
# LastChangedBy::                        :  # Author of last commit. 
# LastChangedDate::                      :  # Date of last commit.

""" Send summary email when run ends (successfully or not) """

import processingfw.pfwconfig as pfwconfig
import processingfw.pfwemail as pfwemail
from processingfw.pfwlog import log_pfw_event
import sys


def summary(argv = None):
    """ Create and send summary email """
    if argv is None:
        argv = sys.argv
    
    debugfh = open('summary.out', 'w', 0)
    sys.stdout = debugfh
    sys.stderr = debugfh
    
    print ' '.join(argv)
    
    if len(argv) < 2:
        print 'Usage: summary configfile status'
        debugfh.close()
        return(pfwconfig.PfwConfig.FAILURE)
    
    if len(argv) == 3:
        status = argv[2]
        # dagman always exits with 0 or 1
        if status == 1:
            status = pfwconfig.PfwConfig.FAILURE
    else:
        print "summary: Missing status value"
        status = None
    
    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': argv[1]})
    
    log_pfw_event(config, 'process', 'mngr', 'j', ['posttask', status])
    
    msgstr = ""
#    msgstr = "End of run tasks (replicating data, ingesting submit runtime, etc) are starting."
    
    msg1 = ""
    subject = ""
    if not status:
        msg1 = "Processing finished with unknown results.\n%s" % msgstr
        subject = "[Unknown]"
#MMG        status = orch.dbutils.update_run_status(config, orch.orchconfig.FAILURE)
    elif 'notarget' in config and config['notarget'] == '1':
        print "notarget =", config['notarget'], int(config['notarget'])

        msg1 = "Processing ended after notarget\n%s" % msgstr
#        subject = "[NOTARGET]"
#MMG        status = orch.dbutils.update_run_status(config, orch.orchconfig.NOTARGET)
    else:
#MMG        status = orch.dbutils.update_run_status(config, status)
    
        if int(status) == pfwconfig.PfwConfig.SUCCESS:
#            msg1 = "Processing is complete.\nEnd of run tasks (replicating data, ingesting submit runtime, etc) are starting."
            msg1 = "Processing has successfully completed.\n"
#            subject = ""
        else:
            print "status = '%s'" % status
            print "type(status) =", type(status)
            print "SUCCESS = '%s'" % pfwconfig.PfwConfig.SUCCESS
            print "type(SUCCESS) =", type(pfwconfig.PfwConfig.SUCCESS)
            msg1 = "Processing aborted with status %s.\n" % (status) 
#            subject = "[FAILED]"
    
    #my $RunID = config->getValueReq("runid");
    #my $msg2 = `$DESHome/bin/desstat -l $Nite $RunID 2>&1`;
    subject = ""
    pfwemail.send_email(config, "processing", status, subject, msg1, '')
    
    print "summary: status = '%s'" % status
    print "summary:", msg1
    print "summary: End"
    debugfh.close()
    return(status)

if __name__ == "__main__":
    sys.exit(summary(sys.argv))
