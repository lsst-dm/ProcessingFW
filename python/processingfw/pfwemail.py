#!/usr/bin/env python
# $Id:$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

""" Utilities for sending PFW emails """

import os
import glob
import subprocess
from cStringIO import StringIO
import processingfw.pfwconfig as pfwconfig

NUMLINES = 50

def send_email(config, block, status, subject, msg1, msg2):
    """create PFW email and send it"""
    project = config['project']
    run = config['submit_run']

    localmachine = os.uname()[1]
    
    mailfile = "email_%s.txt" % (block)
    mailfh = open(mailfile, "w")

    mailfh.write("""
*************************************************************
*                                                           *
*  This is an automated message from DESDM.  Do not reply.  *
*                                                           *
*************************************************************
    """)
    mailfh.write("\n")

    mailfh.write("%s\n\n\n" % msg1)

    mailfh.write("operator = %s\n" % config['operator'])
    mailfh.write("project = %s\n" % project)
    mailfh.write("run = %s\n" % run)
    if 'event_tag' in config:
        mailfh.write("run name = %s\n" % config['event_tag'])
    mailfh.write("\n")

    mailfh.write("Submit:\n")
    mailfh.write("\tmachine = %s\n" % localmachine)
    mailfh.write("\tnode = %s\n" % config['submit_node'])
    mailfh.write("\tDES_HOME = %s\n" % config['des_home'])
    mailfh.write("\tconfig = %s/%s\n" % \
            (config['submit_dir'], config['config_filename']))
    mailfh.write("\tdirectory = %s\n\n" % config['work_dir'])

    mailfh.write("Target:\n")
    mailfh.write("\tmachine = %s\n" % config['login_host'])
    mailfh.write("\tnode = %s\n" % config['target_node'])
    mailfh.write("\tdirectory = %s/%s\n" % \
            (config['archive_root'], config['run_dir']))
    mailfh.write("\n\n")
    mailfh.write("------------------------------\n")

    if msg2: 
        mailfh.write("%s\n" % msg2)

    mailfh.close()

    subject = "DESDM: %s %s %s %s" % (project, run, block, subject)
    if int(status) == pfwconfig.PfwConfig.NOTARGET:
        subject += " [NOTARGET]"
    elif int(status) != pfwconfig.PfwConfig.SUCCESS:
        subject += " [FAILED]"

    if 'email' in config:
        email = config['email']
        print "Sending %s as email to %s (block=%s)" % (mailfile, email, block)
        mailfh = open(mailfile, 'r')
        print subprocess.check_output(['/bin/mail', '-s', '"%s"' % subject, email], stdin=mailfh)
        mailfh.close()
        # don't delete email file as helps others debug as well as sometimes emails are missed
    else:
        print block, "No email address.  Not sending email."
    
def send_subblock_email(config, block, subblock, retval):
    """create PFW subblock email and send it"""
    print "send_subblock_email BEG"
    print "send_subblock_email block=%s" % block
    print "send_subblock_email subblock=%s" % subblock
    print "send_subblock_email retval=%s" % retval
    msg1 = "Failed subblock = %s" % subblock
    msg2 = get_subblock_output(subblock)
    send_email(config, block, retval, "[FAILED]", msg1, msg2)
    print "send_subblock_email END"


def get_job_info(block): 
    """gather target job status info for email"""
    iostr = StringIO()
    iostr.write("%6s\t%25s\t%7s\t%7s\t%s" % \
            ('JOBNUM', 'MODULE', 'STATUS4', 'STATUS5', 'MSG'))
    filepat = "../%s_*/*.jobinfo.out" % block
    jobinfofiles = glob.glob(filepat)
    for fname in jobinfofiles.sort():
        jobinfofh = open(fname, "r")
        iostr.write(jobinfofh.read())
        jobinfofh.close()
    return iostr.getvalue()



def get_subblock_output(subblock):
    """Grab tail of stdout/stderr to include in email"""
    (path, block) = os.path.split(os.getcwd())

    iostr = StringIO()

    fileout = "%s/%s/%s.out" % (path, block, subblock)
    fileerr = "%s/%s/%s.err" % (path, block, subblock)

    iostr.write("Standard output = %s\n" % fileout)
    iostr.write("Standard error = %s\n" % fileerr)
    iostr.write("\n\n")

    iostr.write("===== Standard error  - Last %s lines =====\n" % NUMLINES)
    if os.path.exists(fileerr):
        cmd = "tail -%s %s" % (NUMLINES, fileerr)
        lines = subprocess.check_output(cmd.split())
        iostr.write(lines)
    else:
        iostr.write("Could not read standard err file for %s\n" % subblock)
    iostr.write("\n\n")

    iostr.write("===== Standard output - Last %s lines =====\n" % NUMLINES)
    if os.path.exists(fileout):
        cmd = "tail -%s %s" % (NUMLINES, fileout)
        lines = subprocess.check_output(cmd.split())
        iostr.write(lines)
    else:
        iostr.write("Could not read standard out file for %s\n" % subblock)
    
    return iostr.getvalue()
