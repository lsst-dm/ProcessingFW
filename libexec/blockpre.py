#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import os
import socket

import processingfw.pfwdefs as pfwdefs
import despymisc.miscutils as miscutils
from processingfw.pfwlog import log_pfw_event
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwcondor as pfwcondor
import processingfw.pfwdb as pfwdb


def write_block_condor(config):
    blockname = config['blockname']
    blkdir = config['block_dir']
    run = config['submit_run']
    filename = 'blocktask.condor'
    full_dag_filename = "%s/%s" % (blkdir, filename)
    submit_machine = socket.gethostname() 

    blockbase = config.get_filename('block', {pfwdefs.PF_CURRVALS: {'flabel': '$(jobname)', 'fsuffix':''}})
    jstdout = "%s/%sout" % (blkdir, blockbase)   # base ends with .
    jstderr = "%s/%serr" % (blkdir, blockbase)

    jobattribs = {
                  'executable':'$(exec)',
                  'arguments':'$(args)',
                  'initialdir': blkdir,
                  #'when_to_transfer_output': 'ON_EXIT_OR_EVICT',
                  #'transfer_executable': 'True',
                  'notification': 'Never',
                  'output': jstdout,
                  'error': jstderr,
                  'log': 'blocktask.log',
                  'getenv': 'true',
                  'periodic_remove': '((JobStatus == 5) && (HoldReason =!= "via condor_hold (by user %s)"))' % config['operator'],
                  'periodic_hold': '((NumJobStarts > 0) && (JobStatus == 1))'   # put jobs that have run once and are back in idle on hold
                  }

    userattribs = config.get_condor_attributes(blockname, '$(jobname)')
    reqs = ['NumJobStarts == 0']   # don't want to rerun any job
    jobattribs['universe'] = 'vanilla'
    reqs.append('(machine == "%s")' % submit_machine)
    jobattribs['requirements'] = ' && '.join(reqs)

    pfwcondor.write_condor_descfile('blocktask', filename, jobattribs, userattribs)

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")

    return filename


def blockpre(argv = None):
    if argv is None:
        argv = sys.argv

    realstdout = sys.stdout

    DEFAULT_LOG = 'blockpre.out'

    debugfh = open(DEFAULT_LOG, 'w', 0) 
    sys.stdout = debugfh
    sys.stderr = debugfh
    
    print ' '.join(sys.argv) # command line for debugging
    
    if len(argv) < 2 or len(argv) > 3:
        print 'Usage: blockpre configfile'
        debugfh.close()
        return(pfwdefs.PF_EXIT_FAILURE)

    configfile = sys.argv[1]

    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})

    # make sure values which depend upon block are set correctly
    config.set_block_info()
    config.write(configfile)

    miscutils.fwdebug(0, 'PFWPOST_DEBUG', "blknum = %s" % config[pfwdefs.PF_BLKNUM])
    miscutils.fwdebug(0, 'PFWPOST_DEBUG', "blockname = %s" % config['blockname'])

    blockname = config['blockname']
    blkdir = config['block_dir']


    # now that have more information, can rename output file
    miscutils.fwdebug(0, 'PFWPOST_DEBUG', "getting new_log_name")
    new_log_name = config.get_filename('block', {pfwdefs.PF_CURRVALS:
                                                  {'flabel': 'blockpre',
                                                   'fsuffix':'out'}})
    new_log_name = "%s/%s" % (blkdir, new_log_name)
    miscutils.fwdebug(0, 'PFWPOST_DEBUG', "new_log_name = %s" % new_log_name)

    debugfh.close()
    os.chmod(DEFAULT_LOG, 0666)
    os.rename(DEFAULT_LOG, new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh

    os.chdir(blkdir)

    blocktaskfile = write_block_condor(config)
    
    #if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
    #    dbh = pfwdb.PFWDB(config['submit_des_services'], config['submit_des_db_section'])
    #    dbh.insert_block(config)
    
    log_pfw_event(config, blockname, 'blockpre', 'j', ['pretask'])
    
    miscutils.fwdebug(0, 'PFWPOST_DEBUG', "DONE")
    debugfh.close()

    return(pfwdefs.PF_EXIT_SUCCESS)

if __name__ == "__main__":
    sys.exit(blockpre(sys.argv))
