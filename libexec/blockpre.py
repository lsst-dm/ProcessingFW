#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import os

import processingfw.pfwdefs as pfwdefs
import coreutils.miscutils as coremisc
from processingfw.pfwlog import log_pfw_event
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwdb as pfwdb


def write_block_condor(config):
    blockname = config['blockname']
    blkdir = config['block_dir']
    run = config['submit_run']
    filename = 'blocktask.condor'

    with open("%s/%s" % (blkdir,filename), 'w') as fh:
        fh.write("""universe=local
executable= $(exec)
arguments = $(args)
getenv=true
environment="submit_condorid=$(Cluster).$(Process)"
notification=never
initialdir = %(blkdir)s
output=%(blkdir)s/$(run)_%(block)s_$(jobname).out
error=%(blkdir)s/$(run)_%(block)s_$(jobname).err
log=blocktask.log
queue
        """ % {'block':blockname, 'blkdir':blkdir})
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
        print 'Usage: blockpre configfile [retry]'
        debugfh.close()
        return(pfwdefs.PF_EXIT_FAILURE)

    configfile = sys.argv[1]

    retry = 0
    if len(argv) == 3:
        retry = sys.argv[2]
    coremisc.fwdebug(0, 'PFWPOST_DEBUG', "retry = %s" % retry)
    
    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})

    # make sure values which depend upon block are set correctly
    config.set_block_info()
    config.save_file(configfile)

    coremisc.fwdebug(0, 'PFWPOST_DEBUG', "blknum = %s" % config[pfwdefs.PF_BLKNUM])
    coremisc.fwdebug(0, 'PFWPOST_DEBUG', "blockname = %s" % config['blockname'])
    coremisc.fwdebug(0, 'PFWPOST_DEBUG', "retry = %s" % retry)
    if int(retry) != int(config[pfwdefs.PF_BLKNUM]):
        coremisc.fwdebug(0, 'PFWPOST_DEBUG', "WARNING: blknum != retry")

    blockname = config['blockname']
    blkdir = config['block_dir']


    # now that have more information, can rename output file
    coremisc.fwdebug(0, 'PFWPOST_DEBUG', "getting new_log_name")
    new_log_name = config.get_filename('block', {pfwdefs.PF_CURRVALS:
                                                  {'flabel': 'blockpre',
                                                   'fsuffix':'out'}})
    new_log_name = "%s/%s" % (blkdir, new_log_name)
    coremisc.fwdebug(0, 'PFWPOST_DEBUG', "new_log_name = %s" % new_log_name)

    debugfh.close()
    os.rename(DEFAULT_LOG, new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh


    blocktaskfile = write_block_condor(config)
    
    if coremisc.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['submit_des_services'], config['submit_des_db_section'])
        dbh.insert_block(config)
    
    log_pfw_event(config, blockname, 'blockpre', 'j', ['pretask'])
    
    print "blockpre done\n"
    debugfh.close()

    return(pfwdefs.PF_EXIT_SUCCESS)

if __name__ == "__main__":
    sys.exit(blockpre(sys.argv))
