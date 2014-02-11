#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import os

from processingfw.pfwdefs import *
from coreutils.miscutils import *
from processingfw.pfwlog import log_pfw_event
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwdb as pfwdb


def write_block_condor(config):
    blockname = config['blockname']
    run = config['submit_run']
    filename = 'blocktask.condor'

    with open("../%s/%s" % (blockname,filename), 'w') as fh:
        fh.write("""universe=local
executable= $(exec)
arguments = $(args)
getenv=true
environment="submit_condorid=$(Cluster).$(Process)"
notification=never
initialdir = ../%(block)s
output=../%(block)s/$(run)_%(block)s_$(jobname).out
error=../%(block)s/$(run)_%(block)s_$(jobname).err
log=blocktask.log
queue
        """ % {'block':blockname})
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
        return(PF_EXIT_FAILURE)

    configfile = sys.argv[1]

    retry = 0
    if len(argv) == 3:
        retry = sys.argv[2]
    fwdebug(0, 'PFWPOST_DEBUG', "retry = %s" % retry)
    
    # read sysinfo file
    config = pfwconfig.PfwConfig({'wclfile': configfile})

    # make sure values which depend upon block are set correctly
    config.set_block_info()
    config.save_file(configfile)

    fwdebug(0, 'PFWPOST_DEBUG', "blknum = %s" % config[PF_BLKNUM])
    fwdebug(0, 'PFWPOST_DEBUG', "blockname = %s" % config['blockname'])
    fwdebug(0, 'PFWPOST_DEBUG', "retry = %s" % retry)
    if int(retry) != int(config[PF_BLKNUM]):
        fwdebug(0, 'PFWPOST_DEBUG', "WARNING: blknum != retry")

#    with open("/tmp/mmgpredebug_%s" % os.getpid(), 'w') as fh:
#        fh.write("blknum = %s\n" % config[PF_BLKNUM])
#        fh.write("blockname = %s\n" % config['blockname'])
#        fh.write("retry = %s\n" % retry)
        

    blockname = config['blockname']
    if not os.path.exists('../%s' % blockname):
        os.mkdir('../%s' % blockname)


    # now that have more information, can rename output file
    fwdebug(0, 'PFWPOST_DEBUG', "getting new_log_name")
    new_log_name = config.get_filename('block', {PF_CURRVALS:
                                                  {'flabel': 'blockpre',
                                                   'fsuffix':'out'}})
    new_log_name = "../%s/%s" % (blockname, new_log_name)
    fwdebug(0, 'PFWPOST_DEBUG', "new_log_name = %s" % new_log_name)

    debugfh.close()
    os.rename(DEFAULT_LOG, new_log_name)
    debugfh = open(new_log_name, 'a+')
    sys.stdout = debugfh
    sys.stderr = debugfh

    blocktaskfile = write_block_condor(config)
    uberblocktask = "../uberctrl/%s" % blocktaskfile
    if os.path.exists(uberblocktask):
        os.unlink(uberblocktask)
    os.symlink("../%s/%s" % (blockname, blocktaskfile), "../uberctrl/%s" % blocktaskfile)
    
    if convertBool(config[PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['submit_des_services'], config['submit_des_db_section'])
        dbh.insert_block(config)
    
    log_pfw_event(config, blockname, 'blockpre', 'j', ['pretask'])
    
    print "blockpre done\n"
    debugfh.close()

    return(PF_EXIT_SUCCESS)

if __name__ == "__main__":
    sys.exit(blockpre(sys.argv))
