#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

""" need to write docstring """

import sys
import re
import os
import stat
import subprocess

from processingfw.pfwdefs import *
import processingfw.pfwcondor as pfwcondor
import processingfw.pfwlog as pfwlog
import processingfw.pfwdb as pfwdb
from processingfw.pfwutils import debug


######################################################################
def write_block_dag(config, debugfh=None):
    """  writes block dag file """

    if not debugfh:
        debugfh = sys.stderr
    
    debugfh.write('write_block_dag pwd: %s\n' % os.getcwd())

    block = config['blockname']
    pfwdir = config['processingfw_dir']

#    if not os.path.exists('../%s' % block):
#        os.mkdir('../%s' % block)
#    os.chdir('../%s' % block)
    print "curr dir = ", os.getcwd()


    jobmngr = write_stub_jobmngr_dag(config, block, debugfh)
    dag = config.get_filename('blockdag')

    run = config['submit_run']
    project = config['project']
    runsite = config['runsite']

    dagfh = open(dag, 'w')
    dagfh.write('DOT %s_block.dot\n' % run)

    dagfh.write('JOB begblock blocktask.condor\n')
    dagfh.write('VARS begblock exec="$(pfwdir)/libexec/begblock.py"\n')
    dagfh.write('VARS begblock args="config.des"\n')
    dagfh.write('VARS begblock pfwdir="%s"\n' % pfwdir)
    dagfh.write('VARS begblock project="%s" run="%s" runsite="%s"\n' % (project, run, runsite))
    dagfh.write('VARS begblock block="%s" jobname="begblock"\n' % (block))
    dagfh.write('SCRIPT pre begblock %s/libexec/logpre.py config.des %s j $JOB\n' % (pfwdir, block))
    dagfh.write('SCRIPT post begblock %s/libexec/logpost.py config.des %s j $JOB $RETURN\n' % (pfwdir, block))  

    dagfh.write('\n')
    dagfh.write('JOB jobmngr %s.condor.sub\n' % jobmngr)
    dagfh.write('SCRIPT pre jobmngr %s/libexec/logpre.py config.des %s j $JOB\n' % (pfwdir, block))
    dagfh.write('SCRIPT post jobmngr %s/libexec/logpost.py config.des %s j $JOB $RETURN\n' % (pfwdir, block))

    dagfh.write('\n')
    dagfh.write('JOB endblock blocktask.condor\n')
    dagfh.write('VARS endblock exec="%s/libexec/endblock.py"\n' % pfwdir)
    dagfh.write('VARS endblock args="config.des"\n')
    dagfh.write('VARS endblock jobname="endblock"\n')
    dagfh.write('VARS endblock run="%s"\n' % run)
    dagfh.write('SCRIPT pre endblock %s/libexec/logpre.py config.des %s j $JOB\n' % (pfwdir, block))
    dagfh.write('SCRIPT post endblock %s/libexec/logpost.py config.des %s j $JOB $RETURN\n' % (pfwdir, block))  

    dagfh.write('\nPARENT begblock CHILD jobmngr\n')
    dagfh.write('PARENT jobmngr CHILD endblock\n')
    dagfh.close()
    pfwcondor.add2dag(dag, config.get_dag_cmd_opts(), config.get_condor_attributes("blockmngr"), None, debugfh)
#    os.chdir('../uberctrl')
    return dag


######################################################################
def write_stub_jobmngr_dag(config, block, debugfh=None):
    """  writes stub jobmngr dag file to be overwritten during block """

    if not debugfh:
        debugfh = sys.stderr

    debugfh.write('write_stub_jobmngr pwd: %s\n' % os.getcwd())

    pfwdir = config['processingfw_dir']
    dag = config.get_filename('jobdag')

    dagfh = open(dag, 'w')
    dagfh.write('DOT jobmngr.dot\n')
    dagfh.write('JOB 0001 %s/share/condor/localjob.condor\n' % pfwdir)
    dagfh.write('SCRIPT pre 0001 %s/libexec/logpre.py config.des %s j $JOB' % (pfwdir, block))
    dagfh.write('SCRIPT post 0001 %s/libexec/logpost.py config.des %s j $JOB $RETURN' % (pfwdir, block))
    dagfh.close()

#    pfwcondor.add2dag(dag, config.get_dag_cmd_opts(), config.get_condor_attributes("jobmngr"), "../%s" % block, debugfh)
    pfwcondor.add2dag(dag, config.get_dag_cmd_opts(), config.get_condor_attributes("jobmngr"), None, debugfh)

    os.unlink(dag)
    return dag


######################################################################
#def write_process_dag(config, dag):
#    """ write the process manager dag input file """
#    pfwdir = config['processingfw_dir']
#    blockarray = config.block_array
#
#    dagfh = open(dag, 'w')
#    dagfh.write('DOT process.dot\n')
#    for block in blockarray:
#        config.set_block_info()
#        blockdag = write_block_dag(config, block, sys.stdout)
#        dagfh.write("""
#JOB %(bl)s ../%(bl)s/%(bldag)s.condor.sub
#SCRIPT pre %(bl)s %(pd)s/libexec/logpre.py ../uberctrl/config.des %(bl)s j $JOB
#SCRIPT post %(bl)s %(pd)s/libexec/logpost.py ../uberctrl/config.des %(bl)s j $JOB $RETURN
#""" % ({'bl': block, 'bldag': blockdag, 'pd': pfwdir}))
#
#        config.inc_blknum()
#            
#    config.reset_blknum()
#    config.set_block_info()
#
#    print blockarray
#    print len(blockarray)
#    print range(0,len(blockarray)-1)
#    for i in range(0, len(blockarray)-1):
#        dagfh.write('PARENT %s CHILD %s\n', blockarray[i], blockarray[i+1])
#        
#    dagfh.close()
#    pfwcondor.add2dag(dag, 
#                      config.get_dag_cmd_opts(), 
#                      config.get_condor_attributes("processmngr"), 
#                      None, 
#                      sys.stdout)
#    return dag


######################################################################
def write_main_dag(config, maindag, blockdag):
    """ Writes main manager dag input file """
    pfwdir = config['processingfw_dir']
    project = config['project']
    run = config['submit_run']
    runsite = config['submit_node']

    dagfh = open(maindag, 'w')
    dagfh.write("DOT main.dot\n")

    dagfh.write("""
JOB begrun %s/share/condor/runtask.condor
VARS begrun exec="$(pfwdir)/libexec/begrun.py"
VARS begrun arguments="config.des"
VARS begrun pfwdir="%s"
VARS begrun project="%s" run="%s" runsite="%s"
VARS begrun block="uberctrl" jobname="begrun"
""" % (pfwdir, pfwdir, project, run, runsite))

    dagfh.write("""
JOB blockmngr %s.condor.sub
VARS blockmngr pfwdir="%s"
VARS blockmngr project="%s" run="%s" runsite="%s"
VARS blockmngr block="uberctrl" jobname="blockmngr"
SCRIPT pre blockmngr %s/libexec/blockpre.py config.des 
SCRIPT post blockmngr %s/libexec/blockpost.py config.des $RETURN
RETRY blockmngr 5 UNLESS-EXIT %s
""" % (blockdag, pfwdir, project, run, runsite, pfwdir, pfwdir, PF_EXIT_FAILURE))

    dagfh.write("""
JOB endrun %s/share/condor/runtask.condor
VARS endrun exec="$(pfwdir)/libexec/endrun.py"
VARS endrun arguments="config.des"
VARS endrun pfwdir="%s"
VARS endrun project="%s" run="%s" runsite="%s"
VARS endrun block="uberctrl" jobname="endrun"
""" % (pfwdir, pfwdir, project, run, runsite))

    dagfh.write("""
PARENT begrun CHILD blockmngr
PARENT blockmngr CHILD endrun
""")


    dagfh.close()
    pfwcondor.add2dag(maindag, config.get_dag_cmd_opts(), config.get_condor_attributes("mainmngr"), None, sys.stdout)


######################################################################
def run_sys_checks():
    """ Check valid system environemnt (e.g., condor setup) """
      
    ### Check for Condor in path as well as daemons running
    print '\tChecking for Condor....',
    try:
        pfwcondor.check_condor('7.4.0')
    except Exception as excpt:
        print "Error"
        raise excpt 
              
    print "Done"


######################################################################
def submit_main_dag(config, dagfile, logfh):
    """ Submit main DAG file to Condor"""
    (exitcode, outtuple) = pfwcondor.condor_submit('%s.condor.sub' % 
                            (dagfile))
    if exitcode or re.search('ERROR', outtuple[0]):
        sys.stderr.write('\n%s\n' % (outtuple[0]))

        logfh.write('\ncondor_submit %s.condor.sub\n%s\n' % 
                    (dagfile, outtuple[0]))
    else:
        print '\nImage processing successfully submitted:'
        print '\tRun = %s' % (config['submit_run'])
        if 'event_tag' in config:
            print '\tRun name = %s' % (config['event_tag'])
    print '\n'

    # for completeness, log condorid of pipeline manager
    dagjob = pfwcondor.parse_condor_user_log('%s/%s.dagman.log' % 
                        (config['uberctrl_dir'], dagfile))
    jobids = dagjob.keys()
    condorid = None
    if len(jobids) == 1:
        condorid = int(jobids[0])
    pfwlog.log_pfw_event(config, 'analysis', 'j', 'mngr', 'pretask')
    pfwlog.log_pfw_event(config, 'analysis', 'j', 'mngr', 
                         {'cid': condorid})

    return condorid


######################################################################
def create_submitside_dirs(config):
    """ Create directories for storage of pfw files on submit side """
    # make local working dir
    workdir = config['work_dir']
    debug(3, 'PFWSUBMIT_DEBUG', "workdir = %s" % workdir)

    if os.path.exists(workdir):
        raise Exception('%s subdirectory already exists.\nAborting submission' % (workdir))

    print '\tMaking submit run directory...',
    os.makedirs(workdir)
    print 'DONE'

    uberdir = config['uberctrl_dir']
    debug(3, 'PFWSUBMIT_DEBUG', "uberdir = %s" % uberdir)
    print '\tMaking submit uberctrl directory...',
    os.makedirs(uberdir)
    print 'DONE'


if __name__ == "__main__":
    pass
