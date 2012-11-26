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

import processingfw.pfwcondor as pfwcondor
import processingfw.pfwlog as pfwlog
import processingfw.pfwdb as pfwdb
from processingfw.pfwutils import debug


######################################################################
def write_block_dag(config, block, debugfh=None):
    """  writes block dag file """

    if not debugfh:
        debugfh = sys.stderr
    
    debugfh.write('write_block_dag pwd: %s\n' % os.getcwd())

    pfwdir = config['processingfw_dir']

    if not os.path.exists('../%s' % block):
        os.mkdir('../%s' % block)
    os.chdir('../%s' % block)
    print "curr dir = ", os.getcwd()


    jobmngr = write_stub_jobmngr_dag(config, block, debugfh)
    dag = config.get_filename('mngrdag', {'currentvals': {'dagtype': 'block'}})

    run = config['submit_run']
    dagfh = open(dag, 'w')
    dagfh.write('DOT %s_block.dot\n' % run)

    dagfh.write('JOB begblock %s/share/condor/localjob.condor\n' % pfwdir)
    dagfh.write('VARS begblock exec="%s/libexec/begblock.py"\n' % pfwdir)
    dagfh.write('VARS begblock args="../uberctrl/config.des"\n')
    dagfh.write('VARS begblock jobname="begblock"\n')
    dagfh.write('VARS begblock run="%s"\n' % run)
    dagfh.write('SCRIPT pre begblock %s/libexec/logpre.py ../uberctrl/config.des %s j $JOB\n' % (pfwdir, block))
    dagfh.write('SCRIPT post begblock %s/libexec/logpost.py ../uberctrl/config.des %s j $JOB $RETURN\n' % (pfwdir, block))  

    dagfh.write('\n')
    dagfh.write('JOB jobmngr %s.condor.sub\n' % jobmngr)
    dagfh.write('SCRIPT pre jobmngr %s/libexec/logpre.py ../uberctrl/config.des %s j $JOB\n' % (pfwdir, block))
    dagfh.write('SCRIPT post jobmngr %s/libexec/logpost.py ../uberctrl/config.des %s j $JOB $RETURN\n' % (pfwdir, block))
    dagfh.write('PARENT begblock CHILD jobmngr\n')

    dagfh.close()
    pfwcondor.add2dag(dag, config.get_dag_cmd_opts(), config.get_condor_attributes("blockmngr"), "../%s" % block, debugfh)
    os.chdir('../uberctrl')
    return dag


######################################################################
def write_stub_jobmngr_dag(config, block, debugfh=None):
    """  writes stub jobmngr dag file to be overwritten during block """

    if not debugfh:
        debugfh = sys.stderr

    debugfh.write('write_stub_jobmngr pwd: %s\n' % os.getcwd())

    pfwdir = config['processingfw_dir']
    dag = config.get_filename('mngrdag', {'currentvals': {'dagtype': 'jobmngr'}})

    dagfh = open(dag, 'w')
    dagfh.write('DOT jobmngr.dot\n')
    dagfh.write('JOB 0001 %s/share/condor/localjob.condor\n' % pfwdir)
    dagfh.write('SCRIPT pre 0001 %s/libexec/logpre.py ../uberctrl/config.des %s j $JOB' % (pfwdir, block))
    dagfh.write('SCRIPT post 0001 %s/libexec/logpost.py ../uberctrl/config.des %s j $JOB $RETURN' % (pfwdir, block))
    dagfh.close()

    pfwcondor.add2dag(dag, config.get_dag_cmd_opts(), config.get_condor_attributes("jobmngr"), "../%s" % block, debugfh)

    os.unlink(dag)
    return dag


######################################################################
def write_process_dag(config, dag):
    """ write the process manager dag input file """
    pfwdir = config['processingfw_dir']
    blockarray = config.block_array

    dagfh = open(dag, 'w')
    dagfh.write('DOT process.dot\n')
    for block in blockarray:
        config.set_block_info()
        blockdag = write_block_dag(config, block, sys.stdout)
        dagfh.write("""
JOB %(bl)s ../%(bl)s/%(bldag)s.condor.sub
SCRIPT pre %(bl)s %(pd)s/libexec/logpre.py ../uberctrl/config.des %(bl)s j $JOB
SCRIPT post %(bl)s %(pd)s/libexec/logpost.py ../uberctrl/config.des %(bl)s j $JOB $RETURN
""" % ({'bl': block, 'bldag': blockdag, 'pd': pfwdir}))

        config.inc_blknum()
            
    config.reset_blknum()
    config.set_block_info()

    print blockarray
    print len(blockarray)
    print range(0,len(blockarray)-1)
    for i in range(0, len(blockarray)-1):
        dagfh.write('PARENT %s CHILD %s\n', blockarray[i], blockarray[i+1])
        
    dagfh.close()
    pfwcondor.add2dag(dag, 
                      config.get_dag_cmd_opts(), 
                      config.get_condor_attributes("processmngr"), 
                      None, 
                      sys.stdout)
    return dag


######################################################################
def write_main_dag(config, maindag, processdag):
    """ Writes main manager dag input file """
    pfwdir = config['processingfw_dir']
    project = config['project']
    run = config['submit_run']
    runsite = config['submit_node']

    dagfh = open(maindag, 'w')
    dagfh.write("DOT main.dot\n")

    dagfh.write("""
JOB processmngr %s.condor.sub
VARS processmngr pfwdir="%s" descfg="config.des" 
VARS processmngr project="%s" run="%s" runsite="%s"
VARS processmngr block="main" jobname="processmngr"
SCRIPT pre processmngr %s/libexec/logpre.py config.des mainmngr j $JOB 
SCRIPT post processmngr %s/libexec/summary.py config.des $RETURN
""" % (processdag, pfwdir, project, run, runsite, pfwdir, pfwdir))

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
    if len(jobids) == 1:
        pfwlog.log_pfw_event(config, 'analysis', 'j', 'mngr', 'pretask')
        pfwlog.log_pfw_event(config, 'analysis', 'j', 'mngr', 
                             {'cid': int(jobids[0])})
        dbh = pfwdb.PFWDB()
        dbh.update_attempt_cid(config, int(jobids[0]))


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
