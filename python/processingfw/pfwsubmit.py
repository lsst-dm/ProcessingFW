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

from coreutils.miscutils import *
from processingfw.pfwutils import *
from processingfw.pfwdefs import *
import processingfw.pfwcondor as pfwcondor
import processingfw.pfwlog as pfwlog
import processingfw.pfwdb as pfwdb


def create_common_vars(config, jobname):
    attribs = config.get_condor_attributes(jobname)
    varstr = ""
    if len(attribs) > 0:
        varstr = "VARS %s" % jobname
        for (key,val) in attribs.items():
            varstr += ' %s="%s"' % (key[len(ATTRIB_PREFIX):], val)
    varstr += ' jobname="%s"' % jobname
    varstr += ' pfwdir="%s"' % config['processingfw_dir']

    return varstr
    

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


    dagfh = open(dag, 'w')

    dagfh.write('JOB begblock blocktask.condor\n')
    dagfh.write('VARS begblock exec="$(pfwdir)/libexec/begblock.py"\n')
    dagfh.write('VARS begblock args="../uberctrl/config.des"\n')
    varstr = create_common_vars(config, 'begblock')
    dagfh.write('%s\n' % varstr)
    dagfh.write('SCRIPT pre begblock %s/libexec/logpre.py ../uberctrl/config.des %s j $JOB\n' % (pfwdir, block))
    dagfh.write('SCRIPT post begblock %s/libexec/logpost.py ../uberctrl/config.des %s j $JOB $RETURN\n' % (pfwdir, block))  

    dagfh.write('\n')
    dagfh.write('JOB jobmngr %s.condor.sub\n' % jobmngr)
    dagfh.write('SCRIPT pre jobmngr %s/libexec/logpre.py ../uberctrl/config.des %s j $JOB\n' % (pfwdir, block))
    dagfh.write('SCRIPT post jobmngr %s/libexec/logpost.py ../uberctrl/config.des %s j $JOB $RETURN\n' % (pfwdir, block))

    dagfh.write('\n')
    dagfh.write('JOB endblock blocktask.condor\n')
    dagfh.write('VARS endblock exec="%s/libexec/endblock.py"\n' % pfwdir)
    dagfh.write('VARS endblock args="../uberctrl/config.des"\n')
    varstr = create_common_vars(config, 'endblock')
    dagfh.write('%s\n' % varstr)
    dagfh.write('SCRIPT pre endblock %s/libexec/logpre.py ../uberctrl/config.des %s j $JOB\n' % (pfwdir, block))
    dagfh.write('SCRIPT post endblock %s/libexec/logpost.py ../uberctrl/config.des %s j $JOB $RETURN\n' % (pfwdir, block))  

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
    dagfh.write('JOB 0001 %s/share/condor/localjob.condor\n' % pfwdir)
    dagfh.write('SCRIPT pre 0001 %s/libexec/logpre.py ../uberctrl/config.des %s j $JOB' % (pfwdir, block))
    dagfh.write('SCRIPT post 0001 %s/libexec/logpost.py ../uberctrl/config.des %s j $JOB $RETURN' % (pfwdir, block))
    dagfh.close()

#    pfwcondor.add2dag(dag, config.get_dag_cmd_opts(), config.get_condor_attributes("jobmngr"), "../%s" % block, debugfh)
    pfwcondor.add2dag(dag, config.get_dag_cmd_opts(), config.get_condor_attributes("jobmngr"), None, debugfh)

    os.unlink(dag)
    return dag


######################################################################
def write_main_dag(config, maindag, blockdag):
    """ Writes main manager dag input file """
    pfwdir = config['processingfw_dir']

    dagfh = open(maindag, 'w')

    dagfh.write("""
JOB begrun %s/share/condor/runtask.condor
VARS begrun exec="$(pfwdir)/libexec/begrun.py"
VARS begrun arguments="../uberctrl/config.des"
""" % (pfwdir))
    varstr = create_common_vars(config, 'begrun')
    dagfh.write('%s\n' % varstr)

    dagfh.write("""
JOB blockmngr %s.condor.sub
SCRIPT pre blockmngr %s/libexec/blockpre.py ../uberctrl/config.des 
SCRIPT post blockmngr %s/libexec/blockpost.py ../uberctrl/config.des $RETURN
RETRY blockmngr %s UNLESS-EXIT %s
""" % (blockdag, pfwdir, pfwdir, config['num_blocks'], PF_EXIT_FAILURE))
    varstr = create_common_vars(config, 'blockmngr')
    dagfh.write('%s\n' % varstr)

    dagfh.write("""
JOB endrun %s/share/condor/runtask.condor
VARS endrun exec="$(pfwdir)/libexec/endrun.py"
VARS endrun arguments="../uberctrl/config.des"
""" % (pfwdir))
    varstr = create_common_vars(config, 'endrun')
    dagfh.write('%s\n' % varstr)

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
        print "ERROR"
        raise excpt 
              
    print "DONE"


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
    fwdebug(3, 'PFWSUBMIT_DEBUG', "workdir = %s" % workdir)

    if os.path.exists(workdir):
        raise Exception('%s subdirectory already exists.\nAborting submission' % (workdir))

    print '\tMaking submit run directory...',
    coremakedirs(workdir)
    print 'DONE'

    uberdir = config['uberctrl_dir']
    fwdebug(3, 'PFWSUBMIT_DEBUG', "uberdir = %s" % uberdir)
    print '\tMaking submit uberctrl directory...',
    coremakedirs(uberdir)
    print 'DONE'


if __name__ == "__main__":
    pass
