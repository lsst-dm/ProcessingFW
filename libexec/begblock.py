#!/usr/bin/env python
# Id:
# Rev::                                  :  # Revision of last commit.
# LastChangedBy::                        :  # Author of last commit. 
# LastChangedDate::                      :  # Date of last commit.

import sys
import os

import processingfw.pfwconfig as pfwconfig
import processingfw.pfwutils as pfwutils
from processingfw.runqueries import runqueries
from processingfw.pfwdefs import *
import processingfw.pfwwrappers as pfwwrappers
import processingfw.pfwblock as pfwblock
import processingfw.pfwdb as pfwdb

def begblock(argv):
    if argv == None:
        argv = sys.argv

    configfile = argv[0]
    config = pfwconfig.PfwConfig({'wclfile': configfile}) 
    config.set_block_info()

    os.chdir('../%s' % config['blockname'])

    dbh = pfwdb.PFWDB()
    dbh.insert_block(config)

    pfwutils.debug(3, 'PFWBLOCK_DEBUG', "blknum = %s" % (config[PF_BLKNUM]))
    if config[PF_BLKNUM] == '0':
        pfwutils.debug(3, 'PFWBLOCK_DEBUG', "Calling update_attempt_beg")
        dbh.update_attempt_beg(config)

    modulelist = pfwutils.pfwsplit(config[SW_MODULELIST].lower())
    modules_prev_in_list = {}
    tasks = []
    for modname in modulelist:
        if modname not in config[SW_MODULESECT]:
            raise Exception("Error: Could not find module description for module %s\n" % (modname))

        dbh.insert_blktask(config, modname, "runqueries")
        runqueries(config, modname, modules_prev_in_list)
        dbh.update_blktask_end(config, modname, "runqueries", 1)
        pfwblock.read_master_lists(config, modname, modules_prev_in_list)
        pfwblock.add_file_metadata(config, modname)
        loopvals = pfwblock.get_wrapper_loopvals(config, modname)
        pfwblock.create_sublists(config, modname)
        wrapinst = pfwblock.create_wrapper_inst(config, modname, loopvals)
        pfwblock.assign_data_wrapper_inst(config, modname, wrapinst)
        pfwblock.finish_wrapper_inst(config, modname, wrapinst)
        tasks = tasks + pfwblock.create_module_wrapper_wcl(config, modname, wrapinst)
        modules_prev_in_list[modname] = True

    tasksfile = pfwwrappers.write_workflow_taskfile(config, tasks)
    tarfile = pfwblock.tar_inputwcl(config)
    scriptfile = pfwblock.write_runjob_script(config)
    jobwclfile = pfwblock.write_jobwcl(config, '1', len(tasks))

    dagfile = config.get_filename('mngrdag', {PF_CURRVALS: {'dagtype': 'jobmngr'}})
    numjobs = 1
    dbh.update_block_numexpjobs(config, numjobs)
    pfwblock.create_jobmngr_dag(config, dagfile, scriptfile, tarfile, tasksfile, jobwclfile)
    
    

    jobsdir = '../%s_tjobs' % config['blockname']
    if not os.path.exists(jobsdir):
        os.mkdir(jobsdir)
    os.rename(tarfile, "%s/%s" % (jobsdir, tarfile))
    os.rename(tasksfile, "%s/%s" % (jobsdir, tasksfile))
    os.rename(jobwclfile, "%s/%s" % (jobsdir, jobwclfile))
#    os.rename(scriptfile, "%s/%s" % (jobsdir, scriptfile))
    
    return(PF_SUCCESS)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: begblock.py configfile"
        sys.exit(PF_FAILURE)

    print ' '.join(sys.argv)
    sys.exit(begblock(sys.argv[1:]))
