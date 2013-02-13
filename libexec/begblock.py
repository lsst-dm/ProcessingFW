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

#    os.chdir('../%s' % config['blockname'])

    if 'des_services' in config and config['des_services'] is not None:
        os.environ['DES_SERVICES'] = config['des_services']
    if 'des_db_section' in config:
        os.environ['DES_DB_SECTION'] = config['des_db_section']

    pfwutils.debug(3, 'PFWBLOCK_DEBUG', "blknum = %s" % (config[PF_BLKNUM]))
    if pfwutils.convertBool(config[PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['des_services'], config['des_db_section'])
        dbh.insert_block(config)

        if config[PF_BLKNUM] == '0':
            pfwutils.debug(3, 'PFWBLOCK_DEBUG', "Calling update_attempt_beg")
            dbh.update_attempt_beg(config)

    modulelist = pfwutils.pfwsplit(config[SW_MODULELIST].lower())
    modules_prev_in_list = {}
    tasks = []
    for modname in modulelist:
        if modname not in config[SW_MODULESECT]:
            raise Exception("Error: Could not find module description for module %s\n" % (modname))

        if pfwutils.convertBool(config[PF_USE_DB_OUT]): 
            dbh.insert_blktask(config, modname, "runqueries")
        runqueries(config, modname, modules_prev_in_list)
        if pfwutils.convertBool(config[PF_USE_DB_OUT]): 
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

    dagfile = config.get_filename('jobdag')
    numjobs = 1
    if pfwutils.convertBool(config[PF_USE_DB_OUT]): 
        dbh.update_block_numexpjobs(config, numjobs)
#    jobsdir = '../%s_tjobs' % config['blockname']
#    if not os.path.exists(jobsdir):
#        os.mkdir(jobsdir)

    pfwblock.create_jobmngr_dag(config, dagfile, scriptfile, tarfile, tasksfile, jobwclfile)

    config.save_file(configfile)   # save config, have updated jobnum, wrapnum, etc

#    os.rename(tarfile, "%s/%s" % (jobsdir, tarfile))
#    os.rename(tasksfile, "%s/%s" % (jobsdir, tasksfile))
#    os.rename(jobwclfile, "%s/%s" % (jobsdir, jobwclfile))
#    os.rename(scriptfile, "%s/%s" % (jobsdir, scriptfile))
    
    print "begblock done"
    if PF_DRYRUN in config and pfwutils.convertBool(config[PF_DRYRUN]):
        retval = PF_EXIT_DRYRUN
    else:
        retval = PF_EXIT_SUCCESS
    return(retval)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: begblock.py configfile"
        sys.exit(PF_EXIT_FAILURE)

    print ' '.join(sys.argv)
    sys.exit(begblock(sys.argv[1:]))
