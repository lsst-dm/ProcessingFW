#!/usr/bin/env python
# Id:
# Rev::                                  :  # Revision of last commit.
# LastChangedBy::                        :  # Author of last commit. 
# LastChangedDate::                      :  # Date of last commit.

import sys
import os
import time
from collections import OrderedDict

import processingfw.pfwdefs as pfwdefs 
import despymisc.miscutils as miscutils 
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwutils as pfwutils
from processingfw.runqueries import runqueries
#import processingfw.pfwwrappers as pfwwrappers
import processingfw.pfwblock as pfwblock
import processingfw.pfwdb as pfwdb

def begblock(argv):
    if argv == None:
        argv = sys.argv

    configfile = argv[0]
    config = pfwconfig.PfwConfig({'wclfile': configfile}) 
    config.set_block_info()
    blknum = config[pfwdefs.PF_BLKNUM]

    blkdir = config['block_dir']
    os.chdir(blkdir)


    if 'submit_des_services' in config and config['submit_des_services'] is not None:
        os.environ['DES_SERVICES'] = config['submit_des_services']
    if 'submit_des_db_section' in config and config['submit_des_db_section'] is not None:
        os.environ['DES_DB_SECTION'] = config['submit_des_db_section']

    blktid = -1
    begblktid = -1
    if miscutils.fwdebug_check(3, 'PFWBLOCK_DEBUG'):
        miscutils.fwdebug_print("blknum = %s" % (config[pfwdefs.PF_BLKNUM]))
    if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['submit_des_services'], config['submit_des_db_section'])
        dbh.insert_block(config)
        blktid = config['task_id']['block'][str(blknum)]
        config['task_id']['begblock'] = dbh.create_task(name = 'begblock',
                                      info_table = None,
                                      parent_task_id = blktid,
                                      root_task_id = int(config['task_id']['attempt']),
                                      label = None,
                                      do_begin = True,
                                      do_commit = True)

    
    try:
        modulelist = miscutils.fwsplit(config[pfwdefs.SW_MODULELIST].lower())
        modules_prev_in_list = {}
        inputfiles = []
        outputfiles = []
        tasks = []
    
        joblist = {} 
        masterdata = OrderedDict()
        for modname in modulelist:
            if modname not in config[pfwdefs.SW_MODULESECT]:
                miscutils.fwdie("Error: Could not find module description for module %s\n" % (modname), pfwdefs.PF_EXIT_FAILURE)
    
            task_id = -1
            runqueries(config, modname, modules_prev_in_list)
            pfwblock.read_master_lists(config, modname, masterdata, modules_prev_in_list)
            pfwblock.create_fullnames(config, modname, masterdata)
            pfwblock.add_file_metadata(config, modname)
            sublists = pfwblock.create_sublists(config, modname, masterdata)
            if sublists is not None:
                if miscutils.fwdebug_check(3, 'PFWBLOCK_DEBUG'):
                    miscutils.fwdebug_print("sublists.keys() = %s" % (sublists.keys()))
            loopvals = pfwblock.get_wrapper_loopvals(config, modname)
            wrapinst = pfwblock.create_wrapper_inst(config, modname, loopvals)
            infsect = pfwblock.which_are_inputs(config, modname)
            outfsect = pfwblock.which_are_outputs(config, modname)
            wcnt = 1
            for winst in wrapinst.values():
                stime = time.time()
                if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
                    miscutils.fwdebug_print("winst %d - BEG" % wcnt)
                pfwblock.assign_data_wrapper_inst(config, modname, winst, masterdata, sublists, infsect, outfsect)
                modinputs, modoutputs = pfwblock.finish_wrapper_inst(config, modname, winst, outfsect)
                inputfiles.extend(modinputs)
                outputfiles.extend(modoutputs)
                pfwblock.create_module_wrapper_wcl(config, modname, winst)
                pfwblock.divide_into_jobs(config, modname, winst, joblist)
                etime = time.time()
                if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
                    miscutils.fwdebug_print("winst %d - %s - END" % (wcnt, etime-stime))
                wcnt += 1
            modules_prev_in_list[modname] = True
    
        scriptfile = pfwblock.write_runjob_script(config)
    
        miscutils.fwdebug_print("Creating job files - BEG")
        for jobkey,jobdict in sorted(joblist.items()):
            jobdict['jobnum'] = pfwutils.pad_jobnum(config.inc_jobnum())
            jobdict['jobkeys'] = jobkey
            jobdict['numexpwrap'] = len(jobdict['tasks'])
            if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
                miscutils.fwdebug_print("jobnum = %s, jobkey = %s:" % (jobkey, jobdict['jobnum']))
            jobdict['tasksfile'] = write_workflow_taskfile(config, jobdict['jobnum'], jobdict['tasks'])
            jobdict['inputwcltar'] = pfwblock.tar_inputfiles(config, jobdict['jobnum'], jobdict['inlist'])
            if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
                dbh.insert_job(config, jobdict)
            #(jobdict['jobwclfile'], jobdict['outputwcltar'], jobdict['envfile']) = pfwblock.write_jobwcl(config, jobkey, jobdict['jobnum'], len(jobdict['tasks']), jobdict['wrapinputs'])
            pfwblock.write_jobwcl(config, jobkey, jobdict)
        miscutils.fwdebug_print("Creating job files - END")
    
        numjobs = len(joblist)
        if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
            dbh.update_block_numexpjobs(config, numjobs)
    
        if miscutils.fwdebug_check(6, 'PFWBLOCK_DEBUG'):
            miscutils.fwdebug_print("inputfiles: %s, %s" % (type(inputfiles), inputfiles))
            miscutils.fwdebug_print("outputfiles: %s, %s" % (type(outputfiles), outputfiles))
        files2stage = set(inputfiles) - set(outputfiles)
        pfwblock.stage_inputs(config, files2stage)    
    
    
        if pfwdefs.USE_HOME_ARCHIVE_OUTPUT in config and config[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower() == 'block':
            config['block_outputlist'] = 'potential_outputfiles.list'
            pfwblock.write_output_list(config, outputfiles)
    
    
        dagfile = config.get_filename('jobdag')
        pfwblock.create_jobmngr_dag(config, dagfile, scriptfile, joblist)
    except:
        retval = pfwdefs.PF_EXIT_FAILURE
        config.write(configfile)   # save config, have updated jobnum, wrapnum, etc
        if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
            dbh.end_task(config['task_id']['begblock'], retval, True)
            dbh.end_task(blktid, retval, True)
        raise
        
    
    config.write(configfile)   # save config, have updated jobnum, wrapnum, etc

    if pfwdefs.PF_DRYRUN in config and miscutils.convertBool(config[pfwdefs.PF_DRYRUN]):
        retval = pfwdefs.PF_EXIT_DRYRUN
    else:
        retval = pfwdefs.PF_EXIT_SUCCESS
    if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
        dbh.end_task(config['task_id']['begblock'], retval, True)
    miscutils.fwdebug_print("END - exiting with code %s" % retval)
    return(retval)


def write_workflow_taskfile(config, jobnum, tasks):
    taskfile = config.get_filename('jobtasklist', {pfwdefs.PF_CURRVALS:{'jobnum':jobnum},'required': True, 'interpolate': True})
    tjpad = pfwutils.pad_jobnum(jobnum)
    miscutils.coremakedirs(tjpad)
    with open("%s/%s" % (tjpad, taskfile), 'w') as tasksfh:
        for task in sorted(tasks, key=lambda singletask: int(singletask[0])):
            tasksfh.write("%s, %s, %s, %s, %s\n" % (task[0], task[1], task[2], task[3], task[4]))
    return taskfile



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: begblock.py configfile"
        sys.exit(pfwdefs.PF_EXIT_FAILURE)

    print ' '.join(sys.argv)    # print command so can run by hand if needed
    sys.stdout.flush()
    sys.exit(begblock(sys.argv[1:]))
