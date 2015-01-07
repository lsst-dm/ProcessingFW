#!/usr/bin/env python
# Id:
# Rev::                                  :  # Revision of last commit.
# LastChangedBy::                        :  # Author of last commit. 
# LastChangedDate::                      :  # Date of last commit.

import sys
import os

import processingfw.pfwdefs as pfwdefs 
import coreutils.miscutils as coremisc 
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwutils as pfwutils
from processingfw.runqueries import runqueries
import processingfw.pfwwrappers as pfwwrappers
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
    coremisc.fwdebug(3, 'PFWBLOCK_DEBUG', "blknum = %s" % (config[pfwdefs.PF_BLKNUM]))
    if coremisc.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
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
        modulelist = coremisc.fwsplit(config[pfwdefs.SW_MODULELIST].lower())
        modules_prev_in_list = {}
        inputfiles = []
        outputfiles = []
        tasks = []
    
        joblist = {} 
        for modname in modulelist:
            if modname not in config[pfwdefs.SW_MODULESECT]:
                coremisc.fwdie("Error: Could not find module description for module %s\n" % (modname), pfwdefs.PF_EXIT_FAILURE)
    
            task_id = -1
            runqueries(config, modname, modules_prev_in_list)
            pfwblock.read_master_lists(config, modname, modules_prev_in_list)
            pfwblock.create_fullnames(config, modname)
            pfwblock.add_file_metadata(config, modname)
            pfwblock.create_sublists(config, modname)
            loopvals = pfwblock.get_wrapper_loopvals(config, modname)
            wrapinst = pfwblock.create_wrapper_inst(config, modname, loopvals)
            pfwblock.assign_data_wrapper_inst(config, modname, wrapinst)
            modinputs, modoutputs = pfwblock.finish_wrapper_inst(config, modname, wrapinst)
            inputfiles.extend(modinputs)
            outputfiles.extend(modoutputs)
            pfwblock.create_module_wrapper_wcl(config, modname, wrapinst)
            pfwblock.divide_into_jobs(config, modname, wrapinst, joblist)
            modules_prev_in_list[modname] = True
    
        scriptfile = pfwblock.write_runjob_script(config)
    
        coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "Creating job files - BEG")
        for jobkey,jobdict in sorted(joblist.items()):
            jobdict['jobnum'] = pfwutils.pad_jobnum(config.inc_jobnum())
            jobdict['jobkeys'] = jobkey
            jobdict['numexpwrap'] = len(jobdict['tasks'])
            coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "jobnum = %s, jobkey = %s:" % (jobkey, jobdict['jobnum']))
            jobdict['tasksfile'] = pfwwrappers.write_workflow_taskfile(config, jobdict['jobnum'], jobdict['tasks'])
            jobdict['inputwcltar'] = pfwblock.tar_inputfiles(config, jobdict['jobnum'], jobdict['inlist'])
            if coremisc.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
                dbh.insert_job(config, jobdict)
            #(jobdict['jobwclfile'], jobdict['outputwcltar'], jobdict['envfile']) = pfwblock.write_jobwcl(config, jobkey, jobdict['jobnum'], len(jobdict['tasks']), jobdict['wrapinputs'])
            pfwblock.write_jobwcl(config, jobkey, jobdict)
        coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "Creating job files - END")
    
        numjobs = len(joblist)
        if coremisc.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
            dbh.update_block_numexpjobs(config, numjobs)
    
        coremisc.fwdebug(6, "PFWBLOCK_DEBUG", "inputfiles: %s, %s" % (type(inputfiles), inputfiles))
        coremisc.fwdebug(6, "PFWBLOCK_DEBUG", "outputfiles: %s, %s" % (type(outputfiles), outputfiles))
        files2stage = set(inputfiles) - set(outputfiles)
        pfwblock.stage_inputs(config, files2stage)    
    
    
        if pfwdefs.USE_HOME_ARCHIVE_OUTPUT in config and config[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower() == 'block':
            config['block_outputlist'] = 'potential_outputfiles.list'
            pfwblock.write_output_list(config, outputfiles)
    
    
        dagfile = config.get_filename('jobdag')
        pfwblock.create_jobmngr_dag(config, dagfile, scriptfile, joblist)
    except:
        retval = pfwdefs.PF_EXIT_FAILURE
        config.save_file(configfile)   # save config, have updated jobnum, wrapnum, etc
        if coremisc.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
            dbh.end_task(config['task_id']['begblock'], retval, True)
            dbh.end_task(blktid, retval, True)
        raise
        
    
    config.save_file(configfile)   # save config, have updated jobnum, wrapnum, etc

    if pfwdefs.PF_DRYRUN in config and coremisc.convertBool(config[pfwdefs.PF_DRYRUN]):
        retval = pfwdefs.PF_EXIT_DRYRUN
    else:
        retval = pfwdefs.PF_EXIT_SUCCESS
    if coremisc.convertBool(config[pfwdefs.PF_USE_DB_OUT]): 
        dbh.end_task(config['task_id']['begblock'], retval, True)
    coremisc.fwdebug(0, 'PFWBLOCK_DEBUG', "END - exiting with code %s" % retval)
    return(retval)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: begblock.py configfile"
        sys.exit(pfwdefs.PF_EXIT_FAILURE)

    print ' '.join(sys.argv)    # print command so can run by hand if needed
    sys.stdout.flush()
    sys.exit(begblock(sys.argv[1:]))
