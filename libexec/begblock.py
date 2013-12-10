#!/usr/bin/env python
# Id:
# Rev::                                  :  # Revision of last commit.
# LastChangedBy::                        :  # Author of last commit. 
# LastChangedDate::                      :  # Date of last commit.

import sys
import os

from processingfw.pfwdefs import *
from coreutils.miscutils import *
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

    os.chdir('../%s' % config['blockname'])

    # now that have more information, can rename output file
    #fwdebug(0, 'PFWBLOCK_DEBUG', "getting new_log_name")
    #new_log_name = config.get_filename('block', {PF_CURRVALS:
    #                                                {'flabel': 'begblock',
    #                                                 'fsuffix':'out'}})
    #new_log_name = "../%s/%s" % (config['blockname'], new_log_name)
    #fwdebug(0, 'PFWBLOCK_DEBUG', "new_log_name = %s" % new_log_name)

    #debugfh = open(new_log_name, 'a+')
    #sys.stdout = debugfh
    #sys.stderr = debugfh

    if 'des_services' in config and config['des_services'] is not None:
        os.environ['DES_SERVICES'] = config['des_services']
    if 'des_db_section' in config:
        os.environ['DES_DB_SECTION'] = config['des_db_section']

    fwdebug(3, 'PFWBLOCK_DEBUG', "blknum = %s" % (config[PF_BLKNUM]))
    if convertBool(config[PF_USE_DB_OUT]): 
        dbh = pfwdb.PFWDB(config['des_services'], config['des_db_section'])

#   Moved insert_block into blockpre
#        dbh.insert_block(config)
#
#        if config[PF_BLKNUM] == '0':
#            fwdebug(3, 'PFWBLOCK_DEBUG', "Calling update_attempt_beg")
#            dbh.update_attempt_beg(config)

    modulelist = fwsplit(config[SW_MODULELIST].lower())
    modules_prev_in_list = {}
    inputfiles = []
    outputfiles = []
    tasks = []

    joblist = {} 
    for modname in modulelist:
        if modname not in config[SW_MODULESECT]:
            fwdie("Error: Could not find module description for module %s\n" % (modname), PF_EXIT_FAILURE)

        if convertBool(config[PF_USE_DB_OUT]): 
            dbh.insert_block_task(config, "runqueries_%s" % modname)
        runqueries(config, modname, modules_prev_in_list)
        if convertBool(config[PF_USE_DB_OUT]): 
            dbh.update_block_task_end(config, PF_EXIT_SUCCESS)
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

    fwdebug(0, "PFWBLOCK_DEBUG", "Creating job files - BEG")
    for jobkey,jobdict in sorted(joblist.items()):
        jobdict['jobnum'] = config.inc_jobnum()
        fwdebug(3, "PFWBLOCK_DEBUG", "jobnum = %s, jobkey = %s:" % (jobkey, jobdict['jobnum']))
        jobdict['tasksfile'] = pfwwrappers.write_workflow_taskfile(config, jobdict['jobnum'], jobdict['tasks'])
        jobdict['inputwcltar'] = pfwblock.tar_inputfiles(config, jobdict['jobnum'], jobdict['inlist'])
        (jobdict['jobwclfile'], jobdict['outputwcltar']) = pfwblock.write_jobwcl(config, jobkey, jobdict['jobnum'], len(jobdict['tasks']), jobdict['wrapinputs'])
    fwdebug(0, "PFWBLOCK_DEBUG", "Creating job files - END")

    numjobs = len(joblist)
    if convertBool(config[PF_USE_DB_OUT]): 
        dbh.update_block_numexpjobs(config, numjobs)

    fwdebug(6, "PFWBLOCK_DEBUG", "inputfiles: %s, %s" % (type(inputfiles), inputfiles))
    fwdebug(6, "PFWBLOCK_DEBUG", "outputfiles: %s, %s" % (type(outputfiles), outputfiles))
    files2stage = set(inputfiles) - set(outputfiles)
    pfwblock.stage_inputs(config, files2stage)    


    if USE_HOME_ARCHIVE_OUTPUT in config and config[USE_HOME_ARCHIVE_OUTPUT].lower() == 'block':
        config['block_outputlist'] = 'potential_outputfiles.list'
        pfwblock.write_output_list(config, outputfiles)


    dagfile = config.get_filename('jobdag')
    pfwblock.create_jobmngr_dag(config, dagfile, scriptfile, joblist)

    config.save_file(configfile)   # save config, have updated jobnum, wrapnum, etc

    if PF_DRYRUN in config and convertBool(config[PF_DRYRUN]):
        retval = PF_EXIT_DRYRUN
    else:
        retval = PF_EXIT_SUCCESS
    fwdebug(0, 'PFWBLOCK_DEBUG', "END - exiting with code %s" % retval)
    return(retval)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: begblock.py configfile"
        sys.exit(PF_EXIT_FAILURE)

    print ' '.join(sys.argv)    # print command so can run by hand if needed
    sys.stdout.flush()
    sys.exit(begblock(sys.argv[1:]))
