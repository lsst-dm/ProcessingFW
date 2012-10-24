#!/usr/bin/env python
# Id:
# Rev::                                  :  # Revision of last commit.
# LastChangedBy::                        :  # Author of last commit. 
# LastChangedDate::                      :  # Date of last commit.

import sys

import processingfw.pfwconfig as pfwconfig
import processingfw.pfwutils as pfwutils
from processingfw.runqueries import runqueries
import processingfw.pfwwrappers as pfwwrappers
import processingfw.pfwblock as pfwblock

def begblock(argv):
    if argv == None:
        argv = sys.argv

    configfile = argv[0]
    config = pfwconfig.PfwConfig({'wclfile': configfile}) 
    config.set_block_info()

    module_list = pfwutils.pfwsplit(config['module_list'].lower())
    modules_prev_in_list = {}
    tasks = []
    for modname in module_list:
        if modname not in config['module']:
            raise Exception("Error: Could not find module description for module %s\n" % (modname))

        runqueries(config, modname, modules_prev_in_list)
        pfwblock.read_master_lists(config, modname, modules_prev_in_list)
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

    dagfile = config.get_filename('mngrdag', {'currentvals': {'dagtype': 'jobmngr'}})
    pfwblock.create_jobmngr_dag(config, dagfile, scriptfile, tarfile, tasksfile)

    return(pfwconfig.PfwConfig.SUCCESS)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: begblock.py configfile"
        sys.exit(pfwconfig.PfwConfig.FAILURE)

    print sys.argv
    sys.exit(begblock(sys.argv[1:]))
