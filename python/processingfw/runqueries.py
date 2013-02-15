#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import os
import subprocess
import time

from processingfw.pfwdefs import *
from processingfw.fwutils import *
import processingfw.pfwconfig as pfwconfig
from processingfw.pfwlog import log_pfw_event
import processingfw.pfwblock as pfwblock



###########################################################
def create_master_list(config, modname, moddict, 
        search_name, search_dict):
    print "\tcreate_master_list: BEG"


    if 'qouttype' in search_dict:
        qouttype = search_dict['qouttype']
    else:
        qouttype = 'wcl'  

    qoutfile = config.get_filename('qoutput', {PF_CURRVALS: 
        {'modulename': modname, 
         'searchname': search_name, 
         'suffix': qouttype}})
    qlog = config.get_filename('qoutput', {PF_CURRVALS: 
        {'modulename': modname, 
         'searchname': search_name, 
         'suffix': 'out'}})

    prog = None
    if 'exec' in search_dict:
        prog = search_dict['exec']
        if 'args' not in search_dict:
            print "\t\tWarning:  %s in module %s does not have args defined\n" % \
                   (search_name, modname)
            args = ""
        else:
            args = search_dict['args']
    elif 'query_fields' in search_dict:
        if 'processingfw_dir' in config:
            dirgenquery = config['processingfw_dir']
        elif 'PROCESSINGFW_DIR' in os.environ:
            dirgenquery = os.environ['PROCESSINGFW_DIR']
        else:
            fwdie("Could not determine base path for genquerydb.py")

        prog = "%s/libexec/genquerydb.py" % (dirgenquery)
        args = "--qoutfile %s --qouttype %s --config %s --module %s --search %s" % \
               (qoutfile, qouttype, config['wclfile'], modname, search_name)

    if not prog:
        print "\tWarning: %s in module %s does not have exec or %s defined" % (search_name, modname, SW_QUERYFIELDS)
        return

    search_dict['qoutfile'] = qoutfile
    search_dict['qlog'] = qlog

    prog = config.interpolate(prog, {PF_CURRVALS:{SW_MODULESECT:modname}, 
                              'searchobj':search_dict})

    # handle both outputxml and outputfile args
    args = config.interpolate(args, {PF_CURRVALS:{SW_MODULESECT:modname, 
                              'outputxml':qoutfile, 'outputfile':qoutfile, 
                              'qoutfile':qoutfile}, 
                              'searchobj':search_dict})


    # call code
    cwd = os.getcwd()
    print "\t\tCalling code to create master list for obj %s in module %s" % \
           (search_name, modname)
    print "\t\t", prog, args
    print "\t\tSee output in %s/%s" % (cwd, qlog)
    print "\t\tSee master list will be in %s/%s" % (cwd, qoutfile)

    print "\t\tCreating master list - start ", time.time()
    outfh = open(qlog, "w")
    cmd = "%s %s" % (prog, args)
    process = subprocess.Popen(cmd.split(), shell=False, 
                               stdout=outfh, stderr=subprocess.STDOUT)
    process.wait()
    outfh.close()
#    print process.communicate()
    print "\t\texit=", process.returncode
    print "\t\tCreating master list - end ", time.time()

    if process.returncode != 0:
        fwdie("Error: problem creating master list\n%s" % (cmd))

    print "\tcreate_master_list: END\n"



def runqueries(config, modname, modules_prev_in_list):
    moddict = config[SW_MODULESECT][modname]
    
    # process each "list" in each module
    if SW_LISTSECT in moddict:
        uber_list_dict = moddict[SW_LISTSECT]
        if 'list_order' in moddict:
            listorder = fwsplit(moddict['list_order'].lower())
        else:
            listorder = uber_list_dict.keys()
    
        for listname in listorder:
            list_dict = uber_list_dict[listname]
            if 'depends' not in list_dict or \
                list_dict['depends'] not in modules_prev_in_list:
                print "\t%s-%s: creating master list\n" % \
                      (modname, listname)
                create_master_list(config, modname, 
                                   moddict, listname, list_dict)
    
    # process each "file" in each module
    if SW_FILESECT in moddict:
        for filename, file_dict in moddict[SW_FILESECT].items():
            if 'depends' not in file_dict or \
                not file_dict['depends'] not in modules_prev_in_list:
                print "\t%s-%s: creating master list\n" % \
                      (modname, filename)
                create_master_list(config, modname, 
                                   moddict, filename, file_dict)

def main(argv = None):
    if argv is None:
        argv = sys.argv

    if len(argv) != 3:
        fwdie("Usage: runqueries.pl configfile condorjobid\n")

    configfile = argv[1]
    condorid = argv[2]

    config = pfwconfig.PfwConfig({'wclfile': configfile})
    # log condor jobid
    log_pfw_event(config, config['curr_block'], 'runqueries', 'j', ['cid', condorid])

    if SW_MODULELIST not in config:
        fwdie("Error:  No modules to run.")
    
    ### Get master lists and files calling external codes when needed
    
    modulelist = fwsplit(config[SW_MODULELIST].lower())
    
    modules_prev_in_list = {}
    for modname in modulelist:
        if modname not in config[SW_MODULESECT]:
            fwdie("Error: Could not find module description for module %s\n" % (modname))
        runqueries(config, modname, modules_prev_in_list)
        modules_prev_in_list[modname] = True
        
    pfwblock.read_master_lists(config)
    pfwblock.create_stage_archive_list(config)
    return(0)
    
    
if __name__ == "__main__":
    sys.exit(main(sys.argv))
