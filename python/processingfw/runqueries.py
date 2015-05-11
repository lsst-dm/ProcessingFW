#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

import sys
import os
import subprocess
import time
import traceback

import processingfw.pfwdefs as pfwdefs
import intgutils.intgdefs as intgdefs
import processingfw.pfwutils as pfwutils
import despymisc.miscutils as miscutils
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwdb as pfwdb
from processingfw.pfwlog import log_pfw_event
import processingfw.pfwblock as pfwblock



###########################################################
def create_master_list(config, modname, moddict, 
        search_name, search_dict, search_type):
    miscutils.fwdebug(0, "RUNQUERIES_DEBUG", "BEG")

    if 'qouttype' in search_dict:
        qouttype = search_dict['qouttype']
    else:
        qouttype = intgdefs.DEFAULT_QUERY_OUTPUT_FORMAT

    qoutfile = config.get_filename('qoutput', {pfwdefs.PF_CURRVALS: 
        {'modulename': modname, 
         'searchname': search_name, 
         'suffix': qouttype}})
    qlog = config.get_filename('qoutput', {pfwdefs.PF_CURRVALS: 
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
            miscutils.fwdie("Error: Could not determine base path for genquerydb.py", pfwdefs.PF_EXIT_FAILURE)

        prog = "%s/libexec/genquerydb.py" % (dirgenquery)
        args = "--qoutfile %s --qouttype %s --config %s --module %s --search %s" % \
               (qoutfile, qouttype, config['wclfile'], modname, search_name)

    if not prog:
        print "\tWarning: %s in module %s does not have exec or %s defined" % (search_name, modname, pfwdefs.SW_QUERYFIELDS)
        return

    search_dict['qoutfile'] = qoutfile
    search_dict['qlog'] = qlog

    prog = config.interpolate(prog, {pfwdefs.PF_CURRVALS:{pfwdefs.SW_MODULESECT:modname}, 
                              'searchobj':search_dict})

    # handle both outputxml and outputfile args
    args = config.interpolate(args, {pfwdefs.PF_CURRVALS:{pfwdefs.SW_MODULESECT:modname, 
                              'outputxml':qoutfile, 'outputfile':qoutfile, 
                              'qoutfile':qoutfile}, 
                              'searchobj':search_dict})

    

    # get version for query code
    query_version = None
    if prog in config[pfwdefs.SW_EXEC_DEF]:
        query_version = pfwutils.get_version(prog, config[pfwdefs.SW_EXEC_DEF])

    if search_type == pfwdefs.SW_LISTSECT:
        datatype = 'L'
    elif search_type == pfwdefs.SW_FILESECT:
        datatype = 'F'
    else:
        datatype = search_type[0].upper()

    # call code
    query_tid = None
    if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]):
        pfw_dbh = pfwdb.PFWDB()
        query_tid = pfw_dbh.insert_data_query(config, modname, datatype, search_name,
                                              prog, args, query_version)
        pfw_dbh.close()
    

    cwd = os.getcwd()
    print "\t\tCalling code to create master list for obj %s in module %s" % \
           (search_name, modname)
    print "\t\t", prog, args
    print "\t\tSee output in %s/%s" % (cwd, qlog)
    print "\t\tSee master list will be in %s/%s" % (cwd, qoutfile)

    print "\t\tCreating master list - start ", time.time()

    cmd = "%s %s" % (prog, args)
    exitcode = None
    try:
        exitcode = pfwutils.run_cmd_qcf(cmd, qlog, query_tid, os.path.basename(prog), 5000, config[pfwdefs.PF_USE_QCF])
    except:
        print "******************************"
        print "Error: "
        (type, value, trback) = sys.exc_info()
        print "******************************"
        traceback.print_exception(type, value, trback, file=sys.stdout)
        exitcode = pfwdefs.PF_EXIT_FAILURE

    print "\t\tCreating master list - end ", time.time()
    sys.stdout.flush()
    if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]):
        pfw_dbh = pfwdb.PFWDB()
        pfw_dbh.end_task(query_tid, exitcode, True)
        pfw_dbh.close()

    if exitcode != 0:
        miscutils.fwdie("Error: problem creating master list (exitcode = %s)" % (exitcode), exitcode)
    
    miscutils.fwdebug(0, "RUNQUERIES_DEBUG", "END")




def runqueries(config, modname, modules_prev_in_list):
    moddict = config[pfwdefs.SW_MODULESECT][modname]
    
    # process each "list" in each module
    if pfwdefs.SW_LISTSECT in moddict:
        uber_list_dict = moddict[pfwdefs.SW_LISTSECT]
        if 'list_order' in moddict:
            listorder = miscutils.fwsplit(moddict['list_order'].lower())
        else:
            listorder = uber_list_dict.keys()
    
        for listname in listorder:
            list_dict = uber_list_dict[listname]
            if 'depends' not in list_dict or \
                list_dict['depends'] not in modules_prev_in_list:
                print "\t%s-%s: creating master list\n" % \
                      (modname, listname)
                create_master_list(config, modname, 
                                   moddict, listname, list_dict, pfwdefs.SW_LISTSECT)
    
    # process each "file" in each module
    if pfwdefs.SW_FILESECT in moddict:
        for filename, file_dict in moddict[pfwdefs.SW_FILESECT].items():
            if 'depends' not in file_dict or \
                not file_dict['depends'] not in modules_prev_in_list:
                print "\t%s-%s: creating master list\n" % \
                      (modname, filename)
                create_master_list(config, modname, 
                                   moddict, filename, file_dict, pfwdefs.SW_FILESECT)

def main(argv = None):
    if argv is None:
        argv = sys.argv

    if len(argv) != 3:
        miscutils.fwdie("Usage: runqueries.pl configfile condorjobid\n", pfwdefs.PF_EXIT_FAILURE)

    configfile = argv[1]
    condorid = argv[2]

    config = pfwconfig.PfwConfig({'wclfile': configfile})
    # log condor jobid
    log_pfw_event(config, config['curr_block'], 'runqueries', 'j', ['cid', condorid])

    if pfwdefs.SW_MODULELIST not in config:
        miscutils.fwdie("Error:  No modules to run.", pfwdefs.PF_EXIT_FAILURE)
    
    ### Get master lists and files calling external codes when needed
    
    modulelist = miscutils.fwsplit(config[pfwdefs.SW_MODULELIST].lower())
    
    modules_prev_in_list = {}
    for modname in modulelist:
        if modname not in config[pfwdefs.SW_MODULESECT]:
            miscutils.fwdie("Error: Could not find module description for module %s\n" % (modname), pfwdefs.PF_EXIT_FAILURE)
        runqueries(config, modname, modules_prev_in_list)
        modules_prev_in_list[modname] = True
        
    pfwblock.read_master_lists(config, modname, modules_prev_in_list)
    #pfwblock.create_stage_archive_list(config)
    return(0)
    
    
if __name__ == "__main__":
    sys.exit(main(sys.argv))
