#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

""" Contains functions used to check submit wcl for missing or invalid values """

import sys
import re
import os
import time

from processingfw.pfwdefs import *
from processingfw.pfwutils import *
from coreutils.miscutils import *
from filemgmt.filemgmt_defs import *


#import intgutils.wclutils as wclutils
#import processingfw.pfwdb as pfwdb

NUMCNTS = 4
ERRCNT_POS = 0
WARNCNT_POS = 1
CHANGECNT_POS = 2
CLEANCNT_POS = 3


########################################################################### 
def check_globals(config, indent=''):
    """ Check global settings """


    print "%sChecking globals..." % (indent)

    # initialize counters
    cnts = [0 for i in range(0,NUMCNTS)]

    # always required
    # TODO: unitname might need to be expanded to discover missing variables ???
    for key in ['pipeline', 'pipeprod', 'pipever', 'project', REQNUM, ATTNUM, UNITNAME, 'jira_id', 'target_site',
                'site', 'filename_pattern', 'directory_pattern', 'job_file_mvmt', 'ops_run_dir', 
                PF_USE_QCF, PF_USE_DB_IN, PF_USE_DB_OUT, SW_BLOCKLIST, SW_BLOCKSECT, SW_MODULESECT, 'create_junk_tarball']:
        try:
            if key not in config:
                print "%s    Error: missing %s global key or section" % (indent, key)
                cnts[ERRCNT_POS] += 1
        except:
            print "%s    Error: missing %s global key or section" % (indent, key)
            cnts[ERRCNT_POS] += 1
            

    if PF_USE_DB_IN in config:
        if convertBool(config[PF_USE_DB_IN]):
            if 'submit_des_db_section' not in config:
                print "%s    Error:  using DB (%s), but missing submit_des_db_section" % (indent, PF_USE_DB_IN)
                cnts[ERRCNT_POS] += 1
            if 'submit_des_services' not in config:
                print "%s    Error:  using DB (%s), but missing submit_des_services" % (indent, PF_USE_DB_IN)
                cnts[ERRCNT_POS] += 1

    if PF_USE_DB_OUT in config:
        if convertBool(config[PF_USE_DB_OUT]):
            if 'submit_des_db_section' not in config:
                print "%s    Error:  using DB (%s), but missing submit_des_db_section" % (indent, PF_USE_DB_OUT)
                cnts[ERRCNT_POS] += 1
            if 'submit_des_services' not in config:
                print "%s    Error:  using DB (%s), but missing submit_des_services" % (indent, PF_USE_DB_OUT)
                cnts[ERRCNT_POS] += 1

    # if using QCF must also be writing run info into DB
    if PF_USE_QCF in config and convertBool(config[PF_USE_QCF]) and \
        (PF_USE_DB_OUT in config and not convertBool(config[PF_USE_DB_OUT])):
        print "%s    Error: if %s is true, %s must also be set to true" % (indent, PF_USE_QCF, PF_USE_DB_OUT)
        cnts[ERRCNT_POS] += 1

    if 'operator' not in config:
        print '%s    Error:  Must specify operator' % (indent)
        cnts[ERRCNT_POS] += 1
    elif config['operator'] in ['bcs']:
        print '%s    Error:  Operator cannot be shared login (%s).' % (indent, config['operator'])
        cnts[ERRCNT_POS] += 1

    blocklist = fwsplit(config[SW_BLOCKLIST].lower(),',')
    for blockname in blocklist:
        if blockname not in config[SW_BLOCKSECT]:
            print '%s    Error:  Invalid %s, bad block name (%s)' % (indent, SW_BLOCKLIST, blockname)
            cnts[ERRCNT_POS] += 1
            
    return cnts



########################################################################### 
def check_block(config, indent=''):
    """ check blocks level defs """ 

    cnts = [0 for i in range(0,NUMCNTS)]

    blocklist = fwsplit(config[SW_BLOCKLIST].lower(),',')
    for blockname in blocklist:
        print "%sChecking block %s..." % (indent, blockname)
        config.set_block_info()

        if (PF_USE_DB_IN in config and convertBool(config[PF_USE_DB_IN])):
            (found, val) = config.search('target_des_db_section')
            if not found:
                print "%s    Error:  using DB (%s), but missing target_des_db_section" % (indent, PF_USE_DB_IN)
                cnts[ERRCNT_POS] += 1

            (found, val) = config.search('target_des_services')
            if not found:
                print "%s    Error:  using DB (%s), but missing target_des_services" % (indent, PF_USE_DB_IN)
                cnts[ERRCNT_POS] += 1

        if (PF_USE_DB_OUT in config and convertBool(config[PF_USE_DB_OUT])):
            (found, val) = config.search('target_des_db_section')
            if not found:
                print "%s    Error:  using DB (%s), but missing target_des_db_section" % (indent, PF_USE_DB_OUT)
                cnts[ERRCNT_POS] += 1

            (found, val) = config.search('target_des_services')
            if not found:
                print "%s    Error:  using DB (%s), but missing target_des_services" % (indent, PF_USE_DB_OUT)
                cnts[ERRCNT_POS] += 1

        # check modules
        block = config[SW_BLOCKSECT][blockname]
        if SW_MODULELIST in block:
            modulelist = fwsplit(block[SW_MODULELIST].lower(), ',')

            for modname in modulelist:
                if modname not in config[SW_MODULESECT]:
                    print "%s    Error: block %s - invalid %s" % (indent, blockname, SW_MODULELIST)
                    print "%s        (bad module name: %s, list: %s)" % (indent, modname, modulelist)
                    cnts[ERRCNT_POS] += 1
                else:
                    cnts2 = check_module(config, blockname, modname, indent+'    ')
                    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts

        else:
            print "%s    Error: block %s - missing %s value" % (indent, blockname, SW_MODULESECT)
            cnts[ERRCNT_POS] += 1
    
        config.inc_blknum()

    config.reset_blknum()

    return cnts
    


########################################################################### 
def check_target_archive(config, indent=''):
    """ check info related to target_archive """ 

    cnts = [0 for i in range(0,NUMCNTS)]

    print "%sChecking target_archive..." % (indent)
    blocklist = fwsplit(config[SW_BLOCKLIST].lower(),',')
    for blockname in blocklist:
        config.set_block_info()
        block = config[SW_BLOCKSECT][blockname]

        (found_input, use_target_archive_input) = config.search(USE_TARGET_ARCHIVE_INPUT, {PF_CURRVALS: {'curr_block': blockname}})
        (found_output, use_target_archive_output) = config.search(USE_TARGET_ARCHIVE_OUTPUT, {PF_CURRVALS: {'curr_block': blockname}})
        (found_archive, target_archive) = config.search(TARGET_ARCHIVE, {PF_CURRVALS: {'curr_block': blockname}})

        if not found_input:
            print "%s    Error: block %s - Could not determine %s" % (indent, blockname, USE_TARGET_ARCHIVE_INPUT)
            cnts[ERRCNT_POS] += 1
        elif use_target_archive_input.lower() not in VALID_TARGET_ARCHIVE_INPUT:
            print "%s    Error: block %s - Invalid %s value" % (indent, blockname, USE_TARGET_ARCHIVE_INPUT)
            cnts[ERRCNT_POS] += 1
        
        if not found_output:
            print "%s    Error: block %s - Could not determine %s" % (indent, blockname, USE_TARGET_ARCHIVE_OUTPUT)
            cnts[ERRCNT_POS] += 1
        elif use_target_archive_output.lower() not in VALID_TARGET_ARCHIVE_OUTPUT:
            print "%s    Error: block %s - Invalid %s value" % (indent, blockname, USE_TARGET_ARCHIVE_OUTPUT)
            cnts[ERRCNT_POS] += 1

        # if need to use a target_archive for this block
        if ((found_input and use_target_archive_input.lower() != 'never') or
            (found_output and use_target_archive_output.lower() != 'never')):
            if not found_archive:
                print "%s    Error: block %s - Missing %s value" % (indent, blockname, TARGET_ARCHIVE)
                cnts[ERRCNT_POS] += 1
            elif 'archive' not in config:
                print "%s    Error: block %s - Needs archive section which doesn't exist" % (indent, blockname)
                cnts[ERRCNT_POS] += 1
            elif target_archive not in config['archive']:
                print "%s    Error: block %s - Invalid %s value" % (indent, blockname, TARGET_ARCHIVE)
                cnts[ERRCNT_POS] += 1
            else:
                # check that we have all archive req values exist
                pass

        config.inc_blknum()

    config.reset_blknum()


    return cnts


########################################################################### 
def check_home_archive(config, indent=''):
    """ check info related to target_archive """ 

    cnts = [0 for i in range(0,NUMCNTS)]

    print "%sChecking home archive..." % (indent)
    blocklist = fwsplit(config[SW_BLOCKLIST].lower(),',')
    for blockname in blocklist:
        config.set_block_info()
        block = config[SW_BLOCKSECT][blockname]

        (found_input, use_home_archive_input) = config.search(USE_HOME_ARCHIVE_INPUT, {PF_CURRVALS: {'curr_block': blockname}})
        (found_output, use_home_archive_output) = config.search(USE_HOME_ARCHIVE_OUTPUT, {PF_CURRVALS: {'curr_block': blockname}})
        (found_archive, home_archive) = config.search(HOME_ARCHIVE, {PF_CURRVALS: {'curr_block': blockname}})

        if not found_input:
            print "%s    Error: block %s - Could not determine %s" % (indent, blockname, USE_HOME_ARCHIVE_INPUT)
            cnts[ERRCNT_POS] += 1
        elif use_home_archive_input.lower() not in VALID_HOME_ARCHIVE_INPUT:
            print "%s    Error: block %s - Invalid %s value" % (indent, blockname, USE_HOME_ARCHIVE_INPUT)
            cnts[ERRCNT_POS] += 1
        
        if not found_output:
            print "%s    Error: block %s - Could not determine %s" % (indent, blockname, USE_HOME_ARCHIVE_OUTPUT)
            cnts[ERRCNT_POS] += 1
        elif use_home_archive_output.lower() not in VALID_HOME_ARCHIVE_OUTPUT:
            print "%s    Error: block %s - Invalid %s value" % (indent, blockname, USE_HOME_ARCHIVE_OUTPUT)
            cnts[ERRCNT_POS] += 1

        # if need to use a home_archive for this block
        if ((found_input and use_home_archive_input.lower() != 'never') or
            (found_output and use_home_archive_output.lower() != 'never')):
            if not found_archive:
                print "%s    Error: block %s - Missing %s value" % (indent, blockname, HOME_ARCHIVE)
                cnts[ERRCNT_POS] += 1
            elif 'archive' not in config:
                print "%s    Error: block %s - Needs archive section which doesn't exist" % (indent, blockname)
                cnts[ERRCNT_POS] += 1
            elif home_archive not in config['archive']:
                print "%s    Error: block %s - Invalid %s value" % (indent, blockname, HOME_ARCHIVE)
                cnts[ERRCNT_POS] += 1
            else:
                # check that we have all archive req values exist
                pass
    
        config.inc_blknum()

    config.reset_blknum()

    return cnts


########################################################################### 
def check_module(config, blockname, modname, indent=''):
    cnts = [0 for i in range(0,NUMCNTS)]

    print "%sChecking module %s..." % (indent, modname)
    moddict = config[SW_MODULESECT][modname]
    dataobjs = {SW_INPUTS: {}, SW_OUTPUTS: {}}

    # check that have wrappername (required)
    if SW_WRAPPERNAME not in moddict:
        print "%s    Error: block %s, module %s - missing %s value" % (indent, blockname, modname, SW_WRAPPERNAME)
        cnts[ERRCNT_POS] += 1
                            
    # check that have at least 1 exec section (required)
    execsects = get_exec_sections(moddict, SW_EXECPREFIX) 
    if len(execsects) == 0:
        print "%s    Error: block %s, module %s - 0 exec sections (%s*)" % (indent, blockname, modname, SW_EXECPREFIX)
        cnts[ERRCNT_POS] += 1
    else:
        # check exec sections
        for xsectname in execsects:
            xsectdict = moddict[xsectname]
            cnts2 = check_exec(config, blockname, modname, dataobjs, xsectname, xsectdict, indent+"    ")
            cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
        
    # check file/list sections
    cnts2 = check_dataobjs(config, blockname, modname, moddict, dataobjs, indent+"    ")
    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts

    return cnts



########################################################################### 
def parse_wcl_objname(objname):
    parts = fwsplit(objname, '.')
    #print 'parts=',parts

    sect = name = subname = None

    if len(parts) == 3:    # lists have 3 parts
        (sect, name, subname) = parts
    elif len(parts) == 2:  # files have 2 parts
        (sect, name) = parts
    elif len(parts) == 1:
        name = parts[0]
    else:
        print "%sError: cannot parse objname %s (too many sections/periods)" % (objname)
        
    return sect, name, subname


########################################################################### 
def check_file_valid_input(config, blockname, modname, fname, fdict, indent=''):
    cnts = [0 for i in range(0,NUMCNTS)]

    # check that it has filepat, filename, or query code (required)
    # if filename is a pattern, can I check that all needed values exist?
    if (('listonly' not in fdict or not convertBool(fdict['listonly'])) and 
       SW_FILEPAT not in fdict and FILENAME not in fdict and 
       'fullname' not in fdict and 'query_fields' not in fdict):  
        print "%sError: block %s, module %s, %s, %s - Missing terms needed to determine input filename" % (indent, blockname, modname, SW_INPUTS, fname)
        cnts[ERRCNT_POS] += 1
    
    # check that it has dirpat :    err
    # can I check that all values for dirpat exist?
    if DIRPAT not in fdict:
        print "%sError: block %s, module %s, %s, %s - Missing %s" % (indent, blockname, modname, SW_INPUTS, fname, DIRPAT)
        cnts[ERRCNT_POS] += 1

    return cnts


########################################################################### 
def check_list_valid_input(config, blockname, modname, objname, objdict, indent=''):
    cnts = [0 for i in range(0,NUMCNTS)]

    (sect, name, subname) = parse_wcl_objname(objname)

    # check that it has filepat, filename, or query code (required)
    # if filename is a pattern, can I check that all needed values exist?
    if SW_FILEPAT not in objdict and \
        FILENAME not in objdict and \
        'output_fields' not in objdict:
        print "%sError: block %s, module %s, %s, %s - Missing terms needed to determine list filename" % (indent, blockname, modname, SW_INPUTS, objname)
        cnts[ERRCNT_POS] += 1
    
    # check that it has dirpat :    err
    # can I check that all values for dirpat exist?
    if DIRPAT not in objdict:
        print "%sError: block %s, module %s, %s, %s - Missing %s" % (indent, blockname, modname, SW_INPUTS, objname, DIRPAT)
        cnts[ERRCNT_POS] += 1

    return cnts


########################################################################### 
def check_exec_inputs(config, blockname, modname, dataobjs, xsectname, xsectdict, indent=''):
    # initialize
    cnts = [0 for i in range(0,NUMCNTS)]
    moddict = config[SW_MODULESECT][modname]

    if SW_INPUTS in xsectdict:
        print "%sChecking %s %s..." % (indent, xsectname, SW_INPUTS)
        indent += '    '
        
        #print "%sxsectdict[SW_INPUTS] = %s" % (indent, xsectdict[SW_INPUTS])

        # for each entry in inputs
        for objname in fwsplit(xsectdict[SW_INPUTS],','):
            objname = objname.lower()
            #print '%sobjname=%s' % (indent, objname)

            (sect, name, subname) = parse_wcl_objname(objname) 
            #print '%s(sect, name, subname) = (%s, %s, %s)' % (indent, sect, name, subname)
            if sect is None:
                print "%s    Error: block %s, module %s, %s, %s - Invalid entry (%s).  Missing section label" % (indent, blockname, modname, xsectname, SW_INPUTS, objname)
                cnts[ERRCNT_POS] += 1
            else:
                # check that appears in [file/list]sect : err
                if sect not in moddict or name not in moddict[sect]:
                    print "%s    Error:  block %s, module %s, %s, %s - Invalid entry (%s).  Cannot find definition." % (indent, blockname, modname, xsectname, SW_INPUTS, objname)
                    cnts[ERRCNT_POS] += 1
                elif subname is None:  # file
                    dataobjs[SW_INPUTS][objname] = True
                elif sect != SW_LISTSECT:   # only lists can have subname
                    print "%sError: block %s, module %s, %s, %s, %s - Too many sections/periods for a %s." % (indent, blockname, modname, xsectname, SW_INPUTS, objname, sect)
                    cnts[ERRCNT_POS] += 1
                elif subname not in moddict[SW_FILESECT]:
                    print "%sError: block %s, module %s, %s, %s, %s - Cannot find definition for %s" % (indent, blockname, modname, xsectname, SW_INPUTS, objname, subname)
                    cnts[ERRCNT_POS] += 1
                else:
                    dataobjs[SW_INPUTS]["%s.%s" % (SW_LISTSECT,name)] = True
                    dataobjs[SW_INPUTS]["%s.%s" % (SW_FILESECT,subname)] = True
                    dataobjs[SW_INPUTS][objname] = True
                    fdict = moddict[SW_FILESECT][subname]
                    if ('listonly' not in fdict or not convertBool(fdict['listonly'])): 
                        print "%sWarning: block %s, module %s, %s, %s, %s - File in list does not have listonly=True" % (indent, blockname, modname, xsectname, SW_INPUTS, objname)
                        cnts[WARNCNT_POS] += 1

    return cnts
         

########################################################################### 
def check_file_valid_output(config, blockname, modname, fname, fdict, indent=''):
    cnts = [0 for i in range(0,NUMCNTS)]
    moddict = config[SW_MODULESECT][modname]

    # check that it has dirpat :    err
    # can I check that all values for dirpat exist?
    if DIRPAT not in fdict:
        print "%sError: block %s, module %s, %s, %s - Missing %s" % (indent, blockname, modname, SW_OUTPUTS, fname, DIRPAT)
        cnts[ERRCNT_POS] += 1
    else:
        # todo: check that all values for dirpat exist
        pass

    # check that it has filepat, filename (required)
    if SW_FILEPAT not in fdict and \
       FILENAME not in fdict and \
       'fullname' not in fdict:
        print "%sError: block %s, module %s, %s, %s - Missing terms needed to determine output filename" % (indent, blockname, modname, SW_OUTPUTS, fname)
        cnts[ERRCNT_POS] += 1
    else:
        # todo: if pattern, check that all needed values exist
        pass

    # check that it has filetype :    err
    if FILETYPE not in fdict:
        print "%sError: block %s, module %s, %s, %s - Missing %s" % (indent, blockname, modname, SW_OUTPUTS, fname, FILETYPE)
        cnts[ERRCNT_POS] += 1
    elif fdict[FILETYPE] not in config[FILETYPE_METADATA]:
        print "%sError: block %s, module %s, %s, %s - Invalid %s (%s)" % (indent, blockname, modname, SW_OUTPUTS, fname, FILETYPE)
        cnts[ERRCNT_POS] += 1
               
    return cnts


########################################################################### 
def check_exec_outputs(config, blockname, modname, dataobjs, xsectname, xsectdict, indent=''):
    # initialize
    cnts = [0 for i in range(0,NUMCNTS)]
    moddict = config[SW_MODULESECT][modname]
    

    if SW_OUTPUTS in xsectdict:
        # for each entry in inputs
        print "%sChecking %s %s..." % (indent, xsectname, SW_OUTPUTS)
        indent += '    '
        #print "%sxsectdict[SW_OUTPUTS] = %s" % (indent, xsectdict[SW_OUTPUTS])
        for objname in fwsplit(xsectdict[SW_OUTPUTS],','):
            objname = objname.lower()
            #print '%sobjname=%s' % (indent, objname)

            (sect, name, subname) = parse_wcl_objname(objname) 
            #print '%s(sect, name, subname) = (%s, %s, %s)' % (indent, sect, name, subname)
            if sect is None:
                print "%s    Error: block %s, module %s, %s, %s - Invalid entry (%s).  Missing section label" % (indent, blockname, modname, xsectname, SW_OUTPUTS, objname)
                cnts[ERRCNT_POS] += 1
            else:
                # check that appears in [file/list]sect : err
                if sect not in moddict or name not in moddict[sect]:
                    print "%s    Error:  block %s, module %s, %s, %s - Invalid entry (%s).  Cannot find definition." % (indent, blockname, modname, xsectname, SW_OUTPUTS, objname)
                    cnts[ERRCNT_POS] += 1
                else:
                    dataobjs[SW_OUTPUTS][objname] = True

    return cnts




########################################################################### 
def check_exec_parentchild(config, blockname, modname, dataobjs, xsectname, xsectdict, indent=''):
    # check that parent and children appear in inputs and outputs
    # assumes check_exec_input and check_exec_output have already been executed so there are entries in dataobjs

    cnts = [0 for i in range(0,NUMCNTS)]
    if SW_PARENTCHILD in xsectdict:
        print "%sChecking %s %s..." % (indent, xsectname, SW_PARENTCHILD)
        indent += '    '
        #print "%sxsectdict[SW_PARENTCHILD] = %s" % (indent, xsectdict[SW_PARENTCHILD])
        #print "%sdataobjs[SW_INPUTS] = %s" % (indent, dataobjs[SW_INPUTS])
        #print "%sdataobjs[SW_OUTPUTS] = %s" % (indent, dataobjs[SW_OUTPUTS])
        #print "%sfsplit = %s" % (indent, fwsplit(xsectdict[SW_PARENTCHILD],',') )

        for pair in fwsplit(xsectdict[SW_PARENTCHILD],','):
            pair = pair.lower()
            if ':' in pair:
                (parent, child) = fwsplit(pair, ':') 
                if '.' in parent:
                    if parent not in dataobjs[SW_INPUTS]:
                        print "%sError: block %s, module %s, %s, %s - parent %s not listed in %s" % (indent, blockname, modname, xsectname, SW_PARENTCHILD, parent, SW_INPUTS)
                        cnts[ERRCNT_POS] += 1
                else:
                    print "%sError: block %s, module %s, %s, %s - parent %s missing section label" % (indent, blockname, modname, xsectname, SW_PARENTCHILD, parent)
                    cnts[ERRCNT_POS] += 1
    
                if '.' in child:
                    if child not in dataobjs[SW_OUTPUTS]:
                        print "%sError: block %s, module %s, %s, %s - child %s not listed in %s" % (indent, blockname, modname, xsectname, SW_PARENTCHILD, child, SW_OUTPUTS)
                        cnts[ERRCNT_POS] += 1
                else:
                    print "%sError: block %s, module %s, %s, %s - child %s missing section label" % (indent, blockname, modname, xsectname, SW_PARENTCHILD, child)
                    cnts[ERRCNT_POS] += 1
            else:
                print "%sError: block %s, module %s, %s, %s - Invalid parent/child pair (%s).  Missing colon." % (indent, blockname, modname, xsectname, SW_PARENTCHILD, pair)
                cnts[ERRCNT_POS] += 1
    elif SW_INPUTS in xsectdict and SW_OUTPUTS in xsectdict:
        print "%sWarning: block %s, module %s, %s - has %s and %s, but not %s" % (indent, blockname, modname, xsectname, SW_INPUTS, SW_OUTPUTS, SW_PARENTCHILD)
        cnts[WARNCNT_POS] += 1

    return cnts



########################################################################### 
def check_dataobjs(config, blockname, modname, moddict, dataobjs, indent=''):
    """ calls functions to check files have all needed info as well as note extra file defs """
    
    cnts = [0 for i in range(0,NUMCNTS)]

    # check every file
    if SW_FILESECT in moddict:
        print "%sChecking %s section..." % (indent, SW_FILESECT)
        for fname,fdict in moddict[SW_FILESECT].items():
            key = '%s.%s' % (SW_FILESECT, fname)
            if key not in dataobjs[SW_INPUTS] and \
               key not in dataobjs[SW_OUTPUTS] and \
               ('listonly' not in fdict or not convertBool('listonly')): 
                print "%sWarning: %s.%s does not appear in provenance lines" % (indent+'    ', SW_FILESECT, fname)
                cnts[WARNCNT_POS] += 1

            if key in dataobjs[SW_INPUTS]:
                cnts2 = check_file_valid_input(config, blockname, modname, fname, fdict, indent+'    ')
                cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
                
    
            if key in dataobjs[SW_OUTPUTS]:
                cnts2 = check_file_valid_output(config, blockname, modname, fname, fdict, indent+'    ')
                cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
                
            
    # check every list
    if SW_LISTSECT in moddict:
        print "%sChecking %s section..." % (indent, SW_LISTSECT)
        for lname,ldict in moddict[SW_LISTSECT].items():
            key = '%s.%s' % (SW_LISTSECT, lname)
            if key not in dataobjs[SW_INPUTS] and \
               key not in dataobjs[SW_OUTPUTS]:
                print "%sWarning: %s.%s does not appear in provenance lines" % (indent, SW_LISTSECT, lname)
                cnts[WARNCNT_POS] += 1

            if key in dataobjs[SW_INPUTS]:
                cnts2 = check_list_valid_input(config, blockname, modname, lname, ldict, indent+'    ')
                cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
            

    return cnts



########################################################################### 
def check_exec_cmd(config, blockname, modname, dataobjs, xsectname, xsectdict, indent=''): 
    cnts = [0 for i in range(0,NUMCNTS)]

    # check that each exec section has execname (required)
    if SW_EXECNAME not in xsectdict:
        print "%sError: block %s, module %s, %s - missing %s" % (indent, blockname, modname, xsectname, SW_EXECNAME)
        cnts[ERRCNT_POS] += 1
    elif '/' in xsectdict[SW_EXECNAME]:
        print "%sWarning: block %s, module %s, %s - hardcoded path in %s (%s)" % (indent, blockname, modname, xsectname, SW_EXECNAME, xsectdict[SW_EXECNAME])
        cnts[WARNCNT_POS] += 1

    # almost all production cases would need to have command line arguments
    if SW_CMDARGS not in xsectdict:
        print "%sWarning: block %s, module %s, %s - missing %s" % (indent, blockname, modname, xsectname, SW_CMDARGS)
        cnts[WARNCNT_POS] += 1
    else:
        moddict = config[SW_MODULESECT][modname]
        argvars = search_wcl_for_variables(xsectdict[SW_CMDARGS])
        for var in argvars:
            if var.endswith('.fullname'):
                var2 = var[0:-(len('.fullname'))]
                (sect, name, subname) = parse_wcl_objname(var2)
                if sect not in moddict or name not in moddict[sect]:
                    print "%sError: block %s, module %s, %s, %s - Undefined variable (%s)" % (indent, blockname, modname, xsectname, SW_CMDARGS, var)
                    cnts[ERRCNT_POS] += 1

                if subname and subname not in moddict[SW_FILESECT]:
                    print "%sError: block %s, module %s, %s, %s - Undefined variable (%s)" % (indent, blockname, modname, xsectname, SW_CMDARGS, var)
                    cnts[ERRCNT_POS] += 1
            else:
                (found, val) = config.search(var, { PF_CURRVALS: {'curr_block': blockname, 'curr_module': modname}, 'searchobj': xsectdict, 'required':False, 'interpolate': True})
#                if found:
#                    val2 = config.interpolate(val, { PF_CURRVALS: {'curr_block': blockname, 'curr_module': modname}, 'searchobj': xsectdict, 'interpolate': True, 'required':False})
                    
                    
                
                
                

        # check that all values in args exist?/
        # check for value names that look like file/list names but are missing file/list in front
        # check that all file/list entries in args appears in inputs/outputs : err
    return cnts
           


########################################################################### 
def check_exec(config, blockname, modname, dataobjs, xsectname, xsectdict, indent=''):
    cnts = [0 for i in range(0,NUMCNTS)]

    print "%sChecking %s..." % (indent, xsectname)
    cnts2 = check_exec_inputs(config, blockname, modname, dataobjs, xsectname, xsectdict, indent+'    ')
    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts

    cnts2 = check_exec_outputs(config, blockname, modname, dataobjs, xsectname, xsectdict, indent+'    ')
    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts

    cnts2 = check_exec_parentchild(config, blockname, modname, dataobjs, xsectname, xsectdict, indent+'    ')
    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts

    cnts2 = check_exec_cmd(config, blockname, modname, dataobjs, xsectname, xsectdict, indent+'    ')
    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
    
    return cnts



         

########################################################################### 
def check(config, indent=''):
    """ Check submit wcl """
    
    # initialize counters

    cnts = [0,0,0,0]
    
    cnts2 = check_globals(config, indent)
    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
    if cnts[ERRCNT_POS] > 0:
        print "%sAborting test" % (indent)
        return cnts

    cnts2 = check_block(config, indent) 
    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
    if cnts[ERRCNT_POS] > 0:
        print "%sAborting test" % (indent)
        return cnts
    
    cnts2 = check_target_archive(config, indent) 
    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
    #if cnts[ERRCNT_POS] > 0:
    #    print "%sAborting test" % (indent)
    #    return cnts

    cnts2 = check_home_archive(config, indent) 
    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
    #if cnts[ERRCNT_POS] > 0:
    #    print "%sAborting test" % (indent)
    #    return cnts

    return cnts


if __name__ ==  '__main__':
    print "No main program.   Run descheck.py instead"
