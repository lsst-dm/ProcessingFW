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

import processingfw.pfwdefs as pfwdefs
import despymisc.miscutils as miscutils 
import processingfw.pfwutils as pfwutils
import filemgmt.filemgmt_defs as fmdefs

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
    for key in ['pipeline', 'pipeprod', 'pipever', 'project', pfwdefs.REQNUM, pfwdefs.ATTNUM, pfwdefs.UNITNAME, 'jira_id', 'target_site',
                'site', 'filename_pattern', 'directory_pattern', 'job_file_mvmt', 'ops_run_dir', 
                pfwdefs.PF_USE_QCF, pfwdefs.PF_USE_DB_IN, pfwdefs.PF_USE_DB_OUT, pfwdefs.SW_BLOCKLIST, pfwdefs.SW_BLOCKSECT, pfwdefs.SW_MODULESECT, 'create_junk_tarball']:
        try:
            if key not in config:
                print "%s    Error: missing %s global key or section" % (indent, key)
                cnts[ERRCNT_POS] += 1
        except:
            print "%s    Error: missing %s global key or section" % (indent, key)
            cnts[ERRCNT_POS] += 1
            

    if pfwdefs.PF_USE_DB_IN in config:
        if miscutils.convertBool(config[pfwdefs.PF_USE_DB_IN]):
            if 'submit_des_db_section' not in config:
                print "%s    Error:  using DB (%s), but missing submit_des_db_section" % (indent, pfwdefs.PF_USE_DB_IN)
                cnts[ERRCNT_POS] += 1
            if 'submit_des_services' not in config:
                print "%s    Error:  using DB (%s), but missing submit_des_services" % (indent, pfwdefs.PF_USE_DB_IN)
                cnts[ERRCNT_POS] += 1

    if pfwdefs.PF_USE_DB_OUT in config:
        if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]):
            if 'submit_des_db_section' not in config:
                print "%s    Error:  using DB (%s), but missing submit_des_db_section" % (indent, pfwdefs.PF_USE_DB_OUT)
                cnts[ERRCNT_POS] += 1
            if 'submit_des_services' not in config:
                print "%s    Error:  using DB (%s), but missing submit_des_services" % (indent, pfwdefs.PF_USE_DB_OUT)
                cnts[ERRCNT_POS] += 1

    # if using QCF must also be writing run info into DB
    if pfwdefs.PF_USE_QCF in config and miscutils.convertBool(config[pfwdefs.PF_USE_QCF]) and \
        (pfwdefs.PF_USE_DB_OUT in config and not miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT])):
        print "%s    Error: if %s is true, %s must also be set to true" % (indent, pfwdefs.PF_USE_QCF, pfwdefs.PF_USE_DB_OUT)
        cnts[ERRCNT_POS] += 1

    if 'operator' not in config:
        print '%s    Error:  Must specify operator' % (indent)
        cnts[ERRCNT_POS] += 1
    elif config['operator'] in ['bcs']:
        print '%s    Error:  Operator cannot be shared login (%s).' % (indent, config['operator'])
        cnts[ERRCNT_POS] += 1

    blocklist = miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST].lower(),',')
    for blockname in blocklist:
        if blockname not in config[pfwdefs.SW_BLOCKSECT]:
            print '%s    Error:  Invalid %s, bad block name (%s)' % (indent, pfwdefs.SW_BLOCKLIST, blockname)
            cnts[ERRCNT_POS] += 1
            
    return cnts



########################################################################### 
def check_block(config, indent=''):
    """ check blocks level defs """ 

    cnts = [0 for i in range(0,NUMCNTS)]

    blocklist = miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST].lower(),',')
    for blockname in blocklist:
        print "%sChecking block %s..." % (indent, blockname)
        config.set_block_info()

        if (pfwdefs.PF_USE_DB_IN in config and miscutils.convertBool(config[pfwdefs.PF_USE_DB_IN])):
            (found, val) = config.search('target_des_db_section')
            if not found:
                print "%s    Error:  using DB (%s), but missing target_des_db_section" % (indent, pfwdefs.PF_USE_DB_IN)
                cnts[ERRCNT_POS] += 1

            (found, val) = config.search('target_des_services')
            if not found:
                print "%s    Error:  using DB (%s), but missing target_des_services" % (indent, pfwdefs.PF_USE_DB_IN)
                cnts[ERRCNT_POS] += 1

        if (pfwdefs.PF_USE_DB_OUT in config and miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT])):
            (found, val) = config.search('target_des_db_section')
            if not found:
                print "%s    Error:  using DB (%s), but missing target_des_db_section" % (indent, pfwdefs.PF_USE_DB_OUT)
                cnts[ERRCNT_POS] += 1

            (found, val) = config.search('target_des_services')
            if not found:
                print "%s    Error:  using DB (%s), but missing target_des_services" % (indent, pfwdefs.PF_USE_DB_OUT)
                cnts[ERRCNT_POS] += 1

        # check modules
        block = config[pfwdefs.SW_BLOCKSECT][blockname]
        if pfwdefs.SW_MODULELIST in block:
            modulelist = miscutils.fwsplit(block[pfwdefs.SW_MODULELIST].lower(), ',')

            for modname in modulelist:
                if modname not in config[pfwdefs.SW_MODULESECT]:
                    print "%s    Error: block %s - invalid %s" % (indent, blockname, pfwdefs.SW_MODULELIST)
                    print "%s        (bad module name: %s, list: %s)" % (indent, modname, modulelist)
                    cnts[ERRCNT_POS] += 1
                else:
                    cnts2 = check_module(config, blockname, modname, indent+'    ')
                    cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts

        else:
            print "%s    Error: block %s - missing %s value" % (indent, blockname, pfwdefs.SW_MODULESECT)
            cnts[ERRCNT_POS] += 1
    
        config.inc_blknum()

    config.reset_blknum()

    return cnts
    


########################################################################### 
def check_target_archive(config, indent=''):
    """ check info related to pfwdefs.TARGET_ARCHIVE """ 

    cnts = [0 for i in range(0,NUMCNTS)]

    print "%sChecking pfwdefs.TARGET_ARCHIVE..." % (indent)
    blocklist = miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST].lower(),',')
    for blockname in blocklist:
        config.set_block_info()
        block = config[pfwdefs.SW_BLOCKSECT][blockname]

        (found_input, pfwdefs.USE_TARGET_ARCHIVE_INPUT) = config.search(pfwdefs.USE_TARGET_ARCHIVE_INPUT, {pfwdefs.PF_CURRVALS: {'curr_block': blockname}})
        (found_output, pfwdefs.USE_TARGET_ARCHIVE_OUTPUT) = config.search(pfwdefs.USE_TARGET_ARCHIVE_OUTPUT, {pfwdefs.PF_CURRVALS: {'curr_block': blockname}})
        (found_archive, pfwdefs.TARGET_ARCHIVE) = config.search(pfwdefs.TARGET_ARCHIVE, {pfwdefs.PF_CURRVALS: {'curr_block': blockname}})

        if not found_input:
            print "%s    Error: block %s - Could not determine %s" % (indent, blockname, pfwdefs.USE_TARGET_ARCHIVE_INPUT)
            cnts[ERRCNT_POS] += 1
        elif pfwdefs.USE_TARGET_ARCHIVE_INPUT.lower() not in pfwdefs.VALID_TARGET_ARCHIVE_INPUT:
            print "%s    Error: block %s - Invalid %s value" % (indent, blockname, pfwdefs.USE_TARGET_ARCHIVE_INPUT)
            cnts[ERRCNT_POS] += 1
        
        if not found_output:
            print "%s    Error: block %s - Could not determine %s" % (indent, blockname, pfwdefs.USE_TARGET_ARCHIVE_OUTPUT)
            cnts[ERRCNT_POS] += 1
        elif pfwdefs.USE_TARGET_ARCHIVE_OUTPUT.lower() not in pfwdefs.VALID_TARGET_ARCHIVE_OUTPUT:
            print "%s    Error: block %s - Invalid %s value" % (indent, blockname, pfwdefs.USE_TARGET_ARCHIVE_OUTPUT)
            cnts[ERRCNT_POS] += 1

        # if need to use a pfwdefs.TARGET_ARCHIVE for this block
        if ((found_input and pfwdefs.USE_TARGET_ARCHIVE_INPUT.lower() != 'never') or
            (found_output and pfwdefs.USE_TARGET_ARCHIVE_OUTPUT.lower() != 'never')):
            if not found_archive:
                print "%s    Error: block %s - Missing %s value" % (indent, blockname, pfwdefs.TARGET_ARCHIVE)
                cnts[ERRCNT_POS] += 1
            elif 'archive' not in config:
                print "%s    Error: block %s - Needs archive section which doesn't exist" % (indent, blockname)
                cnts[ERRCNT_POS] += 1
            elif pfwdefs.TARGET_ARCHIVE not in config['archive']:
                print "%s    Error: block %s - Invalid %s value" % (indent, blockname, pfwdefs.TARGET_ARCHIVE)
                cnts[ERRCNT_POS] += 1
            else:
                # check that we have all archive req values exist
                pass

        config.inc_blknum()

    config.reset_blknum()


    return cnts


########################################################################### 
def check_home_archive(config, indent=''):
    """ check info related to pfwdefs.TARGET_ARCHIVE """ 

    cnts = [0 for i in range(0,NUMCNTS)]

    print "%sChecking home archive..." % (indent)
    blocklist = miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST].lower(),',')
    for blockname in blocklist:
        config.set_block_info()
        block = config[pfwdefs.SW_BLOCKSECT][blockname]

        (found_input, pfwdefs.USE_HOME_ARCHIVE_INPUT) = config.search(pfwdefs.USE_HOME_ARCHIVE_INPUT, {pfwdefs.PF_CURRVALS: {'curr_block': blockname}})
        (found_output, pfwdefs.USE_HOME_ARCHIVE_OUTPUT) = config.search(pfwdefs.USE_HOME_ARCHIVE_OUTPUT, {pfwdefs.PF_CURRVALS: {'curr_block': blockname}})
        (found_archive, home_archive) = config.search(pfwdefs.HOME_ARCHIVE, {pfwdefs.PF_CURRVALS: {'curr_block': blockname}})

        if not found_input:
            print "%s    Error: block %s - Could not determine %s" % (indent, blockname, pfwdefs.USE_HOME_ARCHIVE_INPUT)
            cnts[ERRCNT_POS] += 1
        elif pfwdefs.USE_HOME_ARCHIVE_INPUT.lower() not in pfwdefs.VALID_HOME_ARCHIVE_INPUT:
            print "%s    Error: block %s - Invalid %s value" % (indent, blockname, pfwdefs.USE_HOME_ARCHIVE_INPUT)
            cnts[ERRCNT_POS] += 1
        
        if not found_output:
            print "%s    Error: block %s - Could not determine %s" % (indent, blockname, pfwdefs.USE_HOME_ARCHIVE_OUTPUT)
            cnts[ERRCNT_POS] += 1
        elif pfwdefs.USE_HOME_ARCHIVE_OUTPUT.lower() not in pfwdefs.VALID_HOME_ARCHIVE_OUTPUT:
            print "%s    Error: block %s - Invalid %s value" % (indent, blockname, pfwdefs.USE_HOME_ARCHIVE_OUTPUT)
            cnts[ERRCNT_POS] += 1

        # if need to use a home_archive for this block
        if ((found_input and pfwdefs.USE_HOME_ARCHIVE_INPUT.lower() != 'never') or
            (found_output and pfwdefs.USE_HOME_ARCHIVE_OUTPUT.lower() != 'never')):
            if not found_archive:
                print "%s    Error: block %s - Missing %s value" % (indent, blockname, pfwdefs.HOME_ARCHIVE)
                cnts[ERRCNT_POS] += 1
            elif 'archive' not in config:
                print "%s    Error: block %s - Needs archive section which doesn't exist" % (indent, blockname)
                cnts[ERRCNT_POS] += 1
            elif home_archive not in config['archive']:
                print "%s    Error: block %s - Invalid %s value" % (indent, blockname, pfwdefs.HOME_ARCHIVE)
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
    moddict = config[pfwdefs.SW_MODULESECT][modname]
    dataobjs = {pfwdefs.SW_INPUTS: {}, pfwdefs.SW_OUTPUTS: {}}

    # check that have wrappername (required)
    if pfwdefs.SW_WRAPPERNAME not in moddict:
        print "%s    Error: block %s, module %s - missing %s value" % (indent, blockname, modname, pfwdefs.SW_WRAPPERNAME)
        cnts[ERRCNT_POS] += 1
                            
    # check that have at least 1 exec section (required)
    execsects = pfwutils.get_exec_sections(moddict, pfwdefs.SW_EXECPREFIX) 
    if len(execsects) == 0:
        print "%s    Error: block %s, module %s - 0 exec sections (%s*)" % (indent, blockname, modname, pfwdefs.SW_EXECPREFIX)
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
    parts = miscutils.fwsplit(objname, '.')
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
def check_filepat_valid(config, filepat, blockname, modname, objname, objdict, indent=''):
    cnts = [0 for i in range(0,NUMCNTS)]

    if pfwdefs.SW_FILEPATSECT not in config:
        print "%sError: Missing filename pattern definition section (%s)" % (pfwdefs.SW_FILEPATSECT)
        cnts[ERRCNT_POS] += 1
    elif filepat not in config[pfwdefs.SW_FILEPATSECT]:
        print "%sError: block %s, module %s, %s - Missing definition for %s '%s'" % (indent, blockname, modname, objname, pfwdefs.SW_FILEPAT, filepat)
        cnts[ERRCNT_POS] += 1

    # todo: if pattern, check that all needed values exist

    return cnts


########################################################################### 
def check_file_valid_input(config, blockname, modname, fname, fdict, indent=''):
    cnts = [0 for i in range(0,NUMCNTS)]

    # check that any given filename pattern has a definition
    if pfwdefs.SW_FILEPAT in fdict:
        cnts2 = check_filepat_valid(config, fdict[pfwdefs.SW_FILEPAT], blockname, modname, fname, fdict, indent+'    ')
        cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
    
    # check that it has filepat, filename, or query code (required)
    # if filename is a pattern, can I check that all needed values exist?
    if (('listonly' not in fdict or not miscutils.convertBool(fdict['listonly'])) and 
       pfwdefs.SW_FILEPAT not in fdict and pfwdefs.FILENAME not in fdict and 
       'fullname' not in fdict and 'query_fields' not in fdict):  
        print "%sError: block %s, module %s, %s, %s - Missing terms needed to determine input filename" % (indent, blockname, modname, pfwdefs.SW_INPUTS, fname)
        cnts[ERRCNT_POS] += 1
    
    # check that it has pfwdefs.DIRPAT :    err
    # can I check that all values for pfwdefs.DIRPAT exist?
    if pfwdefs.DIRPAT not in fdict:
        print "%sError: block %s, module %s, %s, %s - Missing %s" % (indent, blockname, modname, pfwdefs.SW_INPUTS, fname, pfwdefs.DIRPAT)
        cnts[ERRCNT_POS] += 1

    return cnts


########################################################################### 
def check_list_valid_input(config, blockname, modname, objname, objdict, indent=''):
    cnts = [0 for i in range(0,NUMCNTS)]

    (sect, name, subname) = parse_wcl_objname(objname)

    # check that it has filepat, filename, or query code (required)
    # if filename is a pattern, can I check that all needed values exist?
    if pfwdefs.SW_FILEPAT not in objdict and \
        pfwdefs.FILENAME not in objdict and \
        'output_fields' not in objdict:
        print "%sError: block %s, module %s, %s, %s - Missing terms needed to determine list filename" % (indent, blockname, modname, pfwdefs.SW_INPUTS, objname)
        cnts[ERRCNT_POS] += 1
    
    # check that it has pfwdefs.DIRPAT :    err
    # can I check that all values for pfwdefs.DIRPAT exist?
    if pfwdefs.DIRPAT not in objdict:
        print "%sError: block %s, module %s, %s, %s - Missing %s" % (indent, blockname, modname, pfwdefs.SW_INPUTS, objname, pfwdefs.DIRPAT)
        cnts[ERRCNT_POS] += 1

    return cnts


########################################################################### 
def check_exec_inputs(config, blockname, modname, dataobjs, xsectname, xsectdict, indent=''):
    # initialize
    cnts = [0 for i in range(0,NUMCNTS)]
    moddict = config[pfwdefs.SW_MODULESECT][modname]

    if pfwdefs.SW_INPUTS in xsectdict:
        print "%sChecking %s %s..." % (indent, xsectname, pfwdefs.SW_INPUTS)
        indent += '    '
        
        #print "%sxsectdict[pfwdefs.SW_INPUTS] = %s" % (indent, xsectdict[pfwdefs.SW_INPUTS])

        # for each entry in inputs
        for objname in miscutils.fwsplit(xsectdict[pfwdefs.SW_INPUTS],','):
            objname = objname.lower()
            #print '%sobjname=%s' % (indent, objname)

            (sect, name, subname) = parse_wcl_objname(objname) 
            #print '%s(sect, name, subname) = (%s, %s, %s)' % (indent, sect, name, subname)
            if sect is None:
                print "%s    Error: block %s, module %s, %s, %s - Invalid entry (%s).  Missing section label" % (indent, blockname, modname, xsectname, pfwdefs.SW_INPUTS, objname)
                cnts[ERRCNT_POS] += 1
            else:
                # check that appears in [file/list]sect : err
                if sect not in moddict or name not in moddict[sect]:
                    print "%s    Error:  block %s, module %s, %s, %s - Invalid entry (%s).  Cannot find definition." % (indent, blockname, modname, xsectname, pfwdefs.SW_INPUTS, objname)
                    cnts[ERRCNT_POS] += 1
                elif subname is None:  # file
                    dataobjs[pfwdefs.SW_INPUTS][objname] = True
                elif sect != pfwdefs.SW_LISTSECT:   # only lists can have subname
                    print "%sError: block %s, module %s, %s, %s, %s - Too many sections/periods for a %s." % (indent, blockname, modname, xsectname, pfwdefs.SW_INPUTS, objname, sect)
                    cnts[ERRCNT_POS] += 1
                elif subname not in moddict[pfwdefs.SW_FILESECT]:
                    print "%sError: block %s, module %s, %s, %s, %s - Cannot find definition for %s" % (indent, blockname, modname, xsectname, pfwdefs.SW_INPUTS, objname, subname)
                    cnts[ERRCNT_POS] += 1
                else:
                    dataobjs[pfwdefs.SW_INPUTS]["%s.%s" % (pfwdefs.SW_LISTSECT,name)] = True
                    dataobjs[pfwdefs.SW_INPUTS]["%s.%s" % (pfwdefs.SW_FILESECT,subname)] = True
                    dataobjs[pfwdefs.SW_INPUTS][objname] = True
                    fdict = moddict[pfwdefs.SW_FILESECT][subname]
                    if ('listonly' not in fdict or not miscutils.convertBool(fdict['listonly'])): 
                        print "%sWarning: block %s, module %s, %s, %s, %s - File in list does not have listonly=True" % (indent, blockname, modname, xsectname, pfwdefs.SW_INPUTS, objname)
                        cnts[WARNCNT_POS] += 1

    return cnts
         

########################################################################### 
def check_file_valid_output(config, blockname, modname, fname, fdict, indent=''):
    cnts = [0 for i in range(0,NUMCNTS)]
    moddict = config[pfwdefs.SW_MODULESECT][modname]

    # check that it has pfwdefs.DIRPAT :    err
    # can I check that all values for pfwdefs.DIRPAT exist?
    if pfwdefs.DIRPAT not in fdict:
        print "%sError: block %s, module %s, %s, %s - Missing %s" % (indent, blockname, modname, pfwdefs.SW_OUTPUTS, fname, pfwdefs.DIRPAT)
        cnts[ERRCNT_POS] += 1
    else:
        # todo: check that all values for pfwdefs.DIRPAT exist
        pass

    # check that it has filepat, filename (required)
    if pfwdefs.SW_FILEPAT not in fdict and \
       pfwdefs.FILENAME not in fdict and \
       'fullname' not in fdict:
        print "%sError: block %s, module %s, %s, %s - Missing terms needed to determine output filename" % (indent, blockname, modname, pfwdefs.SW_OUTPUTS, fname)
        cnts[ERRCNT_POS] += 1
    else:

        # check that any given filename pattern has a definition
        if pfwdefs.SW_FILEPAT in fdict:
            cnts2 = check_filepat_valid(config, fdict[pfwdefs.SW_FILEPAT], blockname, modname, fname, fdict, indent + '    ')
            cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts

    # check that it has filetype :    err
    if pfwdefs.FILETYPE not in fdict:
        print "%sError: block %s, module %s, %s, %s - Missing %s" % (indent, blockname, modname, pfwdefs.SW_OUTPUTS, fname, pfwdefs.FILETYPE)
        cnts[ERRCNT_POS] += 1
    elif fdict[pfwdefs.FILETYPE] not in config[fmdefs.FILETYPE_METADATA]:
        print "%sError: block %s, module %s, %s, %s - Invalid %s (%s)" % (indent, blockname, modname, pfwdefs.SW_OUTPUTS, fname, pfwdefs.FILETYPE, fdict[pfwdefs.FILETYPE])
        cnts[ERRCNT_POS] += 1
               
    return cnts


########################################################################### 
def check_exec_outputs(config, blockname, modname, dataobjs, xsectname, xsectdict, indent=''):
    # initialize
    cnts = [0 for i in range(0,NUMCNTS)]
    moddict = config[pfwdefs.SW_MODULESECT][modname]
    

    if pfwdefs.SW_OUTPUTS in xsectdict:
        # for each entry in inputs
        print "%sChecking %s %s..." % (indent, xsectname, pfwdefs.SW_OUTPUTS)
        indent += '    '
        #print "%sxsectdict[pfwdefs.SW_OUTPUTS] = %s" % (indent, xsectdict[pfwdefs.SW_OUTPUTS])
        for objname in miscutils.fwsplit(xsectdict[pfwdefs.SW_OUTPUTS],','):
            objname = objname.lower()
            #print '%sobjname=%s' % (indent, objname)

            (sect, name, subname) = parse_wcl_objname(objname) 
            #print '%s(sect, name, subname) = (%s, %s, %s)' % (indent, sect, name, subname)
            if sect is None:
                print "%s    Error: block %s, module %s, %s, %s - Invalid entry (%s).  Missing section label" % (indent, blockname, modname, xsectname, pfwdefs.SW_OUTPUTS, objname)
                cnts[ERRCNT_POS] += 1
            else:
                # check that appears in [file/list]sect : err
                if sect not in moddict or name not in moddict[sect]:
                    print "%s    Error:  block %s, module %s, %s, %s - Invalid entry (%s).  Cannot find definition." % (indent, blockname, modname, xsectname, pfwdefs.SW_OUTPUTS, objname)
                    cnts[ERRCNT_POS] += 1
                else:
                    dataobjs[pfwdefs.SW_OUTPUTS][objname] = True

    return cnts




########################################################################### 
def check_exec_parentchild(config, blockname, modname, dataobjs, xsectname, xsectdict, indent=''):
    # check that parent and children appear in inputs and outputs
    # assumes check_exec_input and check_exec_output have already been executed so there are entries in dataobjs

    cnts = [0 for i in range(0,NUMCNTS)]
    if pfwdefs.SW_PARENTCHILD in xsectdict:
        print "%sChecking %s %s..." % (indent, xsectname, pfwdefs.SW_PARENTCHILD)
        indent += '    '
        #print "%sxsectdict[pfwdefs.SW_PARENTCHILD] = %s" % (indent, xsectdict[pfwdefs.SW_PARENTCHILD])
        #print "%sdataobjs[pfwdefs.SW_INPUTS] = %s" % (indent, dataobjs[pfwdefs.SW_INPUTS])
        #print "%sdataobjs[pfwdefs.SW_OUTPUTS] = %s" % (indent, dataobjs[pfwdefs.SW_OUTPUTS])
        #print "%sfsplit = %s" % (indent, miscutils.fwsplit(xsectdict[pfwdefs.SW_PARENTCHILD],',') )

        for pair in miscutils.fwsplit(xsectdict[pfwdefs.SW_PARENTCHILD],','):
            pair = pair.lower()
            if ':' in pair:
                (parent, child) = miscutils.fwsplit(pair, ':') 
                if '.' in parent:
                    if parent not in dataobjs[pfwdefs.SW_INPUTS]:
                        print "%sError: block %s, module %s, %s, %s - parent %s not listed in %s" % (indent, blockname, modname, xsectname, pfwdefs.SW_PARENTCHILD, parent, pfwdefs.SW_INPUTS)
                        cnts[ERRCNT_POS] += 1
                else:
                    print "%sError: block %s, module %s, %s, %s - parent %s missing section label" % (indent, blockname, modname, xsectname, pfwdefs.SW_PARENTCHILD, parent)
                    cnts[ERRCNT_POS] += 1
    
                if '.' in child:
                    if child not in dataobjs[pfwdefs.SW_OUTPUTS]:
                        print "%sError: block %s, module %s, %s, %s - child %s not listed in %s" % (indent, blockname, modname, xsectname, pfwdefs.SW_PARENTCHILD, child, pfwdefs.SW_OUTPUTS)
                        cnts[ERRCNT_POS] += 1
                else:
                    print "%sError: block %s, module %s, %s, %s - child %s missing section label" % (indent, blockname, modname, xsectname, pfwdefs.SW_PARENTCHILD, child)
                    cnts[ERRCNT_POS] += 1
            else:
                print "%sError: block %s, module %s, %s, %s - Invalid parent/child pair (%s).  Missing colon." % (indent, blockname, modname, xsectname, pfwdefs.SW_PARENTCHILD, pair)
                cnts[ERRCNT_POS] += 1
    elif pfwdefs.SW_INPUTS in xsectdict and pfwdefs.SW_OUTPUTS in xsectdict:
        print "%sWarning: block %s, module %s, %s - has %s and %s, but not %s" % (indent, blockname, modname, xsectname, pfwdefs.SW_INPUTS, pfwdefs.SW_OUTPUTS, pfwdefs.SW_PARENTCHILD)
        cnts[WARNCNT_POS] += 1

    return cnts



########################################################################### 
def check_dataobjs(config, blockname, modname, moddict, dataobjs, indent=''):
    """ calls functions to check files have all needed info as well as note extra file defs """
    
    cnts = [0 for i in range(0,NUMCNTS)]

    # check every file
    if pfwdefs.SW_FILESECT in moddict:
        print "%sChecking %s section..." % (indent, pfwdefs.SW_FILESECT)
        for fname,fdict in moddict[pfwdefs.SW_FILESECT].items():
            key = '%s.%s' % (pfwdefs.SW_FILESECT, fname)
            if key not in dataobjs[pfwdefs.SW_INPUTS] and \
               key not in dataobjs[pfwdefs.SW_OUTPUTS] and \
               ('listonly' not in fdict or not miscutils.convertBool('listonly')): 
                print "%sWarning: %s.%s does not appear in provenance lines" % (indent+'    ', pfwdefs.SW_FILESECT, fname)
                cnts[WARNCNT_POS] += 1

            if key in dataobjs[pfwdefs.SW_INPUTS]:
                cnts2 = check_file_valid_input(config, blockname, modname, fname, fdict, indent+'    ')
                cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
                
    
            if key in dataobjs[pfwdefs.SW_OUTPUTS]:
                cnts2 = check_file_valid_output(config, blockname, modname, fname, fdict, indent+'    ')
                cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
                
            
    # check every list
    if pfwdefs.SW_LISTSECT in moddict:
        print "%sChecking %s section..." % (indent, pfwdefs.SW_LISTSECT)
        for lname,ldict in moddict[pfwdefs.SW_LISTSECT].items():
            key = '%s.%s' % (pfwdefs.SW_LISTSECT, lname)
            if key not in dataobjs[pfwdefs.SW_INPUTS] and \
               key not in dataobjs[pfwdefs.SW_OUTPUTS]:
                print "%sWarning: %s.%s does not appear in provenance lines" % (indent, pfwdefs.SW_LISTSECT, lname)
                cnts[WARNCNT_POS] += 1

            if key in dataobjs[pfwdefs.SW_INPUTS]:
                cnts2 = check_list_valid_input(config, blockname, modname, lname, ldict, indent+'    ')
                cnts = [x + y for x, y in zip(cnts, cnts2)] # increment counts
            

    return cnts



########################################################################### 
def check_exec_cmd(config, blockname, modname, dataobjs, xsectname, xsectdict, indent=''): 
    cnts = [0 for i in range(0,NUMCNTS)]

    # check that each exec section has execname (required)
    if pfwdefs.SW_EXECNAME not in xsectdict:
        print "%sError: block %s, module %s, %s - missing %s" % (indent, blockname, modname, xsectname, pfwdefs.SW_EXECNAME)
        cnts[ERRCNT_POS] += 1
    elif '/' in xsectdict[pfwdefs.SW_EXECNAME]:
        print "%sWarning: block %s, module %s, %s - hardcoded path in %s (%s)" % (indent, blockname, modname, xsectname, pfwdefs.SW_EXECNAME, xsectdict[pfwdefs.SW_EXECNAME])
        cnts[WARNCNT_POS] += 1

    # almost all production cases would need to have command line arguments
    if pfwdefs.SW_CMDARGS not in xsectdict:
        print "%sWarning: block %s, module %s, %s - missing %s" % (indent, blockname, modname, xsectname, pfwdefs.SW_CMDARGS)
        cnts[WARNCNT_POS] += 1
    else:
        moddict = config[pfwdefs.SW_MODULESECT][modname]
        argvars = pfwutils.search_wcl_for_variables(xsectdict[pfwdefs.SW_CMDARGS])
        for var in argvars:
            if var.endswith('.fullname'):
                var2 = var[0:-(len('.fullname'))]
                (sect, name, subname) = parse_wcl_objname(var2)
                if sect not in moddict or name not in moddict[sect]:
                    print "%sError: block %s, module %s, %s, %s - Undefined variable (%s)" % (indent, blockname, modname, xsectname, pfwdefs.SW_CMDARGS, var)
                    cnts[ERRCNT_POS] += 1

                if subname and subname not in moddict[pfwdefs.SW_FILESECT]:
                    print "%sError: block %s, module %s, %s, %s - Undefined variable (%s)" % (indent, blockname, modname, xsectname, pfwdefs.SW_CMDARGS, var)
                    cnts[ERRCNT_POS] += 1
            else:
                (found, val) = config.search(var, { pfwdefs.PF_CURRVALS: {'curr_block': blockname, 'curr_module': modname}, 'searchobj': xsectdict, 'required':False, 'interpolate': True})
#                if found:
#                    val2 = config.interpolate(val, { pfwdefs.PF_CURRVALS: {'curr_block': blockname, 'curr_module': modname}, 'searchobj': xsectdict, 'interpolate': True, 'required':False})
                    
                    
                
                
                

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
