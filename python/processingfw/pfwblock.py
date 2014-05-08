#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

""" functions used by the block tasks """

import sys
import stat
import os
from collections import OrderedDict
import itertools
import copy
import re
import time
import processingfw.pfwdefs as pfwdefs
#import filemgmt.filemgmt_defs as fmdefs
import coreutils.miscutils as coremisc
import intgutils.metadefs as imetadefs
import coreutils.dbsemaphore as dbsem
import filemgmt.archive_transfer_utils as archive_transfer_utils
import intgutils.wclutils as wclutils
import intgutils.metautils as metautils
import processingfw.pfwutils as pfwutils
import processingfw.pfwcondor as pfwcondor
from processingfw.pfwwrappers import write_wrapper_wcl

#######################################################################
def create_simple_list(config, lname, ldict, currvals):
    """ Create simple filename list file based upon patterns """
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    listname = config.search('listname', 
                            {pfwdefs.PF_CURRVALS: currvals, 
                             'searchobj': ldict, 
                             'required': True, 
                             'interpolate': True})[1]

    filename = config.get_filename(None,
                            {pfwdefs.PF_CURRVALS: currvals, 
                             'searchobj': ldict, 
                             'required': True, 
                             'interpolate': True})[1]

    if type(filename) is list:
        listcontents = '\n'.join(filename)
    else:
        listcontents = filename
 
    listdir = os.path.dirname(listname)
    if len(listdir) > 0 and not os.path.exists(listdir):
        coremisc.coremakedirs(listdir)

    with open(listname, 'w', 0) as listfh:
        listfh.write(listcontents+"\n")
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def get_match_keys(sdict):
    mkeys = []

    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "keys in sdict: %s " % sdict.keys())
    if 'loopkey' in sdict:
        mkeys = coremisc.fwsplit(sdict['loopkey'].lower())
        mkeys.sort()
    elif 'match' in sdict:
        mkeys = coremisc.fwsplit(sdict['match'].lower())
        mkeys.sort()
    elif 'divide_by' in sdict:
        mkeys = coremisc.fwsplit(sdict['divide_by'].lower())
        mkeys.sort()
    
    return mkeys


#######################################################################
def find_sublist(objDef, objInst):

    if len(objDef['sublists'].keys()) > 1:
        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "sublist keys: %s" % (objDef['sublists'].keys()))
        matchkeys = get_match_keys(objDef)
        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "matchkeys: %s" % (matchkeys))
        index = ""
        for mkey in matchkeys:
            if mkey not in objInst:
                coremisc.fwdie("Error: Cannot find match key %s in inst %s" % (mkey, objInst), pfwdefs.PF_EXIT_FAILURE)
            index += objInst[mkey] + '_'
        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "sublist index = "+index)
        if index not in objDef['sublists']:
            coremisc.fwdie("Error: Cannot find sublist matching "+index, pfwdefs.PF_EXIT_FAILURE)
        sublist = objDef['sublists'][index]
    else:
        sublist = objDef['sublists'].values()[0]

    return sublist

#######################################################################
def which_are_inputs(config, modname):
    """ Return dict of files/lists that are inputs for given module """
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    inputs = {pfwdefs.SW_FILESECT: [], pfwdefs.SW_LISTSECT: []}
    outfiles = {}
    
    # For wrappers with more than 1 exec section, the inputs of one exec can be the inputs of a 2nd exec
    #      the framework should not attempt to stage these intermediate files 
    execs = pfwutils.get_exec_sections(config[pfwdefs.SW_MODULESECT][modname], pfwdefs.SW_EXECPREFIX)
    for ekey, einfo in sorted(execs.items()):
        if pfwdefs.SW_OUTPUTS in einfo:
            for outfile in coremisc.fwsplit(einfo[pfwdefs.OW_OUTPUTS]):
                outfiles[outfile] = True
             
        if pfwdefs.SW_INPUTS in einfo: 
            inarr = coremisc.fwsplit(einfo[pfwdefs.SW_INPUTS].lower())
            for inname in inarr:
                if inname not in outfiles:
                    parts = coremisc.fwsplit(inname, '.') 
                    inputs[parts[0]].append('.'.join(parts[1:]))

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", inputs)
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END")
    return inputs
            

#######################################################################
def which_are_outputs(config, modname):
    """ Return dict of files that are outputs for given module """
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    outfiles = {}
    
    execs = pfwutils.get_exec_sections(config[pfwdefs.SW_MODULESECT][modname], pfwdefs.SW_EXECPREFIX)
    for ekey, einfo in sorted(execs.items()):
        if pfwdefs.SW_OUTPUTS in einfo:
            for outfile in coremisc.fwsplit(einfo[pfwdefs.OW_OUTPUTS]):
                parts = coremisc.fwsplit(outfile, '.')
                outfiles['.'.join(parts[1:])] = True

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", outfiles.keys())
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END")
    return outfiles.keys()




    
#######################################################################
def assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals, winst, fname, finfo, is_iter_obj=False):
    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "BEG: Working on file %s" % fname)
    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "theinputs: %s" % theinputs)
    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "outputs: %s" % theoutputs)

    if 'listonly' in finfo and coremisc.convertBool(finfo['listonly']):
        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "Skipping %s due to listonly key" % fname)
        return

    if pfwdefs.IW_FILESECT not in winst:
        winst[pfwdefs.IW_FILESECT] = {}

    winst[pfwdefs.IW_FILESECT][fname] = {}
    if 'sublists' in finfo:  # files came from query
        sublist = find_sublist(finfo, winst)
        if len(sublist['list'][pfwdefs.PF_LISTENTRY]) > 1:
            coremisc.fwdie("Error: more than 1 line to choose from for file (%s)" % sublist['list'][pfwdefs.PF_LISTENTRY], pfwdefs.PF_EXIT_FAILURE)
        line = sublist['list'][pfwdefs.PF_LISTENTRY].values()[0]
        if 'file' not in line:
            coremisc.fwdie("Error: 0 file in line" + str(line), PW_EXIT_FAILURE)
            
        if len(line['file']) > 1:
            raise Exception("more than 1 file to choose from for file" + line['file'])
        finfo = line['file'].values()[0]
        coremisc.fwdebug(6, "PFWBLOCK_DEBUG", "finfo = %s" % finfo)

        fullname = finfo['fullname']
        winst[pfwdefs.IW_FILESECT][fname]['fullname'] = fullname

        # save input and output filenames (with job scratch path)
        # In order to preserve capitalization, put on right side of =, using dummy count for left side
        if fname in theinputs[pfwdefs.SW_FILESECT]:
            winst['wrapinputs'][len(winst['wrapinputs'])+1] = fullname
        elif fname in theoutputs:
            winst['wrapoutputs'][len(winst['wrapoutputs'])+1] = fullname

        coremisc.fwdebug(6, "PFWBLOCK_DEBUG", "Assigned filename for fname %s (%s)" % (fname, finfo['filename']))
    elif 'fullname' in moddict[pfwdefs.SW_FILESECT][fname]:
        winst[pfwdefs.IW_FILESECT][fname]['fullname'] = moddict[pfwdefs.SW_FILESECT][fname]['fullname']
        coremisc.fwdebug(6, "PFWBLOCK_DEBUG", "Copied fullname for %s = %s" % (fname, winst[pfwdefs.IW_FILESECT][fname]))
        if fname in theinputs[pfwdefs.SW_FILESECT]:
            winst['wrapinputs'][len(winst['wrapinputs'])+1] = moddict[pfwdefs.SW_FILESECT][fname]['fullname']
        elif fname in theoutputs:
            winst['wrapoutputs'][len(winst['wrapoutputs'])+1] = moddict[pfwdefs.SW_FILESECT][fname]['fullname']
    else:
        if 'filename' in moddict[pfwdefs.SW_FILESECT][fname]:
            winst[pfwdefs.IW_FILESECT][fname]['filename'] = config.search('filename', {pfwdefs.PF_CURRVALS: currvals, 
                                                                               'searchobj': moddict[pfwdefs.SW_FILESECT][fname], 
                                                                               'expand': True, 
                                                                               'required': True,
                                                                               'interpolate':True})[1]
        else:
            coremisc.fwdebug(6, "PFWBLOCK_DEBUG", "creating filename for %s" % fname) 
            sobj = copy.deepcopy(finfo)
            sobj.update(winst)
            winst[pfwdefs.IW_FILESECT][fname]['filename'] = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals, 
                                                                'searchobj': sobj,
                                                                'expand': True}) 

        # Add runtime path to filename
        coremisc.fwdebug(3,"PFWBLOCK_DEBUG", "creating path for %s" % fname)
        path = config.get_filepath('runtime', None, {pfwdefs.PF_CURRVALS: currvals, 'searchobj': finfo})
        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "\tpath = %s" % path)
        if type(winst[pfwdefs.IW_FILESECT][fname]['filename']) is list:
            winst[pfwdefs.IW_FILESECT][fname]['fullname'] = []
            coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "%s is a list, number of names = %s" % (fname,len(winst[pfwdefs.IW_FILESECT][fname]['filename'])))
            for f in winst[pfwdefs.IW_FILESECT][fname]['filename']:
                coremisc.fwdebug(6, "PFWBLOCK_DEBUG", "path + filename = %s/%s" % (path,f))
                winst[pfwdefs.IW_FILESECT][fname]['fullname'].append("%s/%s" % (path, f))
                if fname in theinputs[pfwdefs.SW_FILESECT]:
                    winst['wrapinputs'][len(winst['wrapinputs'])+1] = "%s/%s" % (path,f)
                elif fname in theoutputs:
                    winst['wrapoutputs'][len(winst['wrapoutputs'])+1] = "%s/%s" % (path,f) 

            winst[pfwdefs.IW_FILESECT][fname]['fullname'] = ','.join(winst[pfwdefs.IW_FILESECT][fname]['fullname'])
        else:
            coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "Adding path to filename for %s" % fname)
            winst[pfwdefs.IW_FILESECT][fname]['fullname'] = "%s/%s" % (path, winst[pfwdefs.IW_FILESECT][fname]['filename'])
            if fname in theinputs[pfwdefs.SW_FILESECT]:
                winst['wrapinputs'][len(winst['wrapinputs'])+1] = winst[pfwdefs.IW_FILESECT][fname]['fullname']
            elif fname in theoutputs:
                winst['wrapoutputs'][len(winst['wrapoutputs'])+1] = winst[pfwdefs.IW_FILESECT][fname]['fullname']


        del winst[pfwdefs.IW_FILESECT][fname]['filename']

    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "is_iter_obj = %s %s" % (is_iter_obj, finfo))
    if finfo is not None and is_iter_obj:
        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "is_iter_obj = true")
        for key,val in finfo.items():
            if key not in ['fullname','filename']:
                coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "is_iter_obj: saving %s" % key)
                winst[key] = val
        
    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "END: Done working on file %s" % fname)



#######################################################################
def assign_list_to_wrapper_inst(config, moddict, currvals, winst, lname, ldict):
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG: Working on list %s from %s" % (lname, moddict['modulename']))
    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "currvals = %s" % (currvals))
    coremisc.fwdebug(6, "PFWBLOCK_DEBUG", "ldict = %s" % (ldict))

    if pfwdefs.IW_LISTSECT not in winst:
        winst[pfwdefs.IW_LISTSECT] = {}

    winst[pfwdefs.IW_LISTSECT][lname] = {}

    sobj = copy.deepcopy(ldict)
    sobj.update(winst)
    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "sobj = %s" % (sobj))

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "creating listdir and listname")

    listdir = config.get_filepath('runtime', 'list', {pfwdefs.PF_CURRVALS: currvals,
                         'required': True, 'interpolate': True,
                         'searchobj': sobj})
    
    listname = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals,
                                   'searchobj': sobj, 'required': True, 'interpolate': True})
    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "listname = %s" % (listname))
    listname = "%s/%s" % (listdir, listname)

    winst[pfwdefs.IW_LISTSECT][lname]['fullname'] = listname
    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "full listname = %s" % (winst[pfwdefs.IW_LISTSECT][lname]['fullname']))
    if 'sublists' in ldict:
        sublist = find_sublist(ldict, winst)
        for llabel,lldict in sublist['list'][pfwdefs.PF_LISTENTRY].items():
            for flabel,fdict in lldict['file'].items():
                winst['wrapinputs'][len(winst['wrapinputs'])+1] = fdict['fullname']
        output_list(config, winst[pfwdefs.IW_LISTSECT][lname]['fullname'], sublist, lname, ldict, currvals)

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END")

                        
#######################################################################
def assign_data_wrapper_inst(config, modname, wrapperinst):
    """ Assign data like files and lists to wrapper instances """
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    moddict = config[pfwdefs.SW_MODULESECT][modname] 
    currvals = { 'curr_module': modname}
    (found, loopkeys) = config.search('wrapperloop', 
                       {pfwdefs.PF_CURRVALS: currvals,
                        'required': False, 'interpolate': True})
    if found:
        loopkeys = coremisc.fwsplit(loopkeys.lower())
    else:
        loopkeys = []

    # figure out which lists/files are input files
    theinputs = which_are_inputs(config, modname)
    theoutputs = which_are_outputs(config, modname)

    for winst in wrapperinst.values():
        winst['wrapinputs'] = {}
        winst['wrapoutputs'] = {}

        # create currvals
        currvals = { 'curr_module': modname, pfwdefs.PF_WRAPNUM: winst[pfwdefs.PF_WRAPNUM]}
        for key in loopkeys:
            currvals[key] = winst[key]
        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "currvals " + str(currvals))

        # do wrapper loop object first, if exists, to provide keys for filenames
        iter_obj_key = get_wrap_iter_obj_key(config, moddict)

        if iter_obj_key is not None or pfwdefs.SW_FILESECT in moddict:
            coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "%s: Assigning files to wrapper inst" % winst[pfwdefs.PF_WRAPNUM])

        if iter_obj_key is not None:
            (iter_obj_sect, iter_obj_name) = coremisc.fwsplit(iter_obj_key, '.')
            iter_obj_dict = pfwutils.get_wcl_value(iter_obj_key, moddict) 
            coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "iter_obj %s %s" % (iter_obj_name, iter_obj_sect))
            if iter_obj_sect.lower() == pfwdefs.SW_FILESECT.lower():
                assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals, winst, iter_obj_name, iter_obj_dict, True)
            elif iter_obj_sect.lower() == pfwdefs.SW_LISTSECT.lower():
                assign_list_to_wrapper_inst(config, moddict, currvals, winst, iter_obj_name, iter_obj_dict)
            else:
                coremisc.fwdie("Error: unknown iter_obj_sect (%s)" % iter_obj_sect, pfwdefs.PF_EXIT_FAILURE)

        
        if pfwdefs.SW_FILESECT in moddict:
            for fname, fdict in moddict[pfwdefs.SW_FILESECT].items(): 
                if iter_obj_key is not None and \
                   iter_obj_sect.lower() == pfwdefs.SW_FILESECT.lower() and \
                   iter_obj_name.lower() == fname.lower():
                    continue    # already did iter_obj
                assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals, winst, fname, fdict)

        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "currvals " + str(currvals))

        if pfwdefs.SW_LISTSECT in moddict:
            coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "%s: Assigning lists to wrapper inst" % winst[pfwdefs.PF_WRAPNUM])
            for lname, ldict in moddict[pfwdefs.SW_LISTSECT].items():
                if iter_obj_key is not None and \
                   iter_obj_sect.lower() == pfwdefs.SW_LISTSECT.lower() and \
                   iter_obj_name.lower() == lname.lower():
                    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "skipping list %s as already did for it as iter_obj" % lname)
                    continue    # already did iter_obj
                assign_list_to_wrapper_inst(config, moddict, currvals, winst, lname, ldict)
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def output_list(config, listname, sublist, lname, ldict, currvals):
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG: %s (%s)" % (lname, listname))
    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "list dict: %s" % ldict)

    listdir = os.path.dirname(listname)
    coremisc.coremakedirs(listdir)

    format = 'textsp'
    if 'format' in ldict:
        format = ldict['format']

    if 'columns' in ldict:
        columns = ldict['columns'].lower()
    else:
        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "columns not in ldict, so defaulting to fullname")
        columns = 'fullname'
    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "columns = %s" % columns)
    

    with open(listname, "w") as listfh:
        for linenick, linedict in sublist['list'][pfwdefs.PF_LISTENTRY].items():
            output_line(listfh, linedict, format, coremisc.fwsplit(columns))
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")




#####################################################################
def output_line(listfh, line, format, keyarr):
    """ output line into fo input list for science code"""
    coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "BEG line=%s  keyarr=%s" % (line, keyarr))

    format = format.lower()

    if format == 'config' or format == 'wcl':
        fh.write("<file>\n")

    numkeys = len(keyarr)
    for i in range(0, numkeys):
        key = keyarr[i]
        value = None
        coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "key: %s" % key)

        if '.' in  key:
            coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "Found period in key")
            [nickname, key2] = key.replace(' ','').split('.')
            coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "\tnickname = %s, key2 = %s" % (nickname, key2))
            value = get_value_from_line(line, key2, nickname, None)
            if value == None:
                coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "Didn't find value in line with nickname %s" % (nickname))
                coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "Trying to find %s without nickname" % (key2))
                value = get_value_from_line(line, key2, None, 1)
                if value == None:
                    coremisc.fwdie("Error: could not find value %s for line...\n%s" % (key, line), pfwdefs.PF_EXIT_FAILURE)
                else: # assume nickname was really table name
                    coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "\tassuming nickname (%s) was really table name" % (nickname))
                    key = key2
        else:
            value = get_value_from_line(line, key, None, 1)

        # handle last field (separate to avoid trailing comma)
        coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "printing key=%s value=%s" % (key, value))
        if i == numkeys - 1:
            print_value(listfh, key, value, format, True)
        else:
            print_value(listfh, key, value, format, False)

    if format == "config" or format == 'wcl':
        listfh.write("</file>\n")
    else:
        listfh.write("\n")


#####################################################################
def print_value(fh, key, value, format, last):
    """ output value to input list in correct format """
    format = format.lower()
    if format == 'config' or format == 'wcl':
        fh.write("     %s=%s\n" % (key,value))
    else:
        fh.write(value)
        if not last:
            if format == 'textcsv':
                fh.write(',')
            elif format == 'texttab':
                fh.write('\t')
            else:
                fh.write(' ')
    


#######################################################################
def finish_wrapper_inst(config, modname, wrapperinst):
    """ Finish creating wrapper instances with tasks like making input and output filenames """
    
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    moddict = config[pfwdefs.SW_MODULESECT][modname] 
    outputfiles = which_are_outputs(config, modname)

    input_filenames = []
    output_filenames = []
    for winst in wrapperinst.values():
        for f in winst['wrapinputs'].values():
            input_filenames.append(coremisc.parse_fullname(f, coremisc.CU_PARSE_FILENAME))

        for f in winst['wrapoutputs'].values():
            output_filenames.append(coremisc.parse_fullname(f, coremisc.CU_PARSE_FILENAME))



        # create searching options
        currvals = {'curr_module': modname, pfwdefs.PF_WRAPNUM: winst[pfwdefs.PF_WRAPNUM]}
        searchopts = {pfwdefs.PF_CURRVALS: currvals, 
                      'searchobj': winst, 
                      'interpolate': True,
                      'required': True}

        
        if pfwdefs.SW_FILESECT in moddict:
            for fname, fdict in moddict[pfwdefs.SW_FILESECT].items(): 
                if 'listonly' in fdict and coremisc.convertBool(fdict['listonly']):
                    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "Skipping %s due to listonly key" % fname)
                    continue

                coremisc.fwdebug(3, 'PFWBLOCK_DEBUG', '%s: working on file: %s' % (winst[pfwdefs.PF_WRAPNUM], fname))
                coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "fullname = %s" % (winst[pfwdefs.IW_FILESECT][fname]['fullname']))

                
                for k in ['filetype', imetadefs.WCL_META_REQ, imetadefs.WCL_META_OPT, pfwdefs.SAVE_FILE_ARCHIVE, pfwdefs.DIRPAT]:
                    if k in fdict:
                        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "%s copying %s" % (fname, k))
                        winst[pfwdefs.IW_FILESECT][fname][k] = copy.deepcopy(fdict[k])
                    else:
                        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "%s: no %s" % (fname, k))

                if pfwdefs.SW_OUTPUT_OPTIONAL in fdict:
                    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "%s copying %s " % (fname, pfwdefs.SW_OUTPUT_OPTIONAL))
                    
                    winst[pfwdefs.IW_FILESECT][fname][pfwdefs.IW_OUTPUT_OPTIONAL] = coremisc.convertBool(fdict[pfwdefs.SW_OUTPUT_OPTIONAL])

                hdrups = pfwutils.get_hdrup_sections(fdict, imetadefs.WCL_UPDATE_HEAD_PREFIX)
                for hname, hdict in hdrups.items():
                    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "%s copying %s" % (fname, hname))
                    winst[pfwdefs.IW_FILESECT][fname][hname] = copy.deepcopy(hdict)

                # save OPS path for archive
                coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "Is fname (%s) in outputfiles? %s" % (fname, fname in outputfiles))
                filesave = coremisc.checkTrue(pfwdefs.SAVE_FILE_ARCHIVE, fdict, True)
                coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "Is save_file_archive true? %s" % (filesave))
                mastersave = config[pfwdefs.MASTER_SAVE_FILE]
                if fname in outputfiles:
                    winst[pfwdefs.IW_FILESECT][fname][pfwdefs.SAVE_FILE_ARCHIVE] = filesave  # canonicalize
                    if pfwdefs.DIRPAT not in fdict:
                        print "Warning: Could not find %s in %s's section" % (pfwdefs.DIRPAT,fname)
                    else:
                        searchobj = copy.deepcopy(fdict)
                        searchobj.update(winst)
                        searchopts['searchobj'] = searchobj
                        winst[pfwdefs.IW_FILESECT][fname]['archivepath'] = config.get_filepath('ops', 
                                                                        fdict[pfwdefs.DIRPAT], searchopts)

            coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "fdict = %s" % fdict)
            coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "winst[%s] = %s" % (pfwdefs.IW_FILESECT,  winst[pfwdefs.IW_FILESECT]))

        if pfwdefs.SW_LISTSECT in moddict:
            for lname, ldict in moddict[pfwdefs.SW_LISTSECT].items(): 
                for k in ['columns']:
                    if k in ldict:
                        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "%s copying %s" % (lname, k))
                        winst[pfwdefs.IW_LISTSECT][lname][k] = copy.deepcopy(ldict[k])
                    else:
                        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "%s: no %s" % (lname, k))

        # wrappername
        winst['wrappername'] = config.search('wrappername', searchopts)[1]

        # input wcl fullname
        inputwcl_name = config.get_filename('inputwcl', searchopts)
        inputwcl_path = config.get_filepath('runtime', 'inputwcl', searchopts) 
        winst['inputwcl'] = inputwcl_path + '/' + inputwcl_name


        # log fullname
        log_name = config.get_filename('log', searchopts)
        log_path = config.get_filepath('runtime', 'log', searchopts)
        winst['log'] = log_path + '/' + log_name
        winst['log_archive_path'] = config.get_filepath('ops', 'log', searchopts)
        output_filenames.append(winst['log'])


        # output wcl fullname
        outputwcl_name = config.get_filename('outputwcl', searchopts)
        outputwcl_path = config.get_filepath('runtime', 'outputwcl', searchopts)
        winst['outputwcl'] = outputwcl_path + '/' + outputwcl_name


    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return input_filenames, output_filenames


#######################################################################
def add_file_metadata(config, modname):
    """ add file metadata sections to a single file section from a module"""

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "Working on module " + modname)
    moddict = config[pfwdefs.SW_MODULESECT][modname]
    execs = pfwutils.get_exec_sections(moddict, pfwdefs.SW_EXECPREFIX)

    if pfwdefs.SW_FILESECT in moddict:
        filemgmt = None         
        try:
            filemgmt_class = coremisc.dynamically_load_class(config['filemgmt'])
            paramDict = config.get_param_info(filemgmt_class.requested_config_vals(), 
                                               {pfwdefs.PF_CURRVALS: {'curr_module': modname}})
            filemgmt = filemgmt_class(config=paramDict)
        except:
            print "Error:  Problems dynamically loading class (%s) in order to get metadata specs" % config['filemgmt']
            raise

        for k in execs:
            if pfwdefs.SW_OUTPUTS in moddict[k]:
                for outfile in coremisc.fwsplit(moddict[k][pfwdefs.SW_OUTPUTS]):
                    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "Working on output file " + outfile)
                    m = re.match('%s.(\w+)' % pfwdefs.SW_FILESECT, outfile)
                    if m:
                        fname = m.group(1)
                        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "Working on file " + fname)
                        fdict = moddict[pfwdefs.SW_FILESECT][fname]
                        filetype = fdict['filetype'].lower()
                        wclsect = "%s.%s" % (pfwdefs.IW_FILESECT, fname)

                        print "len(config[FILE_HEADER_INFO]) =", len(config['FILE_HEADER_INFO'])
                        meta_specs = metautils.get_metadata_specs(filetype, config['FILETYPE_METADATA'], config['FILE_HEADER'], 
                                                        wclsect, updatefits=True)
                        coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "meta_specs = %s" % meta_specs)
                        coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "fdict = %s" % fdict)
                        fdict.update(meta_specs)

             
                        # add descriptions/types to submit-wcl specified updates if missing
                        hdrups = pfwutils.get_hdrup_sections(fdict, imetadefs.WCL_UPDATE_HEAD_PREFIX)
                        for hname, hdict in sorted(hdrups.items()):
                            for key,val in hdict.items():
                                if key != imetadefs.WCL_UPDATE_WHICH_HEAD:
                                    valparts = coremisc.fwsplit(val, '/')
                                    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "hdrup: key, valparts = %s, %s" % (key, valparts))
                                    if len(valparts) == 1:
                                        if 'COPY{' not in valparts[0]:  # wcl specified value, look up rest from config
                                            newvaldict = metautils.create_update_items('V', [key], config['file_header'], header_value={key:val}) 
                                            hdict.update(newvaldict)
                                    elif len(valparts) != 3:  # 3 is valid full spec of update header line
                                        coremisc.fwdie('Error:  invalid header update line (%s = %s)\nNeeds value[/descript/type]' % (key,val), pfwdefs.PF_EXIT_FAILURE)


                        # add some fields needed by framework for processing output wcl (not stored in database)
                        if imetadefs.WCL_META_WCL not in fdict[imetadefs.WCL_META_REQ]:
                            fdict[imetadefs.WCL_META_REQ][imetadefs.WCL_META_WCL] = ''
                        else:
                            fdict[imetadefs.WCL_META_REQ][imetadefs.WCL_META_WCL] += ','

                        fdict[imetadefs.WCL_META_REQ][imetadefs.WCL_META_WCL] += '%(sect)s.fullname,%(sect)s.sectname' % ({'sect':wclsect})
                    else:
                        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "output file %s doesn't have definition (%s) " % (k, pfwdefs.SW_FILESECT))

                coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "output file dictionary for %s = %s" % (outfile, fdict))
                
            else:
                coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "No was_generated_by for %s" % (k))

    else:
        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "No file section (%s)" % pfwdefs.SW_FILESECT)
        
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")

    
                


#######################################################################
def init_use_archive_info(config, jobwcl, which_use_input, which_use_output, which_archive):
    if which_use_input in config:
        jobwcl[which_use_input] = config[which_use_input].lower()
    else:
        jobwcl[which_use_input] = 'never'

    if which_use_output in config:
        jobwcl[which_use_output] = config[which_use_output].lower()
    else:
        jobwcl[which_use_output] = 'never'
    
    if jobwcl[which_use_input] != 'never' or jobwcl[which_use_output] != 'never':
        jobwcl[which_archive] = config[which_archive]
        archive = jobwcl[which_archive]
    else:
        jobwcl[which_archive] = None
        archive = 'no_archive'

    return archive


#######################################################################
def write_jobwcl(config, jobkey, jobdict):
    """ write a little config file containing variables needed at the job level """
    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "BEG jobnum=%s jobkey=%s" % (jobdict['jobnum'], jobkey))

    jobdict['jobwclfile'] = config.get_filename('jobwcl', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM: jobdict['jobnum']}, 'required': True, 'interpolate': True})
    jobdict['outputwcltar'] = config.get_filename('outputwcltar', {pfwdefs.PF_CURRVALS:{'jobnum': jobdict['jobnum']}, 'required': True, 'interpolate': True})

    jobdict['envfile'] = config.get_filename('envfile')

    jobwcl = {pfwdefs.REQNUM: config.search(pfwdefs.REQNUM, { 'required': True,
                                    'interpolate': True})[1], 
              pfwdefs.UNITNAME:config.search(pfwdefs.UNITNAME, { 'required': True,
                                    'interpolate': True})[1], 
              pfwdefs.ATTNUM: config.search(pfwdefs.ATTNUM, { 'required': True,
                                    'interpolate': True})[1], 
              pfwdefs.PF_BLKNUM: config.search(pfwdefs.PF_BLKNUM, { 'required': True,
                                    'interpolate': True})[1], 
              pfwdefs.PF_JOBNUM: jobdict['jobnum'],
              'numexpwrap': len(jobdict['tasks']),
              'usedb': config.search(pfwdefs.PF_USE_DB_OUT, { 'required': True,
                                    'interpolate': True})[1], 
              'useqcf': config.search(pfwdefs.PF_USE_QCF, {'required': True,
                                    'interpolate': True})[1], 
              'pipeprod': config.search('pipeprod', {'required': True,
                                    'interpolate': True})[1], 
              'pipever': config.search('pipever', {'required': True,
                                    'interpolate': True})[1], 
              'jobkeys': jobkey[1:].replace('_',','),
              'archive': config['archive'],
              'output_wcl_tar': jobdict['outputwcltar'],
              'envfile': jobdict['envfile'],
              'junktar': config.get_filename('junktar', {pfwdefs.PF_CURRVALS:{'jobnum': jobdict['jobnum']}}),
              'junktar_archive_path': config.get_filepath('ops', 'junktar', {pfwdefs.PF_CURRVALS:{'jobnum': jobdict['jobnum']}}),
              'runjob_task_id': jobdict['runjob_task_id']
            }

    if pfwdefs.CREATE_JUNK_TARBALL in config and coremisc.convertBool(config[pfwdefs.CREATE_JUNK_TARBALL]):
        jobwcl[pfwdefs.CREATE_JUNK_TARBALL] = True
    else:
        jobwcl[pfwdefs.CREATE_JUNK_TARBALL] = False

    if 'transfer_stats' in config:
        jobwcl['transfer_stats'] = config['transfer_stats']

    if 'transfer_semname' in config:
        jobwcl['transfer_semname'] = config['transfer_semname']

    if pfwdefs.MASTER_SAVE_FILE in config:
        jobwcl[pfwdefs.MASTER_SAVE_FILE] = config[pfwdefs.MASTER_SAVE_FILE]
    else:
        jobwcl[pfwdefs.MASTER_SAVE_FILE] = pfwdefs.MASTER_SAVE_FILE_DEFAULT

    target_archive = init_use_archive_info(config, jobwcl, pfwdefs.USE_TARGET_ARCHIVE_INPUT, 
                                           pfwdefs.USE_TARGET_ARCHIVE_OUTPUT, pfwdefs.TARGET_ARCHIVE)
    home_archive = init_use_archive_info(config, jobwcl, pfwdefs.USE_HOME_ARCHIVE_INPUT, 
                                           pfwdefs.USE_HOME_ARCHIVE_OUTPUT, pfwdefs.HOME_ARCHIVE)

    print "target_archive = ", target_archive
    print "home_archive = ", home_archive


    # include variables needed by target archive's file mgmt class
    if jobwcl[pfwdefs.TARGET_ARCHIVE] is not None:
        print "jobwcl[TARGET_ARCHIVE] = ", jobwcl[pfwdefs.TARGET_ARCHIVE]
        try:
            filemgmt_class = coremisc.dynamically_load_class(config['archive'][target_archive]['filemgmt'])
            valDict = config.get_param_info(filemgmt_class.requested_config_vals())
            jobwcl.update(valDict)
        except Exception as err:
            print "ERROR\nError: creating loading job_file_mvmt class\n%s" % err
            raise

    # include variables needed by home archive's file mgmt class
    if jobwcl[pfwdefs.HOME_ARCHIVE] is not None:
        print "jobwcl[HOME_ARCHIVE] = ", jobwcl[pfwdefs.HOME_ARCHIVE]
        try:
            filemgmt_class = coremisc.dynamically_load_class(config['archive'][home_archive]['filemgmt'])
            valDict = config.get_param_info(filemgmt_class.requested_config_vals(),
                                        {pfwdefs.PF_CURRVALS: config['archive'][home_archive]})
            jobwcl.update(valDict)
        except Exception as err:
            print "ERROR\nError: creating loading job_file_mvmt class\n%s" % err
            raise

    try: 
        jobwcl['job_file_mvmt'] = config['job_file_mvmt'][config['curr_site']][home_archive][target_archive]
    except:
        print "\n\n\nError: Problem trying to find: config['job_file_mvmt'][%s][%s][%s]" % (config['curr_site'], home_archive,target_archive)
        print "USE_HOME_ARCHIVE_INPUT =", jobwcl[pfwdefs.USE_HOME_ARCHIVE_INPUT]
        print "USE_HOME_ARCHIVE_OUTPUT =", jobwcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT]
        print "site =", config['curr_site']
        print "home_archive =", home_archive
        print "target_archive =", target_archive
        print 'job_file_mvmt =' 
        pretty_print_dict(config['job_file_mvmt'])
        print "\n"
        raise

    # include variables needed by job_file_mvmt class
    try:
        jobfilemvmt_class = coremisc.dynamically_load_class(jobwcl['job_file_mvmt']['mvmtclass'])
        valDict = config.get_param_info(jobfilemvmt_class.requested_config_vals(),
                                        {pfwdefs.PF_CURRVALS: jobwcl['job_file_mvmt']})
        jobwcl.update(valDict)
    except Exception as err:
        print "ERROR\nError: creating loading job_file_mvmt class\n%s" % err
        raise


    if coremisc.convertBool(config[pfwdefs.PF_USE_DB_OUT]):
        if 'target_des_services' in config and config['target_des_services'] is not None: 
            jobwcl['des_services'] = config['target_des_services']
        jobwcl['des_db_section'] = config['target_des_db_section']


    jobwcl['filetype_metadata'] = config['filetype_metadata']
    jobwcl[pfwdefs.IW_EXEC_DEF] = config[pfwdefs.SW_EXEC_DEF]
    jobwcl['wrapinputs'] = jobdict['wrapinputs']

    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "jobwcl.keys() = %s" % jobwcl.keys())
   
    tjpad = "%04d" % (int(jobdict['jobnum']))
    coremisc.coremakedirs(tjpad)
    with open("%s/%s" % (tjpad, jobdict['jobwclfile']), 'w') as wclfh:
        wclutils.write_wcl(jobwcl, wclfh, True, 4)

    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "END\n\n")
    

#######################################################################
def add_needed_values(config, modname, wrapinst, wrapwcl):
    """ Make sure all variables in the wrapper instance have values in the wcl """
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    
    # start with those needed by framework
    neededvals = {pfwdefs.REQNUM: config.search(pfwdefs.REQNUM,
                                   {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                    'searchobj': wrapinst,
                                    'required': True,
                                    'interpolate': True})[1], 
                  pfwdefs.UNITNAME:config.search(pfwdefs.UNITNAME, 
                                   {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                    'searchobj': wrapinst,
                                    'required': True,
                                    'interpolate': True})[1], 
                  pfwdefs.ATTNUM: config.search(pfwdefs.ATTNUM,
                                   {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                    'searchobj': wrapinst,
                                    'required': True,
                                    'interpolate': True})[1], 
                  pfwdefs.PF_BLKNUM: config.search(pfwdefs.PF_BLKNUM,
                                   {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                    'searchobj': wrapinst,
                                    'required': True,
                                    'interpolate': True})[1], 
                  pfwdefs.PF_JOBNUM: config.search(pfwdefs.PF_JOBNUM,
                                   {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                    'searchobj': wrapinst,
                                    'required': True,
                                    'interpolate': True})[1], 
                  pfwdefs.PF_WRAPNUM: config.search(pfwdefs.PF_WRAPNUM,
                                   {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                    'searchobj': wrapinst,
                                    'required': True,
                                    'interpolate': True})[1], 
                 }

    # start with specified
    if 'req_vals' in config[pfwdefs.SW_MODULESECT][modname]: 
        for rv in coremisc.fwsplit(config[pfwdefs.SW_MODULESECT][modname]['req_vals']):
            neededvals[rv] = True

    # go through all values in wcl
    neededvals.update(pfwutils.search_wcl_for_variables(wrapwcl))

    # add neededvals to wcl (values can also contain vars)
    done = False
    count = 0
    maxtries = 1000
    while not done and count < maxtries:
        done = True
        count += 1
        for nval in neededvals.keys():
            coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "nval = %s" % nval)
            if type(neededvals[nval]) is bool:
                if ':' in nval:
                    nval = nval.split(':')[0]

                if '.' not in nval:
                    (found, val) = config.search(nval, 
                                   {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                                    'searchobj': wrapinst, 
                                    'required': True, 
                                    'interpolate': False})
                    if not found:
                        print "WHYYYYYYYYY"
                else:
                    try:
                        val = pfwutils.get_wcl_value(nval, wrapwcl)
                    except KeyError as err:
                        print "----- Searching for value in wcl:", nval
                        print wclutils.write_wcl(wrapwcl)
                        raise(err)
                        

                coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "val = %s" % val)

                neededvals[nval] = val
                viter = [m.group(1) for m in re.finditer('(?i)\$\{([^}]+)\}', val)]
                for vstr in viter:
                    if ':' in vstr:
                        vstr = vstr.split(':')[0]
                    if vstr not in neededvals:
                        neededvals[vstr] = True
                        done = False
                    
    if count >= maxtries:
        raise Exception("Error: exceeded maxtries")


    # add needed values to wrapper wcl
    for key, val in neededvals.items():
        pfwutils.set_wcl_value(key, val, wrapwcl)
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def create_wrapper_inst(config, modname, loopvals):
    """ Create set of empty wrapper instances """
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    wrapperinst = {}
    (found, loopkeys) = config.search('wrapperloop', 
                   {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                    'required': False, 'interpolate': True})
    wrapperinst = {}
    if found:
        coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "loopkeys = %s" % loopkeys)
        loopkeys = coremisc.fwsplit(loopkeys.lower())
        loopkeys.sort()  # sort so can make same key easily

        for instvals in loopvals:
            coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "creating instance for %s" % str(instvals) )
            
            config.inc_wrapnum()
            winst = {pfwdefs.PF_WRAPNUM: config[pfwdefs.PF_WRAPNUM]}

            if len(instvals) != len(loopkeys):
                coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "Error: invalid number of values for instance")
                coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "\t%d loopkeys (%s)" % (len(loopkeys), loopkeys))
                coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "\t%d instvals (%s)" % (len(instvals), instvals))
                raise IndexError("Invalid number of values for instance")

            try:
                instkey = ""
                for k in range(0, len(loopkeys)):
                    winst[loopkeys[k]] = instvals[k] 
                    instkey += instvals[k] + '_'
            except:
                coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "Error: problem trying to create wrapper instance")
                coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "\tWas creating instance for %s" % str(instvals) )
                coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "\tloopkeys = %s" % loopkeys)
                raise

            wrapperinst[instkey] = winst
    else:
        config.inc_wrapnum()
        wrapperinst['noloop'] = {pfwdefs.PF_WRAPNUM: config[pfwdefs.PF_WRAPNUM]}

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "Number wrapper inst: %s" % len(wrapperinst))
    if len(wrapperinst) == 0:
        coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "Error: 0 wrapper inst")
        raise Exception("Error: 0 wrapper instances")
        
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return wrapperinst


#####################################################################
def read_master_lists(config, modname, modules_prev_in_list):
    """ Read master lists and files from files created earlier """
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    # create python list of files and lists for this module
    searchobj = config.combine_lists_files(modname)

    for (sname, sdict) in searchobj:
        # get filename for file containing dataset
        if 'qoutfile' in sdict:
            qoutfile = sdict['qoutfile']
            print "\t\t%s: reading master dataset from %s" % (sname, qoutfile)

            # read dataset file
            starttime = time.time()
            print "\t\t\tReading file - start ", starttime
            if qoutfile.endswith(".xml"):
                raise Exception("xml datasets not supported yet")
            elif qoutfile.endswith(".wcl"):
                with open(qoutfile, 'r') as wclfh:
                    master = wclutils.read_wcl(wclfh, filename=qoutfile)
                    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "master.keys() = " % master.keys())
            else:
                raise Exception("Unsupported dataset format in qoutfile for object %s in module %s (%s) " % (sname, modname, qoutfile))
            endtime = time.time()
            print "\t\t\tReading file - end ", endtime
            print "\t\t\tReading file took %s seconds" % (endtime - starttime)

            numlines = len(master['list'][pfwdefs.PF_LISTENTRY])
            print "\t\t\tNumber of lines in dataset %s: %s\n" % (sname, numlines)

            if numlines == 0:
                raise Exception("ERROR: 0 lines in dataset %s in module %s" % (sname, modname))

            sdict['master'] = master

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def create_fullnames(config, modname):
    """ add paths to filenames """    # what about compression extension

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    dataset = config.combine_lists_files(modname)
    moddict = config[pfwdefs.SW_MODULESECT][modname]

    for (sname, sdict) in dataset:
        if 'master' in sdict:
            master = sdict['master']
            numlines = len(master['list'][pfwdefs.PF_LISTENTRY])
            print "\t%s-%s: number of lines in master = %s" % (modname, sname, numlines)
            if numlines == 0:
                coremisc.fwdie("Error: 0 lines in master list", pfwdefs.PF_EXIT_FAILURE)

            if 'columns' in sdict:   # list
                colarr = coremisc.fwsplit(sdict['columns'])
                dictcurr = {}
                for c in colarr:
                    m = re.search("(\S+).fullname", c)
                    if m:
                        flabel = m.group(1)
                        if flabel in moddict[pfwdefs.SW_FILESECT]:
                            dictcurr[flabel] = copy.deepcopy(moddict[pfwdefs.SW_FILESECT][flabel])
                            dictcurr[flabel]['curr_module'] = modname
                        else:
                            print "list files = ", moddict[pfwdefs.SW_FILESECT].keys()
                            coremisc.fwdie("Error: Looking at list columns - could not find %s def in dataset" % flabel, pfwdefs.PF_EXIT_FAILURE)
                        
                for llabel,ldict in master['list'][pfwdefs.PF_LISTENTRY].items():
                    for flabel,fdict in ldict['file'].items():
                        if flabel in dictcurr:
                            path = config.get_filepath('runtime', None, {pfwdefs.PF_CURRVALS: dictcurr[flabel], 'searchobj': fdict})
                            fdict['fullname'] = "%s/%s" % (path, fdict['filename'])
                        elif len(dictcurr) == 1:
                            path = config.get_filepath('runtime', None, {pfwdefs.PF_CURRVALS: dictcurr.values()[0], 'searchobj': fdict})
                            fdict['fullname'] = "%s/%s" % (path, fdict['filename'])
                        else:
                            print "dictcurr: ", dictcurr.keys()
                            coremisc.fwdie("Error: Looking at lines - could not find %s def in dictcurr" % flabel, pfwdefs.PF_EXIT_FAILURE)
                            
                     
            else:  # file
                currvals = copy.deepcopy(sdict) 
                currvals['curr_module'] = modname

                for llabel,ldict in master['list'][pfwdefs.PF_LISTENTRY].items():
                    for flabel,fdict in ldict['file'].items():
                        path = config.get_filepath('runtime', None, {pfwdefs.PF_CURRVALS: currvals, 'searchobj': fdict})
                        fdict['fullname'] = "%s/%s" % (path, fdict['filename'])
        else:
            print "\t%s-%s: no masterlist...skipping" % (modname, sname)

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def create_sublists(config, modname):
    """ break master lists into sublists based upon match or divide_by """
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    dataset = config.combine_lists_files(modname)

    for (sname, sdict) in dataset:
        if 'master' in sdict:
            master = sdict['master']
            numlines = len(master['list'][pfwdefs.PF_LISTENTRY])
            print "\t%s-%s: number of lines in master = %s" % (modname, sname, numlines)
            if numlines == 0:
                coremisc.fwdie("Error: 0 lines in master list", pfwdefs.PF_EXIT_FAILURE)

            sublists = {}
            keys = get_match_keys(sdict)

            if len(keys) > 0: 
                sdict['keyvals'] = {} 
                print "\t%s-%s: dividing by %s" % (modname, sname, keys)
                for linenick, linedict in master['list'][pfwdefs.PF_LISTENTRY].items():
                    index = ""
                    listkeys = []
                    for key in keys:
                        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "key = %s" % key)
                        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "linedict = %s" % linedict)
                        val = get_value_from_line(linedict, key, None, 1)
                        index += val + '_'
                        listkeys.append(val)
                    sdict['keyvals'][index] = listkeys
                    if index not in sublists:
                        sublists[index] = {'list': {pfwdefs.PF_LISTENTRY: {}}}
                    sublists[index]['list'][pfwdefs.PF_LISTENTRY][linenick] = linedict
            else:
                sublists['onlyone'] = master

            del sdict['master']
            sdict['sublists'] = sublists
            print "\t%s-%s: number of sublists = %s" % (modname, sname, len(sublists))
            coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "sublist.keys()=%s" % sublists.keys())
            coremisc.fwdebug(4, "PFWBLOCK_DEBUG", "sublists[sublists.keys()[0]]=%s" % sublists[sublists.keys()[0]])
            print ""
            print ""
        else:
            print "\t%s-%s: no masterlist...skipping" % (modname, sname)

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def get_wrap_iter_obj_key(config, moddict):
    iter_obj_key = None
    if 'loopobj' in moddict:
        iter_obj_key = moddict['loopobj'].lower()
    else:
        coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "Could not find loopobj. moddict keys = %s" % moddict.keys())
        coremisc.fwdebug(6, "PFWBLOCK_DEBUG", "Could not find loopobj in modict %s" % moddict)
    return iter_obj_key


#######################################################################
def get_wrapper_loopvals(config, modname):
    """ get the values for the wrapper loop keys """
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    loopvals = []

    moddict = config[pfwdefs.SW_MODULESECT][modname]
    (found, loopkeys) = config.search('wrapperloop', 
                   {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                    'required': False, 'interpolate': True})
    if found:
        coremisc.fwdebug(0,"PFWBLOCK_DEBUG", "\tloopkeys = %s" % loopkeys)
        loopkeys = coremisc.fwsplit(loopkeys.lower())
        loopkeys.sort()  # sort so can make same key easily


        ## determine which list/file would determine loop values
        iter_obj_key = get_wrap_iter_obj_key(config, moddict)
        coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "iter_obj_key=%s" % iter_obj_key)

        ## get wrapper loop values
        if iter_obj_key is not None:
            loopdict = pfwutils.get_wcl_value(iter_obj_key, moddict) 
            ## check if loopobj has info from query
            if 'keyvals' in loopdict:
                loopvals = loopdict['keyvals'].values()
            else:
                print "Warning: Couldn't find keyvals for loopobj", moddict['loopobj']     

        if len(loopvals) == 0:
            print "\tDefaulting to wcl values"
            loopvals = []
            for key in loopkeys:
                coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "key=%s" % key)
                (found, val) = config.search(key, 
                            {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                            'required': False, 
                            'interpolate': True})
                coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "found=%s" % found)
                if found:
                    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "val=%s" % val)
                    val = coremisc.fwsplit(val)
                    loopvals.append(val)
            loopvals = itertools.product(*loopvals)
        
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return loopvals


#############################################################
def get_value_from_line(line, key, nickname=None, numvals=None):
    """ Return value from a line in master list """
    coremisc.fwdebug(1, "PFWBLOCK_DEBUG", "BEG: key = %s, nickname = %s, numvals = %s" % (key, nickname, numvals))
    # returns None if 0 matches
    #         scalar value if 1 match
    #         array if > 1 match

    # since values could be repeated across files in line, 
    # create hash of values to get unique values
    valhash = {}

    if '.' in key:
        coremisc.fwdebug(1, "PFWBLOCK_DEBUG", "Found nickname")
        (nickname, key) = key.split('.')

    # is value defined at line level?
    if key in line:
         valhash[line[key]] = True

    # check files
    if 'file' in line:
        if nickname is not None:
            if nickname in line['file'] and key in line['file'][nickname]:
                valhash[line['file'][nickname][key]] = True
        else:
            for fnickname, fdict in line['file'].items():
                if key in fdict:
                    valhash[fdict[key]] = True

    valarr = valhash.keys()

    if numvals is not None and len(valarr) != numvals:
        print "Error: in get_value_from_line:" 
        print "\tnumber found (%s) doesn't match requested (%s)\n" % (len(valarr), numvals)
        if nickname is not None:
            print "\tnickname =", nickname

        print "\tvalue to find:", key
        print "\tline:", wclutils.write_wcl(line)
        print "\tvalarr:", valarr
        coremisc.fwdie("Error: number found (%s) doesn't match requested (%s)" % (len(valarr), numvals), pfwdefs.PF_EXIT_FAILURE)

    if len(valarr) == 0:
        retval = None
    elif numvals == 1 or len(valarr) == 1:
        retval = valarr[0].strip()
    else:
        retval = valarr.strip()

    coremisc.fwdebug(1, "PFWBLOCK_DEBUG", "END\n\n")
    return retval


#######################################################################
# Assumes currvals includes specific values (e.g., band, ccd)
def create_single_wrapper_wcl(config, modname, wrapinst):
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s %s" % (modname, wrapinst[pfwdefs.PF_WRAPNUM]))

    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "\twrapinst=%s" % wrapinst)

    currvals = {'curr_module': modname, pfwdefs.PF_WRAPNUM: wrapinst[pfwdefs.PF_WRAPNUM]}


    wrapperwcl = {'modname': modname}



    # file is optional
    if pfwdefs.IW_FILESECT in wrapinst:
        wrapperwcl[pfwdefs.IW_FILESECT] = copy.deepcopy(wrapinst[pfwdefs.IW_FILESECT])
        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "\tfile=%s" % wrapperwcl[pfwdefs.IW_FILESECT])
        for (sectname, sectdict) in wrapperwcl[pfwdefs.IW_FILESECT].items():
            sectdict['sectname'] = sectname

    # list is optional
    if pfwdefs.IW_LISTSECT in wrapinst:
        wrapperwcl[pfwdefs.IW_LISTSECT] = copy.deepcopy(wrapinst[pfwdefs.IW_LISTSECT])
        coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "\tlist=%s" % wrapperwcl[pfwdefs.IW_LISTSECT])


    # do we want exec_list variable?
    coremisc.fwdebug(3, "PFWBLOCK_DEBUG", "\tpfwdefs.SW_EXECPREFIX=%s" % pfwdefs.SW_EXECPREFIX)
    numexec = 0
    modname = currvals['curr_module']
    moddict = config[pfwdefs.SW_MODULESECT][modname]
    execs = pfwutils.get_exec_sections(moddict, pfwdefs.SW_EXECPREFIX)
    for execkey in execs:
        coremisc.fwdebug(3, 'PFWBLOCK_DEBUG', "Working on exec section (%s)"% execkey)
        numexec += 1
        iwkey = execkey.replace(pfwdefs.SW_EXECPREFIX, pfwdefs.IW_EXECPREFIX)
        wrapperwcl[iwkey] = {}
        execsect = moddict[execkey]
        coremisc.fwdebug(3, 'PFWBLOCK_DEBUG', "\t\t(%s)" % (execsect))
        for key, val in execsect.items():
            coremisc.fwdebug(5, 'PFWBLOCK_DEBUG', "\t\t%s (%s)" % (key, val))
            if key == pfwdefs.SW_INPUTS:
                iwexkey = pfwdefs.IW_INPUTS
            elif key == pfwdefs.SW_OUTPUTS:
                iwexkey = pfwdefs.IW_OUTPUTS
            elif key == pfwdefs.SW_ANCESTRY:
                iwexkey = pfwdefs.IW_ANCESTRY
            else:
                iwexkey = key

            if key != 'cmdline':
                wrapperwcl[iwkey][iwexkey] = config.interpolate(val, {pfwdefs.PF_CURRVALS: currvals, 'searchobj': val,
                                                            'required': True, 'interpolate': True})
            else:
                wrapperwcl[iwkey]['cmdline'] = copy.deepcopy(val)
        if 'execnum' not in wrapperwcl[execkey]:
            result = re.match('%s(\d+)' % pfwdefs.IW_EXECPREFIX, execkey)
            if not result:
                coremisc.fwdie('Error:  Could not determine execnum from exec label %s' % execkey, pfwdefs.PF_EXIT_FAILURE)
            wrapperwcl[execkey]['execnum'] = result.group(1)

    if pfwdefs.SW_WRAPSECT in config[pfwdefs.SW_MODULESECT][modname]:
        coremisc.fwdebug(3, 'PFWBLOCK_DEBUG', "Copying wrapper section (%s)"% pfwdefs.SW_WRAPSECT)
        wrapperwcl[pfwdefs.IW_WRAPSECT] = copy.deepcopy(config[pfwdefs.SW_MODULESECT][modname][pfwdefs.SW_WRAPSECT])

    if pfwdefs.IW_WRAPSECT not in wrapperwcl:
        coremisc.fwdebug(3, 'PFWBLOCK_DEBUG', "%s (%s): Initializing wrapper section (%s)"% (modname, wrapinst[pfwdefs.PF_WRAPNUM], pfwdefs.IW_WRAPSECT))
        wrapperwcl[pfwdefs.IW_WRAPSECT] = {}
    wrapperwcl[pfwdefs.IW_WRAPSECT]['pipeline'] = config['pipeprod']
    wrapperwcl[pfwdefs.IW_WRAPSECT]['pipever'] = config['pipever']

    wrapperwcl[pfwdefs.IW_WRAPSECT]['wrappername'] = wrapinst['wrappername']
    wrapperwcl[pfwdefs.IW_WRAPSECT]['outputwcl'] = wrapinst['outputwcl']
    wrapperwcl[pfwdefs.IW_WRAPSECT]['tmpfile_prefix'] =  config.search('tmpfile_prefix',
                                {pfwdefs.PF_CURRVALS: currvals,
                                 'required': True, 'interpolate': True})[1]
    wrapperwcl['log'] = wrapinst['log'] 
    wrapperwcl['log_archive_path'] = wrapinst['log_archive_path']


    if numexec == 0:
        wclutils.write_wcl(config[pfwdefs.SW_MODULESECT][modname])
        raise Exception("Error:  Could not find an exec section for module %s" % modname)
        

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")

    return wrapperwcl


# translate sw terms to iw terms in values if needed
def translate_sw_iw(config, wrapperwcl, modname, winst):
    coremisc.fwdebug(1, "PFWBLOCK_DEBUG", "BEG %s" % modname)


    if ( (pfwdefs.SW_FILESECT == pfwdefs.IW_FILESECT) and 
         (pfwdefs.SW_LISTSECT == pfwdefs.IW_LISTSECT) ):
        print "Skipping translation SW to IW"
    else:
        translation = [(pfwdefs.SW_FILESECT, pfwdefs.IW_FILESECT),
                       (pfwdefs.SW_LISTSECT, pfwdefs.IW_LISTSECT)]
        wrappervars = {}
        wcltodo = [wrapperwcl]
        while len(wcltodo) > 0:
            wcl = wcltodo.pop()
            for key,val in wcl.items():
                coremisc.fwdebug(4, 'PFWBLOCK_DEBUG', "key = %s" % (key))
                if type(val) is dict or type(val) is OrderedDict:
                    wcltodo.append(val)
                elif type(val) is str:
                    coremisc.fwdebug(4, 'PFWBLOCK_DEBUG', "val = %s, %s" % (val, type(val)))
                    for (sw, iw) in translation:
                        val = val.replace(sw+'.', iw+'.')
                    coremisc.fwdebug(4, 'PFWBLOCK_DEBUG', "final value = %s" % (val))
                    wcl[key] = val

    #print "new wcl = ", wclutils.write_wcl(wrapperwcl, sys.stdout, True, 4)
    coremisc.fwdebug(1, "PFWBLOCK_DEBUG", "END\n\n")
               


#######################################################################
def create_module_wrapper_wcl(config, modname, wrapinst):
    """ Create wcl for wrapper instances for a module """
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    tasks = []

    if modname not in config[pfwdefs.SW_MODULESECT]:
        raise Exception("Error: Could not find module description for module %s\n" % (modname))




    for inst in wrapinst.values():
        wrapperwcl = create_single_wrapper_wcl(config, modname, inst)
        
        translate_sw_iw(config, wrapperwcl, modname, inst)
        add_needed_values(config, modname, inst, wrapperwcl)
        write_wrapper_wcl(config, inst['inputwcl'], wrapperwcl) 

        (exists, val) = config.search(pfwdefs.SW_WRAPPER_DEBUG, {pfwdefs.PF_CURRVALS: {'curr_module': modname}})
        if exists:
            inst['wrapdebug'] = val
        else:
            inst['wrapdebug'] = 0

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def divide_into_jobs(config, modname, wrapinst, joblist):
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    if pfwdefs.SW_DIVIDE_JOBS_BY not in config and len(joblist) > 1:
        coremisc.fwdie("Error: no %s in config, but already > 1 job" % pfwdefs.SW_DIVIDE_JOBS_BY)

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "number of wrapinst = %s" % len(wrapinst))

    for inst in wrapinst.values():
        key = '_nokey'
        if pfwdefs.SW_DIVIDE_JOBS_BY in config:
            key = ""
            for divb in coremisc.fwsplit(config[pfwdefs.SW_DIVIDE_JOBS_BY], ','):
                key += "_"+config.get(divb, None, {pfwdefs.PF_CURRVALS: {'curr_module':modname}, 'searchobj': inst, 'interpolate': True, 'required':True})
                
        if key not in joblist:
            joblist[key] = {'tasks':[], 'inlist':[], 'wrapinputs':{}}
        joblist[key]['tasks'].append([inst[pfwdefs.PF_WRAPNUM], inst['wrappername'], inst['inputwcl'], inst['wrapdebug'], inst['log']])
        joblist[key]['inlist'].append(inst['inputwcl'])
        if inst['wrapinputs'] is not None and len(inst['wrapinputs']) > 0:
            joblist[key]['wrapinputs'][inst[pfwdefs.PF_WRAPNUM]] = inst['wrapinputs']
        if pfwdefs.IW_LISTSECT in inst:
            for linfo in inst[pfwdefs.IW_LISTSECT].values():
                joblist[key]['inlist'].append(linfo['fullname'])

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "number of job lists = %s " % len(joblist.keys()))
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "\tkeys = %s " % ','.join(joblist.keys()))
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n")
            

def write_runjob_script(config):
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")

    jobdir = config.get_filepath('runtime', 'jobdir', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM:"$padjnum"}})
    print "The jobdir =", jobdir

    usedb = coremisc.convertBool(config[pfwdefs.PF_USE_DB_OUT])
    scriptfile = config.get_filename('runjob') 

    #      Since wcl's variable syntax matches shell variable syntax and 
    #      underscores are used to separate name parts, have to use place 
    #      holder for jobnum and replace later with shell variable
    #      Otherwise, get_filename fails to substitute for padjnum
    envfile = config.get_filename('envfile', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM:"9999"}})
    envfile = envfile.replace("j9999", "j${padjnum}")

    scriptstr = """#!/bin/sh
echo "PFW: job_shell_script cmd: $0 $@";
if [ $# -ne 6 ]; then
    echo "Usage: $0 <jobnum> <input tar> <job wcl> <tasklist> <env file> <output tar>";
    echo "PFW: job_shell_script exit_status: 1" 
    exit 1;
fi
jobnum=$1
padjnum=`/usr/bin/printf %04d $jobnum`
intar=$2
jobwcl=$3
tasklist=$4
envfile=$5
outputtar=$6
initdir=`/bin/pwd`
"""

    # setup job environment
    scriptstr += """
export SHELL=/bin/bash    # needed for setup to work in Condor environment
shd1=`/bin/date "+%%s"`
echo "PFW: job_shell_script starttime: $shd1" 
echo -n "PFW: job_shell_script exechost: "
/bin/hostname
echo ""

BATCHID=""
if /usr/bin/test -n "$SUBMIT_CONDORID"; then
    echo "PFW: condorid $SUBMIT_CONDORID"
    BATCHID=$SUBMIT_CONDORID
fi

### Output batch jobid for record keeping
### specific to batch scheduler
if /usr/bin/test -n "$PBS_JOBID"; then
   BATCHID=`echo $PBS_JOBID | /bin/cut -d'.' -f1`
   NP=`/bin/awk 'END {print NR}' $PBS_NODEFILE`
fi
if /usr/bin/test -n "$LSB_JOBID"; then
   BATCHID=$LSB_JOBID
fi
if /usr/bin/test -n "$LOADL_STEP_ID"; then
   BATCHID=`echo $LOADL_STEP_ID | /bin/awk -F "." '{ print $(NF-1) "." $(NF) }'`
fi
if /usr/bin/test -n "$CONDOR_ID"; then
   BATCHID=$CONDOR_ID
fi
if /usr/bin/test -n "$BATCHID"; then
    echo "PFW: batchid $BATCHID"
fi

echo ""
echo ""
echo "Initial condor job directory = " $initdir
echo "Files copied over by condor:"
ls -l
echo ""
echo "Creating empty job output files to guarantee condor job nice exit"
touch $envfile
tar -cvf $outputtar --files-from /dev/null

echo "Sourcing script to set up EUPS (%(eups)s)"
source %(eups)s 

echo "Using eups to setup up %(pipe)s %(ver)s"
d1=`/bin/date "+%%s"` 
echo "PFW: eups_setup starttime: $d1" 
setup --nolock %(pipe)s %(ver)s
mystat=$?
d2=`/bin/date "+%%s"` 
echo "PFW: eups_setup endtime: $d2" 
if [ $mystat != 0 ]; then
    echo "Error: eups setup had non-zero exit code ($mystat)"
    shd2=`/bin/date "+%%s"`
    echo "PFW: job_shell_script endtime: $shd2" 
    echo "PFW: job_shell_script exit_status: $mystat" 
    exit $mystat    # note exit code not passed back through grid universe jobs
fi
""" % ({'eups': config['setupeups'], 
        'pipe':config['pipeprod'],
        'ver':config['pipever']})

    if not usedb:
        scriptstr += 'echo "DESDMTIME: eups_setup $((d2-d1)) secs"'

    # add any job environment from submit wcl
    scriptstr += 'echo ""\n'
    if pfwdefs.SW_JOB_ENVIRONMENT in config:
        for name,value in config[pfwdefs.SW_JOB_ENVIRONMENT].items():
            scriptstr += 'export %s="%s"\n' % (name.upper(), value)
    scriptstr += 'echo ""\n'


    # print start of job information 

    scriptstr +="""
echo "Saving environment after setting up meta package to $envfile"
env | sort > $envfile
pwd
ls -l $envfile
""" 
   
    if pfwdefs.SW_JOB_BASE_DIR in config and config[pfwdefs.SW_JOB_BASE_DIR] is not None:
        full_job_dir = config[pfwdefs.SW_JOB_BASE_DIR] + '/' + jobdir
        print "full_job_dir =", full_job_dir
        scriptstr += """
echo ""
echo "Making target job's directory (%(full_job_dir)s)"
if [ ! -e %(full_job_dir)s ]; then
    mkdir -p %(full_job_dir)s
fi
cd %(full_job_dir)s
        """ % ({'full_job_dir': full_job_dir})
    else:
        print "%s wasn't specified.   Running job in condor job directory" % pfwdefs.SW_JOB_BASE_DIR

    # untar file containing input wcl files
    scriptstr += """
echo ""
echo "Untaring input tar: $intar"
d1=`/bin/date "+%s"` 
echo "PFW: untaring_input_tar starttime: $d1"
tar -xzf $initdir/$intar
d2=`/bin/date "+%s"` 
echo "PFW: untaring_input_tar endtime: $d2"
"""
    if not usedb:
        scriptstr += 'echo "DESDMTIME: untar_input_tar $((d2-d1)) secs"'

    # copy files so can test by hand after job
    # save initial directory to job wcl file
    scriptstr += """
echo "Copying job wcl and task list to job working directory"
d1=`/bin/date "+%s"`
echo "PFW: copy_job_setup starttime: $d1"
cp $initdir/$jobwcl $jobwcl
cp $initdir/$tasklist $tasklist
d2=`/bin/date "+%s"`
echo "PFW: copy_job_setup endtime: $d2"
echo "condor_job_init_dir = " $initdir >> $jobwcl
"""
    if not usedb:
        scriptstr += 'echo "DESDMTIME: copy_jobwcl_tasklist $((d2-d1)) secs"'

    # call the job workflow program
    scriptstr += """
echo ""
echo "Calling pfwrunjob.py"
echo "cmd> ${PROCESSINGFW_DIR}/libexec/pfwrunjob.py --config $jobwcl $tasklist"
d1=`/bin/date "+%s"`
echo "PFW: pfwrunjob starttime: $d1" 
${PROCESSINGFW_DIR}/libexec/pfwrunjob.py --config $jobwcl $tasklist
rjstat=$?
d2=`/bin/date "+%s"`
echo "PFW: pfwrunjob endtime: $d2" 
echo ""
echo ""
shd2=`/bin/date "+%s"`
echo "PFW: job_shell_script endtime: $shd2" 
echo "PFW: job_shell_script exit_status: $rjstat" 
"""
    
    if not usedb:
        scriptstr += """
echo "DESDMTIME: pfwrunjob.py $((d2-d1)) secs"
echo "DESDMTIME: job_shell_script $((shd2-shd1)) secs"
"""

    scriptstr += "exit $rjstat"

    # write shell script to file
    with open(scriptfile, 'w') as scriptfh:
        scriptfh.write(scriptstr)

    os.chmod(scriptfile, stat.S_IRWXU | stat.S_IRWXG)

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return scriptfile



#######################################################################
def create_jobmngr_dag(config, dagfile, scriptfile, joblist):
    """ Write job manager DAG file """

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    config['numjobs'] = len(joblist)
    condorfile = create_runjob_condorfile(config, scriptfile)

    pfwdir = config['processingfw_dir']
    blockname = config['curr_block']
    blkdir = config['block_dir']


    with open("%s/%s" % (blkdir, dagfile), 'w') as dagfh:
        for jobkey,jobdict in joblist.items(): 
            jobnum = jobdict['jobnum']
            tjpad = "%04d" % (int(jobnum))

            dagfh.write('JOB %s %s\n' % (tjpad, condorfile))
            dagfh.write('VARS %s jobnum="%s"\n' % (tjpad, tjpad))
            dagfh.write('VARS %s exec="../%s"\n' % (tjpad, scriptfile))
            dagfh.write('VARS %s args="%s %s %s %s %s %s"\n' % (tjpad, jobnum, jobdict['inputwcltar'], jobdict['jobwclfile'], jobdict['tasksfile'], jobdict['envfile'], jobdict['outputwcltar']))
            dagfh.write('VARS %s transinput="%s,%s,%s"\n' % (tjpad, jobdict['inputwcltar'], jobdict['jobwclfile'], jobdict['tasksfile']))
            dagfh.write('VARS %s transoutput="%s,%s"\n' % (tjpad, jobdict['outputwcltar'], jobdict['envfile']))
            dagfh.write('SCRIPT pre %s %s/libexec/jobpre.py ../uberctrl/config.des $JOB\n' % (tjpad, pfwdir)) 
            dagfh.write('SCRIPT post %s %s/libexec/jobpost.py ../uberctrl/config.des %s $JOB %s %s $RETURN\n' % (tjpad, pfwdir, blockname, jobdict['inputwcltar'], jobdict['outputwcltar'])) 

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def tar_inputfiles(config, jobnum, inlist):
    """ Tar the input wcl files for a single job """
    inputtar = config.get_filename('inputwcltar', {pfwdefs.PF_CURRVALS:{'jobnum': jobnum}})
    tjpad = "%04d" % (int(jobnum))
    coremisc.coremakedirs(tjpad)
    
    pfwutils.tar_list("%s/%s" % (tjpad, inputtar), inlist)
    return inputtar


#######################################################################
def create_runjob_condorfile(config, scriptfile):
    """ Write runjob condor description file for target job """
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")

    blockbase = config.get_filename('block', {pfwdefs.PF_CURRVALS: {'flabel': 'runjob', 'fsuffix':''}})
    initialdir = "%s/%s" % (config['block_dir'], '$(jobnum)')

    condorfile = '%s/%scondor' % (config['block_dir'], blockbase)
    
    jobbase = config.get_filename('job', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM:'$(jobnum)', 'flabel': 'runjob', 'fsuffix':''}})
    jobattribs = { 
                'executable':'%s/%s' % (config['block_dir'], scriptfile), 
                'arguments':'$(args)',
                'initialdir':initialdir,
                'when_to_transfer_output': 'ON_EXIT_OR_EVICT',
                'transfer_input_files': '$(transinput)', 
                'transfer_executable': 'True',
                'notification': 'Never',
                'output':'%sout' % jobbase,
                'error':'%serr' % jobbase,
                'log': '%slog' % blockbase,
                'periodic_release': '((CurrentTime - EnteredCurrentStatus) > 1800) && (HoldReason =!= "via condor_hold (by user %s)")' % config['operator'],
                'periodic_remove' : '((JobStatus == 1) && (JobRunCount =!= Undefined))'
                 }


    userattribs = config.get_condor_attributes('$(jobnum)')
    targetinfo = config.get_grid_info()
    print "targetinfo=",targetinfo
    if 'gridtype' not in targetinfo:
        coremisc.fwdie("Error:  Missing gridtype", pfwdefs.PF_EXIT_FAILURE)
    else:
        targetinfo['gridtype'] = targetinfo['gridtype'].lower()
        print 'GRIDTYPE =', targetinfo['gridtype']

    reqs = []
    if targetinfo['gridtype'] == 'condor':
        jobattribs['universe'] = 'vanilla'

        if 'concurrency_limits' in config:
            jobattribs['concurrency_limits'] = config['concurrency_limits']

        if 'batchtype' not in targetinfo:
            coremisc.fwdie("Error: Missing batchtype", pfwdefs.PF_EXIT_FAILURE)
        else:
            targetinfo['batchtype'] = targetinfo['batchtype'].lower()

        if targetinfo['batchtype'] == 'glidein':
            if 'uiddomain' not in config:
                coremisc.fwdie("Error: Cannot determine uiddomain for matching to a glidein", pfwdefs.PF_EXIT_FAILURE)
            reqs.append('(UidDomain == "%s")' % config['uiddomain'])
            if 'glidein_name' in config and config['glidein_name'].lower() != 'none':
                reqs.append('(GLIDEIN_NAME == "%s")' % config.interpolate(config['glidein_name']))

            reqs.append('(FileSystemDomain != "")')
            reqs.append('(Arch != "")')
            reqs.append('(OpSys != "")')
            reqs.append('(Disk != -1)')
            reqs.append('(Memory != -1)')
    
            if 'glidein_use_wall' in config and coremisc.convertBool(config['glidein_use_wall']):
                reqs.append("(TimeToLive > \$(wall)*60)")   # wall is in mins, TimeToLive is in secs

        elif targetinfo['batchtype'] == 'local':
            jobattribs['universe'] = 'vanilla'
            if 'loginhost' in config:
                machine = config['loginhost']
            elif 'gridhost' in config:
                machine = config['gridhost']
            else:
                coremisc.fwdie("Error:  Cannot determine machine name (missing loginhost and gridhost)", pfwdefs.PF_EXIT_FAILURE)

            reqs.append('(machine == "%s")' % machine)
        elif 'dynslots' in targetinfo['batchtype'].lower():
            if 'request_memory' in config:
                jobattribs['request_memory'] = config['request_memory'] 
            if 'request_cpus' in config:
                jobattribs['request_cpus'] = config['request_cpus'] 
    else:
        print "Grid job"
        jobattribs['universe'] = 'grid'
        jobattribs['grid_resource'] = pfwcondor.create_resource(targetinfo)
        jobattribs['stream_output'] = 'False'
        jobattribs['stream_error'] = 'False'
        jobattribs['transfer_output_files'] = '$(transoutput)'
        globus_rsl = pfwcondor.create_rsl(targetinfo)
        if len(globus_rsl) > 0:
            jobattribs['globus_rsl'] = globus_rsl
        print "jobattribs=", jobattribs

    if len(reqs) > 0:
        jobattribs['requirements'] = ' && '.join(reqs)
    print "jobattribs=", jobattribs
    pfwcondor.write_condor_descfile('runjob', condorfile, jobattribs, userattribs)

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return condorfile



#######################################################################
def stage_inputs(config, inputfiles):
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "number of input files needed at target = %s" % len(inputfiles))
    coremisc.fwdebug(6, "PFWBLOCK_DEBUG", "input files %s" % inputfiles)

    if (pfwdefs.USE_HOME_ARCHIVE_INPUT in config and 
        (config[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() == pfwdefs.TARGET_ARCHIVE.lower() or
        config[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() == 'all')):

        coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "home_archive = %s" % config[pfwdefs.HOME_ARCHIVE])
        coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "target_archive = %s" % config[pfwdefs.TARGET_ARCHIVE])
        sys.stdout.flush()
        sem = None
        if wcl['use_db']:
            sem = dbsem.DBSemaphore('filetrans')
            print "Semaphore info:\n", sem
        archive_transfer_utils.archive_copy(config['archive'][config[pfwdefs.HOME_ARCHIVE]], 
                                            config['archive'][config[pfwdefs.TARGET_ARCHIVE]],
                                            config['archive_transfer'],
                                            inputfiles, config)
        if sem is not None:
            del sem

    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def write_output_list(config, outputfiles):
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "output files %s" % outputfiles)

    if 'block_outputlist' not in config:
        coremisc.fwdie("Error:  Could not find block_outputlist in config.   Internal Error.", pfwdefs.PF_EXIT_FAILURE)

    with open(config['block_outputlist'], 'w') as fh:
        for f in outputfiles:
            fh.write("%s\n" % coremisc.parse_fullname(f, coremisc.CU_PARSE_FILENAME))
    
    coremisc.fwdebug(0, "PFWBLOCK_DEBUG", "END")
    
