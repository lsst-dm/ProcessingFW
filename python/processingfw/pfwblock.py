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
from processingfw.pfwdefs import *
from filemgmt.filemgmt_defs import *
from coreutils.miscutils import *
import filemgmt.archive_transfer_utils as archive_transfer_utils
import intgutils.wclutils as wclutils
import intgutils.metautils as metautils
from intgutils.metadefs import *
import processingfw.pfwutils as pfwutils
import processingfw.pfwcondor as pfwcondor
from processingfw.pfwwrappers import write_wrapper_wcl

#######################################################################
def create_simple_list(config, lname, ldict, currvals):
    """ Create simple filename list file based upon patterns """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    listname = config.search('listname', 
                            {PF_CURRVALS: currvals, 
                             'searchobj': ldict, 
                             'required': True, 
                             'interpolate': True})[1]

    filename = config.get_filename(None,
                            {PF_CURRVALS: currvals, 
                             'searchobj': ldict, 
                             'required': True, 
                             'interpolate': True})[1]

    if type(filename) is list:
        listcontents = '\n'.join(filename)
    else:
        listcontents = filename
 
    listdir = os.path.dirname(listname)
    if len(listdir) > 0 and not os.path.exists(listdir):
        os.mkdir(listdir)

    with open(listname, 'w', 0) as listfh:
        listfh.write(listcontents+"\n")
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def get_match_keys(sdict):
    mkeys = []

    fwdebug(3, "PFWBLOCK_DEBUG", "keys in sdict: %s " % sdict.keys())
    if 'loopkey' in sdict:
        mkeys = fwsplit(sdict['loopkey'].lower())
        mkeys.sort()
    elif 'match' in sdict:
        mkeys = fwsplit(sdict['match'].lower())
        mkeys.sort()
    elif 'divide_by' in sdict:
        mkeys = fwsplit(sdict['divide_by'].lower())
        mkeys.sort()
    
    return mkeys


#######################################################################
def find_sublist(objDef, objInst):

    if len(objDef['sublists'].keys()) > 1:
        fwdebug(3, "PFWBLOCK_DEBUG", "sublist keys: %s" % (objDef['sublists'].keys()))
        matchkeys = get_match_keys(objDef)
        fwdebug(3, "PFWBLOCK_DEBUG", "matchkeys: %s" % (matchkeys))
        index = ""
        for mkey in matchkeys:
            if mkey not in objInst:
                fwdie("Error: Cannot find match key %s in inst %s" % (mkey, objInst), PF_EXIT_FAILURE)
            index += objInst[mkey] + '_'
        fwdebug(3, "PFWBLOCK_DEBUG", "sublist index = "+index)
        if index not in objDef['sublists']:
            fwdie("Error: Cannot find sublist matching "+index, PF_EXIT_FAILURE)
        sublist = objDef['sublists'][index]
    else:
        sublist = objDef['sublists'].values()[0]

    return sublist

#######################################################################
def which_are_inputs(config, modname):
    """ Return dict of files/lists that are inputs for given module """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    inputs = {SW_FILESECT: [], SW_LISTSECT: []}
    outfiles = {}
    
    # For wrappers with more than 1 exec section, the inputs of one exec can be the inputs of a 2nd exec
    #      the framework should not attempt to stage these intermediate files 
    execs = pfwutils.get_exec_sections(config[SW_MODULESECT][modname], SW_EXECPREFIX)
    for ekey, einfo in sorted(execs.items()):
        if SW_OUTPUTS in einfo:
            for outfile in fwsplit(einfo[OW_OUTPUTS]):
                outfiles[outfile] = True
             
        if SW_INPUTS in einfo: 
            inarr = fwsplit(einfo[SW_INPUTS].lower())
            for inname in inarr:
                if inname not in outfiles:
                    parts = fwsplit(inname, '.') 
                    inputs[parts[0]].append('.'.join(parts[1:]))

    fwdebug(0, "PFWBLOCK_DEBUG", inputs)
    fwdebug(0, "PFWBLOCK_DEBUG", "END")
    return inputs
            

#######################################################################
def which_are_outputs(config, modname):
    """ Return dict of files that are outputs for given module """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    outfiles = {}
    
    execs = pfwutils.get_exec_sections(config[SW_MODULESECT][modname], SW_EXECPREFIX)
    for ekey, einfo in sorted(execs.items()):
        if SW_OUTPUTS in einfo:
            for outfile in fwsplit(einfo[OW_OUTPUTS]):
                parts = fwsplit(outfile, '.')
                outfiles['.'.join(parts[1:])] = True

    fwdebug(0, "PFWBLOCK_DEBUG", outfiles.keys())
    fwdebug(0, "PFWBLOCK_DEBUG", "END")
    return outfiles.keys()




    
#######################################################################
def assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals, winst, fname, finfo, is_iter_obj=False):
    fwdebug(3, "PFWBLOCK_DEBUG", "BEG: Working on file %s" % fname)
    fwdebug(3, "PFWBLOCK_DEBUG", "theinputs: %s" % theinputs)
    fwdebug(3, "PFWBLOCK_DEBUG", "outputs: %s" % theoutputs)

    if 'listonly' in finfo and convertBool(finfo['listonly']):
        fwdebug(3, "PFWBLOCK_DEBUG", "Skipping %s due to listonly key" % fname)
        return

    if IW_FILESECT not in winst:
        winst[IW_FILESECT] = {}

    winst[IW_FILESECT][fname] = {}
    if 'sublists' in finfo:  # files came from query
        sublist = find_sublist(finfo, winst)
        if len(sublist['list'][PF_LISTENTRY]) > 1:
            fwdie("Error: more than 1 line to choose from for file (%s)" % sublist['list'][PF_LISTENTRY], PF_EXIT_FAILURE)
        line = sublist['list'][PF_LISTENTRY].values()[0]
        if 'file' not in line:
            fwdie("Error: 0 file in line" + str(line), PW_EXIT_FAILURE)
            
        if len(line['file']) > 1:
            raise Exception("more than 1 file to choose from for file" + line['file'])
        finfo = line['file'].values()[0]
        fwdebug(6, "PFWBLOCK_DEBUG", "finfo = %s" % finfo)

        fullname = finfo['fullname']
        winst[IW_FILESECT][fname]['fullname'] = fullname

        # save input and output filenames (with job scratch path)
        # In order to preserve capitalization, put on right side of =, using dummy count for left side
        if fname in theinputs[SW_FILESECT]:
            winst['wrapinputs'][len(winst['wrapinputs'])+1] = fullname
        elif fname in theoutputs:
            winst['wrapoutputs'][len(winst['wrapoutputs'])+1] = fullname

        fwdebug(6, "PFWBLOCK_DEBUG", "Assigned filename for fname %s (%s)" % (fname, finfo['filename']))
    elif 'fullname' in moddict[SW_FILESECT][fname]:
        winst[IW_FILESECT][fname]['fullname'] = moddict[SW_FILESECT][fname]['fullname']
        fwdebug(6, "PFWBLOCK_DEBUG", "Copied fullname for %s = %s" % (fname, winst[IW_FILESECT][fname]))
        if fname in theinputs[SW_FILESECT]:
            winst['wrapinputs'][len(winst['wrapinputs'])+1] = moddict[SW_FILESECT][fname]['fullname']
        elif fname in theoutputs:
            winst['wrapoutputs'][len(winst['wrapoutputs'])+1] = moddict[SW_FILESECT][fname]['fullname']
    else:
        if 'filename' in moddict[SW_FILESECT][fname]:
            winst[IW_FILESECT][fname]['filename'] = config.search('filename', {PF_CURRVALS: currvals, 
                                                                               'searchobj': moddict[SW_FILESECT][fname], 
                                                                               'expand': True, 
                                                                               'required': True,
                                                                               'interpolate':True})[1]
        else:
            fwdebug(6, "PFWBLOCK_DEBUG", "creating filename for %s" % fname) 
            sobj = copy.deepcopy(finfo)
            sobj.update(winst)
            winst[IW_FILESECT][fname]['filename'] = config.get_filename(None, {PF_CURRVALS: currvals, 
                                                                'searchobj': sobj,
                                                                'expand': True}) 

        # Add runtime path to filename
        fwdebug(3,"PFWBLOCK_DEBUG", "creating path for %s" % fname)
        path = config.get_filepath('runtime', None, {PF_CURRVALS: currvals, 'searchobj': finfo})
        fwdebug(3, "PFWBLOCK_DEBUG", "\tpath = %s" % path)
        if type(winst[IW_FILESECT][fname]['filename']) is list:
            winst[IW_FILESECT][fname]['fullname'] = []
            fwdebug(3, "PFWBLOCK_DEBUG", "%s is a list, number of names = %s" % (fname,len(winst[IW_FILESECT][fname]['filename'])))
            for f in winst[IW_FILESECT][fname]['filename']:
                fwdebug(6, "PFWBLOCK_DEBUG", "path + filename = %s/%s" % (path,f))
                winst[IW_FILESECT][fname]['fullname'].append("%s/%s" % (path, f))
                if fname in theinputs[SW_FILESECT]:
                    winst['wrapinputs'][len(winst['wrapinputs'])+1] = "%s/%s" % (path,f)
                elif fname in theoutputs:
                    winst['wrapoutputs'][len(winst['wrapoutputs'])+1] = "%s/%s" % (path,f) 

            winst[IW_FILESECT][fname]['fullname'] = ','.join(winst[IW_FILESECT][fname]['fullname'])
        else:
            fwdebug(3, "PFWBLOCK_DEBUG", "Adding path to filename for %s" % fname)
            winst[IW_FILESECT][fname]['fullname'] = "%s/%s" % (path, winst[IW_FILESECT][fname]['filename'])
            if fname in theinputs[SW_FILESECT]:
                winst['wrapinputs'][len(winst['wrapinputs'])+1] = winst[IW_FILESECT][fname]['fullname']
            elif fname in theoutputs:
                winst['wrapoutputs'][len(winst['wrapoutputs'])+1] = winst[IW_FILESECT][fname]['fullname']


#        winst[IW_FILESECT][fname]['filename'] = config.get_filename(None, {PF_CURRVALS: currvals, 'searchobj': finfo}) 
#        if type(winst[IW_FILESECT][fname]['filename']) is list:
#            winst[IW_FILESECT][fname]['filename'] = ','.join(winst[IW_FILESECT][fname]['filename'])
#    if IW_REQ_META in finfo:
#        winst[IW_FILESECT][fname][IW_REQ_META] = copy.deepcopy(finfo[IW_REQ_META])
        del winst[IW_FILESECT][fname]['filename']

    fwdebug(3, "PFWBLOCK_DEBUG", "is_iter_obj = %s %s" % (is_iter_obj, finfo))
    if finfo is not None and is_iter_obj:
        fwdebug(3, "PFWBLOCK_DEBUG", "is_iter_obj = true")
        for key,val in finfo.items():
            if key not in ['fullname','filename']:
                fwdebug(3, "PFWBLOCK_DEBUG", "is_iter_obj: saving %s" % key)
                winst[key] = val
        
    fwdebug(3, "PFWBLOCK_DEBUG", "END: Done working on file %s" % fname)



#######################################################################
def assign_list_to_wrapper_inst(config, moddict, currvals, winst, lname, ldict):
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG: Working on list %s from %s" % (lname, moddict['modulename']))
    fwdebug(3, "PFWBLOCK_DEBUG", "currvals = %s" % (currvals))
    fwdebug(6, "PFWBLOCK_DEBUG", "ldict = %s" % (ldict))

    if IW_LISTSECT not in winst:
        winst[IW_LISTSECT] = {}

    winst[IW_LISTSECT][lname] = {}

    sobj = copy.deepcopy(ldict)
    sobj.update(winst)
    fwdebug(3, "PFWBLOCK_DEBUG", "sobj = %s" % (sobj))

    fwdebug(0, "PFWBLOCK_DEBUG", "creating listdir and listname")

    listdir = config.get_filepath('runtime', 'list', {PF_CURRVALS: currvals,
                         'required': True, 'interpolate': True,
                         'searchobj': sobj})
    
    listname = config.get_filename(None, {PF_CURRVALS: currvals,
                                   'searchobj': sobj, 'required': True, 'interpolate': True})
    fwdebug(3, "PFWBLOCK_DEBUG", "listname = %s" % (listname))
    listname = "%s/%s" % (listdir, listname)

    winst[IW_LISTSECT][lname]['fullname'] = listname
    fwdebug(3, "PFWBLOCK_DEBUG", "full listname = %s" % (winst[IW_LISTSECT][lname]['fullname']))
    if 'sublists' in ldict:
        sublist = find_sublist(ldict, winst)
        for llabel,lldict in sublist['list'][PF_LISTENTRY].items():
            for flabel,fdict in lldict['file'].items():
                winst['wrapinputs'][len(winst['wrapinputs'])+1] = fdict['fullname']
        output_list(config, winst[IW_LISTSECT][lname]['fullname'], sublist, lname, ldict, currvals)
#    else:
#        create_simple_list(config, lname, ldict, currvals)
    fwdebug(0, "PFWBLOCK_DEBUG", "END")

                        
#######################################################################
def assign_data_wrapper_inst(config, modname, wrapperinst):
    """ Assign data like files and lists to wrapper instances """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    moddict = config[SW_MODULESECT][modname] 
    currvals = { 'curr_module': modname}
    (found, loopkeys) = config.search('wrapperloop', 
                       {PF_CURRVALS: currvals,
                        'required': False, 'interpolate': True})
    if found:
        loopkeys = fwsplit(loopkeys.lower())
    else:
        loopkeys = []

    # figure out which lists/files are input files
    theinputs = which_are_inputs(config, modname)
    theoutputs = which_are_outputs(config, modname)

    for winst in wrapperinst.values():
        winst['wrapinputs'] = {}
        winst['wrapoutputs'] = {}

        # create currvals
        currvals = { 'curr_module': modname, PF_WRAPNUM: winst[PF_WRAPNUM]}
        for key in loopkeys:
            currvals[key] = winst[key]
        fwdebug(3, "PFWBLOCK_DEBUG", "currvals " + str(currvals))

        # do wrapper loop object first, if exists, to provide keys for filenames
        iter_obj_key = get_wrap_iter_obj_key(config, moddict)

        if iter_obj_key is not None or SW_FILESECT in moddict:
            fwdebug(0, "PFWBLOCK_DEBUG", "%s: Assigning files to wrapper inst" % winst[PF_WRAPNUM])

        if iter_obj_key is not None:
            (iter_obj_sect, iter_obj_name) = fwsplit(iter_obj_key, '.')
            iter_obj_dict = pfwutils.get_wcl_value(iter_obj_key, moddict) 
            fwdebug(3, "PFWBLOCK_DEBUG", "iter_obj %s %s" % (iter_obj_name, iter_obj_sect))
            if iter_obj_sect.lower() == SW_FILESECT.lower():
                assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals, winst, iter_obj_name, iter_obj_dict, True)
            elif iter_obj_sect.lower() == SW_LISTSECT.lower():
                assign_list_to_wrapper_inst(config, moddict, currvals, winst, iter_obj_name, iter_obj_dict)
            else:
                fwdie("Error: unknown iter_obj_sect (%s)" % iter_obj_sect, PF_EXIT_FAILURE)

        
        if SW_FILESECT in moddict:
            for fname, fdict in moddict[SW_FILESECT].items(): 
                if iter_obj_key is not None and \
                   iter_obj_sect.lower() == SW_FILESECT.lower() and \
                   iter_obj_name.lower() == fname.lower():
                    continue    # already did iter_obj
                assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals, winst, fname, fdict)

        fwdebug(3, "PFWBLOCK_DEBUG", "currvals " + str(currvals))

        if SW_LISTSECT in moddict:
            fwdebug(0, "PFWBLOCK_DEBUG", "%s: Assigning lists to wrapper inst" % winst[PF_WRAPNUM])
            for lname, ldict in moddict[SW_LISTSECT].items():
                if iter_obj_key is not None and \
                   iter_obj_sect.lower() == SW_LISTSECT.lower() and \
                   iter_obj_name.lower() == lname.lower():
                    fwdebug(3, "PFWBLOCK_DEBUG", "skipping list %s as already did for it as iter_obj" % lname)
                    continue    # already did iter_obj
                assign_list_to_wrapper_inst(config, moddict, currvals, winst, lname, ldict)
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def output_list(config, listname, sublist, lname, ldict, currvals):
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG: %s (%s)" % (lname, listname))
    fwdebug(3, "PFWBLOCK_DEBUG", "list dict: %s" % ldict)

    listdir = os.path.dirname(listname)
    coremakedirs(listdir)

    format = 'textsp'
    if 'format' in ldict:
        format = ldict['format']

    if 'columns' in ldict:
        columns = ldict['columns'].lower()
    else:
        fwdebug(3, "PFWBLOCK_DEBUG", "columns not in ldict, so defaulting to fullname")
        columns = 'fullname'
    fwdebug(3, "PFWBLOCK_DEBUG", "columns = %s" % columns)
    

    with open(listname, "w") as listfh:
        for linenick, linedict in sublist['list'][PF_LISTENTRY].items():
            output_line(listfh, linedict, format, fwsplit(columns))
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")




#####################################################################
def output_line(listfh, line, format, keyarr):
    """ output line into fo input list for science code"""
    fwdebug(4, "PFWBLOCK_DEBUG", "BEG line=%s  keyarr=%s" % (line, keyarr))

    format = format.lower()

    if format == 'config' or format == 'wcl':
        fh.write("<file>\n")

    numkeys = len(keyarr)
    for i in range(0, numkeys):
        key = keyarr[i]
        value = None
        fwdebug(4, "PFWBLOCK_DEBUG", "key: %s" % key)

        if '.' in  key:
            fwdebug(4, "PFWBLOCK_DEBUG", "Found period in key")
            [nickname, key2] = key.replace(' ','').split('.')
            fwdebug(4, "PFWBLOCK_DEBUG", "\tnickname = %s, key2 = %s" % (nickname, key2))
            value = get_value_from_line(line, key2, nickname, None)
            if value == None:
                fwdebug(4, "PFWBLOCK_DEBUG", "Didn't find value in line with nickname %s" % (nickname))
                fwdebug(4, "PFWBLOCK_DEBUG", "Trying to find %s without nickname" % (key2))
                value = get_value_from_line(line, key2, None, 1)
                if value == None:
                    fwdie("Error: could not find value %s for line...\n%s" % (key, line), PF_EXIT_FAILURE)
                else: # assume nickname was really table name
                    fwdebug(4, "PFWBLOCK_DEBUG", "\tassuming nickname (%s) was really table name" % (nickname))
                    key = key2
        else:
            value = get_value_from_line(line, key, None, 1)

        # handle last field (separate to avoid trailing comma)
        fwdebug(4, "PFWBLOCK_DEBUG", "printing key=%s value=%s" % (key, value))
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
    
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    moddict = config[SW_MODULESECT][modname] 
    outputfiles = which_are_outputs(config, modname)

    input_filenames = []
    output_filenames = []
    for winst in wrapperinst.values():
        for f in winst['wrapinputs'].values():
            input_filenames.append(parse_fullname(f, 2))

        for f in winst['wrapoutputs'].values():
            output_filenames.append(parse_fullname(f, 2))



        # create searching options
        currvals = {'curr_module': modname, PF_WRAPNUM: winst[PF_WRAPNUM]}
        #(found, loopkeys) = config.search('wrapperloop', searchopts)
        #if found:
        #    loopkeys = fwsplit(loopkeys.lower())
        #    for key in loopkeys:
        #        currvals[key] = winst[key]
        searchopts = {PF_CURRVALS: currvals, 
                      'searchobj': winst, 
                      'interpolate': True,
                      'required': True}

        
        if SW_FILESECT in moddict:
            for fname, fdict in moddict[SW_FILESECT].items(): 
                if 'listonly' in fdict and convertBool(fdict['listonly']):
                    fwdebug(3, "PFWBLOCK_DEBUG", "Skipping %s due to listonly key" % fname)
                    continue

                fwdebug(3, 'PFWBLOCK_DEBUG', '%s: working on file: %s' % (winst[PF_WRAPNUM], fname))
                fwdebug(4, "PFWBLOCK_DEBUG", "fullname = %s" % (winst[IW_FILESECT][fname]['fullname']))

                
                for k in ['filetype', WCL_META_REQ, WCL_META_OPT, SAVE_FILE_ARCHIVE, DIRPAT]:
                    if k in fdict:
                        fwdebug(3, "PFWBLOCK_DEBUG", "%s copying %s" % (fname, k))
                        winst[IW_FILESECT][fname][k] = copy.deepcopy(fdict[k])
                    else:
                        fwdebug(3, "PFWBLOCK_DEBUG", "%s: no %s" % (fname, k))

                if SW_OUTPUT_OPTIONAL in fdict:
                    fwdebug(3, "PFWBLOCK_DEBUG", "%s copying %s " % (fname, SW_OUTPUT_OPTIONAL))
                    
                    winst[IW_FILESECT][fname][IW_OUTPUT_OPTIONAL] = convertBool(fdict[SW_OUTPUT_OPTIONAL])

                hdrups = pfwutils.get_hdrup_sections(fdict, WCL_UPDATE_HEAD_PREFIX)
                for hname, hdict in hdrups.items():
                    fwdebug(3, "PFWBLOCK_DEBUG", "%s copying %s" % (fname, hname))
                    winst[IW_FILESECT][fname][hname] = copy.deepcopy(hdict)

                # save OPS path for archive
                fwdebug(4, "PFWBLOCK_DEBUG", "Is fname (%s) in outputfiles? %s" % (fname, fname in outputfiles))
                fwdebug(4, "PFWBLOCK_DEBUG", "Is save_file_archive true? %s" % (checkTrue(SAVE_FILE_ARCHIVE, fdict, True)))
                if checkTrue(SAVE_FILE_ARCHIVE, fdict, True) and fname in outputfiles: 
                    winst[IW_FILESECT][fname][SAVE_FILE_ARCHIVE] = True   # canonicalize
                    if DIRPAT not in fdict:
                        print "Warning: Could not find %s in %s's section" % (DIRPAT,fname)
                    else:
                        searchobj = copy.deepcopy(fdict)
                        searchobj.update(winst)
                        searchopts['searchobj'] = searchobj
                        winst[IW_FILESECT][fname]['archivepath'] = config.get_filepath('ops', 
                                                                        fdict[DIRPAT], searchopts)
                else:
                    winst[IW_FILESECT][fname][SAVE_FILE_ARCHIVE] = False   # canonicalize

            fwdebug(4, "PFWBLOCK_DEBUG", "fdict = %s" % fdict)
            fwdebug(4, "PFWBLOCK_DEBUG", "winst[%s] = %s" % (IW_FILESECT,  winst[IW_FILESECT]))

        if SW_LISTSECT in moddict:
            for lname, ldict in moddict[SW_LISTSECT].items(): 
                for k in ['columns']:
                    if k in ldict:
                        fwdebug(3, "PFWBLOCK_DEBUG", "%s copying %s" % (lname, k))
                        winst[IW_LISTSECT][lname][k] = copy.deepcopy(ldict[k])
                    else:
                        fwdebug(3, "PFWBLOCK_DEBUG", "%s: no %s" % (lname, k))

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


    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return input_filenames, output_filenames


#######################################################################
def add_file_metadata(config, modname):
    """ add file metadata sections to a single file section from a module"""

    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    fwdebug(0, "PFWBLOCK_DEBUG", "Working on module " + modname)
    moddict = config[SW_MODULESECT][modname]
    execs = pfwutils.get_exec_sections(moddict, SW_EXECPREFIX)

    if SW_FILESECT in moddict:
        filemgmt = None         
        try:
            filemgmt_class = dynamically_load_class(config['filemgmt'])
            paramDict = config.get_param_info(filemgmt_class.requested_config_vals(), 
                                               {PF_CURRVALS: {'curr_module': modname}})
            filemgmt = filemgmt_class(config=paramDict)
        except:
            print "Error:  Problems dynamically loading class (%s) in order to get metadata specs" % config['filemgmt']
            raise

        for k in execs:
            if SW_OUTPUTS in moddict[k]:
                for outfile in fwsplit(moddict[k][SW_OUTPUTS]):
                    fwdebug(3, "PFWBLOCK_DEBUG", "Working on output file " + outfile)
                    m = re.match('%s.(\w+)' % SW_FILESECT, outfile)
                    if m:
                        fname = m.group(1)
                        fwdebug(3, "PFWBLOCK_DEBUG", "Working on file " + fname)
                        fdict = moddict[SW_FILESECT][fname]
                        filetype = fdict['filetype'].lower()
                        wclsect = "%s.%s" % (IW_FILESECT, fname)

                        print "len(config[FILE_HEADER_INFO]) =", len(config['FILE_HEADER_INFO'])
                        meta_specs = metautils.get_metadata_specs(filetype, config['FILETYPE_METADATA'], config['FILE_HEADER'], 
                                                        wclsect, updatefits=True)
                        fwdebug(0, "PFWBLOCK_DEBUG", "meta_specs = %s" % meta_specs)
                        fwdebug(0, "PFWBLOCK_DEBUG", "fdict = %s" % fdict)
                        fdict.update(meta_specs)

             
                        # add descriptions/types to submit-wcl specified updates if missing
                        hdrups = pfwutils.get_hdrup_sections(fdict, WCL_UPDATE_HEAD_PREFIX)
                        for hname, hdict in sorted(hdrups.items()):
                            for key,val in hdict.items():
                                if key != WCL_UPDATE_WHICH_HEAD:
                                    valparts = fwsplit(val, '/')
                                    fwdebug(3, "PFWBLOCK_DEBUG", "hdrup: key, valparts = %s, %s" % (key, valparts))
                                    if len(valparts) == 1:
                                        if 'COPY{' not in valparts[0]:  # wcl specified value, look up rest from config
                                            newvaldict = metautils.create_update_items('V', [key], config['file_header'], header_value={key:val}) 
                                            hdict.update(newvaldict)
                                    elif len(valparts) != 3:  # 3 is valid full spec of update header line
                                        fwdie('Error:  invalid header update line (%s = %s)\nNeeds value[/descript/type]' % (key,val), PF_EXIT_FAILURE)


                        # add some fields needed by framework for processing output wcl (not stored in database)
                        if WCL_META_WCL not in fdict[WCL_META_REQ]:
                            fdict[WCL_META_REQ][WCL_META_WCL] = ''
                        else:
                            fdict[WCL_META_REQ][WCL_META_WCL] += ','

                        fdict[WCL_META_REQ][WCL_META_WCL] += '%(sect)s.fullname,%(sect)s.sectname' % ({'sect':wclsect})
                        #print fdict
                        #sys.exit(1)
                    else:
                        fwdebug(3, "PFWBLOCK_DEBUG", "output file %s doesn't have definition (%s) " % (k, SW_FILESECT))

                fwdebug(3, "PFWBLOCK_DEBUG", "output file dictionary for %s = %s" % (outfile, fdict))
                
            else:
                fwdebug(3, "PFWBLOCK_DEBUG", "No was_generated_by for %s" % (k))

    else:
        fwdebug(3, "PFWBLOCK_DEBUG", "No file section (%s)" % SW_FILESECT)
        
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")

    #exit(0)
    
                




#######################################################################
def write_jobwcl(config, jobkey, jobnum, numexpwrap, wrapinputs):
    """ write a little config file containing variables needed at the job level """
    fwdebug(3, "PFWBLOCK_DEBUG", "BEG jobnum=%s jobkey=%s" % (jobnum, jobkey))

    jobwclfile = config.get_filename('jobwcl', {PF_CURRVALS: {PF_JOBNUM: jobnum}, 'required': True, 'interpolate': True})
    outputwcltar = config.get_filename('outputwcltar', {PF_CURRVALS:{'jobnum': jobnum}, 'required': True, 'interpolate': True})

    #      Since wcl's variable syntax matches shell variable syntax and 
    #      underscores are used to separate name parts, have to use place 
    #      holder for jobnum and replace later with shell variable
    #      Otherwise, get_filename fails to substitute for padjnum
    #envfile = config.get_filename('envfile', {PF_CURRVALS: {PF_JOBNUM:"9999"}})
    #envfile = envfile.replace("j9999", "j${padjnum}")
    envfile = config.get_filename('envfile')

    jobwcl = {REQNUM: config.search(REQNUM, { 'required': True,
                                    'interpolate': True})[1], 
              UNITNAME:config.search(UNITNAME, { 'required': True,
                                    'interpolate': True})[1], 
              ATTNUM: config.search(ATTNUM, { 'required': True,
                                    'interpolate': True})[1], 
              PF_BLKNUM: config.search(PF_BLKNUM, { 'required': True,
                                    'interpolate': True})[1], 
              PF_JOBNUM: jobnum,
              'numexpwrap': numexpwrap,
              'usedb': config.search(PF_USE_DB_OUT, { 'required': True,
                                    'interpolate': True})[1], 
              'useqcf': config.search(PF_USE_QCF, {'required': True,
                                    'interpolate': True})[1], 
              'pipeprod': config.search('pipeprod', {'required': True,
                                    'interpolate': True})[1], 
              'pipever': config.search('pipever', {'required': True,
                                    'interpolate': True})[1], 
              'jobkeys': jobkey[1:].replace('_',','),
              'archive': config['archive'],
              'output_wcl_tar': outputwcltar,
              'envfile': envfile,
              'junktar': config.get_filename('junktar', {PF_CURRVALS:{'jobnum': jobnum}}),
              'junktar_archive_path': config.get_filepath('ops', 'junktar', {PF_CURRVALS:{'jobnum': jobnum}})
            }

    if CREATE_JUNK_TARBALL in config and convertBool(config[CREATE_JUNK_TARBALL]):
        jobwcl[CREATE_JUNK_TARBALL] = True
    else:
        jobwcl[CREATE_JUNK_TARBALL] = False


    if not USE_TARGET_ARCHIVE_INPUT in config or convertBool(config[USE_TARGET_ARCHIVE_INPUT]):
        jobwcl[USE_TARGET_ARCHIVE_INPUT] = True
    else:
        jobwcl[USE_TARGET_ARCHIVE_INPUT] = False


    if not USE_TARGET_ARCHIVE_OUTPUT in config or convertBool(config[USE_TARGET_ARCHIVE_OUTPUT]):
        jobwcl[USE_TARGET_ARCHIVE_OUTPUT] = True
    else:
        jobwcl[USE_TARGET_ARCHIVE_OUTPUT] = False


    if jobwcl[USE_TARGET_ARCHIVE_INPUT] or jobwcl[USE_TARGET_ARCHIVE_OUTPUT]: 
        jobwcl[TARGET_ARCHIVE] = config[TARGET_ARCHIVE]
        target_archive = config[TARGET_ARCHIVE]
    else:
        jobwcl[TARGET_ARCHIVE] = None
        target_archive = 'no_archive'


    if USE_HOME_ARCHIVE_INPUT in config:
        jobwcl[USE_HOME_ARCHIVE_INPUT] = config[USE_HOME_ARCHIVE_INPUT].lower()
    else:
        jobwcl[USE_HOME_ARCHIVE_INPUT] = 'never'

    if USE_HOME_ARCHIVE_OUTPUT in config:
        jobwcl[USE_HOME_ARCHIVE_OUTPUT] = config[USE_HOME_ARCHIVE_OUTPUT].lower()
    else:
        jobwcl[USE_HOME_ARCHIVE_OUTPUT] = 'never'
    

    if jobwcl[USE_HOME_ARCHIVE_INPUT] != 'never' or jobwcl[USE_HOME_ARCHIVE_OUTPUT] != 'never':
        jobwcl[HOME_ARCHIVE] = config[HOME_ARCHIVE]
        home_archive = config[HOME_ARCHIVE]
    else:
        jobwcl[HOME_ARCHIVE] = None
        home_archive = 'no_archive'



    # include variables needed by target archive's file mgmt class
    if jobwcl[TARGET_ARCHIVE] is not None:
        try:
            filemgmt_class = dynamically_load_class(config['archive'][target_archive]['filemgmt'])
            valDict = config.get_param_info(filemgmt_class.requested_config_vals())
            jobwcl.update(valDict)
        except Exception as err:
            print "ERROR\nError: creating loading job_file_mvmt class\n%s" % err
            raise

    # include variables needed by home archive's file mgmt class
    if jobwcl[HOME_ARCHIVE] is not None:
        try:
            filemgmt_class = dynamically_load_class(config['archive'][home_archive]['filemgmt'])
            valDict = config.get_param_info(filemgmt_class.requested_config_vals(),
                                        {PF_CURRVALS: config['archive'][home_archive]})
            jobwcl.update(valDict)
        except Exception as err:
            print "ERROR\nError: creating loading job_file_mvmt class\n%s" % err
            raise

    try: 
        jobwcl['job_file_mvmt'] = config['job_file_mvmt'][config['curr_site']][home_archive][target_archive]
    except:
        print "\n\n\nError: Problem trying to find: config['job_file_mvmt'][%s][%s][%s]" % (config['curr_site'], home_archive,target_archive)
        print "USE_HOME_ARCHIVE_INPUT =", jobwcl[USE_HOME_ARCHIVE_INPUT]
        print "USE_HOME_ARCHIVE_OUTPUT =", jobwcl[USE_HOME_ARCHIVE_OUTPUT]
        print "site =", config['curr_site']
        print "home_archive =", home_archive
        print "target_archive =", target_archive
        print 'job_file_mvmt =' 
        pretty_print_dict(config['job_file_mvmt'])
        print "\n"
        raise

    # include variables needed by job_file_mvmt class
    try:
        jobfilemvmt_class = dynamically_load_class(jobwcl['job_file_mvmt']['mvmtclass'])
        valDict = config.get_param_info(jobfilemvmt_class.requested_config_vals(),
                                        {PF_CURRVALS: jobwcl['job_file_mvmt']})
        jobwcl.update(valDict)
    except Exception as err:
        print "ERROR\nError: creating loading job_file_mvmt class\n%s" % err
        raise


    if convertBool(config[PF_USE_DB_OUT]):
        if 'target_des_services' in config and config['target_des_services'] is not None: 
            jobwcl['des_services'] = config['target_des_services']
        jobwcl['des_db_section'] = config['target_des_db_section']


    jobwcl['filetype_metadata'] = config['filetype_metadata']
    jobwcl[IW_EXEC_DEF] = config[SW_EXEC_DEF]
    jobwcl['wrapinputs'] = wrapinputs

    fwdebug(3, "PFWBLOCK_DEBUG", "jobwcl.keys() = %s" % jobwcl.keys())
   
    with open(jobwclfile, 'w') as wclfh:
        wclutils.write_wcl(jobwcl, wclfh, True, 4)

    fwdebug(3, "PFWBLOCK_DEBUG", "END\n\n")
    return (jobwclfile, outputwcltar, envfile)
    

#######################################################################
def add_needed_values(config, modname, wrapinst, wrapwcl):
    """ Make sure all variables in the wrapper instance have values in the wcl """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    
    # start with those needed by framework
    neededvals = {REQNUM: config.search(REQNUM,
                                   {PF_CURRVALS: {'curr_module': modname},
                                    'searchobj': wrapinst,
                                    'required': True,
                                    'interpolate': True})[1], 
                  UNITNAME:config.search(UNITNAME, 
                                   {PF_CURRVALS: {'curr_module': modname},
                                    'searchobj': wrapinst,
                                    'required': True,
                                    'interpolate': True})[1], 
                  ATTNUM: config.search(ATTNUM,
                                   {PF_CURRVALS: {'curr_module': modname},
                                    'searchobj': wrapinst,
                                    'required': True,
                                    'interpolate': True})[1], 
                  PF_BLKNUM: config.search(PF_BLKNUM,
                                   {PF_CURRVALS: {'curr_module': modname},
                                    'searchobj': wrapinst,
                                    'required': True,
                                    'interpolate': True})[1], 
                  PF_JOBNUM: config.search(PF_JOBNUM,
                                   {PF_CURRVALS: {'curr_module': modname},
                                    'searchobj': wrapinst,
                                    'required': True,
                                    'interpolate': True})[1], 
                  PF_WRAPNUM: config.search(PF_WRAPNUM,
                                   {PF_CURRVALS: {'curr_module': modname},
                                    'searchobj': wrapinst,
                                    'required': True,
                                    'interpolate': True})[1], 
                 }

    # start with specified
    if 'req_vals' in config[SW_MODULESECT][modname]: 
        for rv in fwsplit(config[SW_MODULESECT][modname]['req_vals']):
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
            fwdebug(4, "PFWBLOCK_DEBUG", "nval = %s" % nval)
            if type(neededvals[nval]) is bool:
                if ':' in nval:
                    nval = nval.split(':')[0]

                if '.' not in nval:
                    (found, val) = config.search(nval, 
                                   {PF_CURRVALS: {'curr_module': modname},
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
                        

                fwdebug(4, "PFWBLOCK_DEBUG", "val = %s" % val)

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


    #print "neededvals = "
    #wclutils.write_wcl(neededvals)
    #print "wrapwcl = "
    #wclutils.write_wcl(wrapwcl)


    # add needed values to wrapper wcl
    for key, val in neededvals.items():
        pfwutils.set_wcl_value(key, val, wrapwcl)
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def create_wrapper_inst(config, modname, loopvals):
    """ Create set of empty wrapper instances """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    wrapperinst = {}
    (found, loopkeys) = config.search('wrapperloop', 
                   {PF_CURRVALS: {'curr_module': modname},
                    'required': False, 'interpolate': True})
    wrapperinst = {}
    if found:
        fwdebug(0, "PFWBLOCK_DEBUG", "loopkeys = %s" % loopkeys)
        loopkeys = fwsplit(loopkeys.lower())
        loopkeys.sort()  # sort so can make same key easily

        for instvals in loopvals:
            fwdebug(3, "PFWBLOCK_DEBUG", "creating instance for %s" % str(instvals) )
            
            config.inc_wrapnum()
            winst = {PF_WRAPNUM: config[PF_WRAPNUM]}

            if len(instvals) != len(loopkeys):
                fwdebug(0, "PFWBLOCK_DEBUG", "Error: invalid number of values for instance")
                fwdebug(0, "PFWBLOCK_DEBUG", "\t%d loopkeys (%s)" % (len(loopkeys), loopkeys))
                fwdebug(0, "PFWBLOCK_DEBUG", "\t%d instvals (%s)" % (len(instvals), instvals))
                raise IndexError("Invalid number of values for instance")

            try:
                instkey = ""
                for k in range(0, len(loopkeys)):
                    winst[loopkeys[k]] = instvals[k] 
                    instkey += instvals[k] + '_'
            except:
                fwdebug(0, "PFWBLOCK_DEBUG", "Error: problem trying to create wrapper instance")
                fwdebug(0, "PFWBLOCK_DEBUG", "\tWas creating instance for %s" % str(instvals) )
                fwdebug(0, "PFWBLOCK_DEBUG", "\tloopkeys = %s" % loopkeys)
                raise

            wrapperinst[instkey] = winst
    else:
        config.inc_wrapnum()
        wrapperinst['noloop'] = {PF_WRAPNUM: config[PF_WRAPNUM]}

    fwdebug(0, "PFWBLOCK_DEBUG", "Number wrapper inst: %s" % len(wrapperinst))
    if len(wrapperinst) == 0:
        fwdebug(0, "PFWBLOCK_DEBUG", "Error: 0 wrapper inst")
        raise Exception("Error: 0 wrapper instances")
        
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return wrapperinst


#####################################################################
def read_master_lists(config, modname, modules_prev_in_list):
    """ Read master lists and files from files created earlier """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    # create python list of files and lists for this module
    searchobj = config.combine_lists_files(modname)

    for (sname, sdict) in searchobj:
#        if 'depends' not in sdict or \
#            sdict['depends'] not in modules_prev_in_list:
            # get filename for file containing dataset
        if 'qoutfile' in sdict:
            qoutfile = sdict['qoutfile']
#            else:  # assume depricated xml
#                qoutfile = "%s_%s.xml" % (modname, sname)
            print "\t\t%s: reading master dataset from %s" % (sname, qoutfile)

            # read dataset file
            starttime = time.time()
            print "\t\t\tReading file - start ", starttime
            if qoutfile.endswith(".xml"):
#                master = pfwxml.read_xml(qoutfile)
                raise Exception("xml datasets not supported yet")
            elif qoutfile.endswith(".wcl"):
                with open(qoutfile, 'r') as wclfh:
                    master = wclutils.read_wcl(wclfh, filename=qoutfile)
                    fwdebug(3, "PFWBLOCK_DEBUG", "master.keys() = " % master.keys())
            else:
                raise Exception("Unsupported dataset format in qoutfile for object %s in module %s (%s) " % (sname, modname, qoutfile))
            endtime = time.time()
            print "\t\t\tReading file - end ", endtime
            print "\t\t\tReading file took %s seconds" % (endtime - starttime)

            numlines = len(master['list'][PF_LISTENTRY])
            print "\t\t\tNumber of lines in dataset %s: %s\n" % (sname, numlines)

            if numlines == 0:
                raise Exception("ERROR: 0 lines in dataset %s in module %s" % (sname, modname))

            sdict['master'] = master
#            sdict['depends'] = 0
#        else:
#            sdict['depends'] = 1

    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def create_fullnames(config, modname):
    """ add paths to filenames """    # what about compression extension

    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    dataset = config.combine_lists_files(modname)
    moddict = config[SW_MODULESECT][modname]

    for (sname, sdict) in dataset:
        if 'master' in sdict:
            master = sdict['master']
            numlines = len(master['list'][PF_LISTENTRY])
            print "\t%s-%s: number of lines in master = %s" % (modname, sname, numlines)
            if numlines == 0:
                fwdie("Error: 0 lines in master list", PF_EXIT_FAILURE)

            if 'columns' in sdict:   # list
                colarr = fwsplit(sdict['columns'])
                dictcurr = {}
                for c in colarr:
                    m = re.search("(\S+).fullname", c)
                    if m:
                        flabel = m.group(1)
                        if flabel in moddict[SW_FILESECT]:
                            dictcurr[flabel] = copy.deepcopy(moddict[SW_FILESECT][flabel])
                            dictcurr[flabel]['curr_module'] = modname
                        else:
                            print "list files = ", moddict[SW_FILESECT].keys()
                            fwdie("Error: Looking at list columns - could not find %s def in dataset" % flabel, PF_EXIT_FAILURE)
                        
                for llabel,ldict in master['list'][PF_LISTENTRY].items():
                    for flabel,fdict in ldict['file'].items():
                        if flabel in dictcurr:
                            path = config.get_filepath('runtime', None, {PF_CURRVALS: dictcurr[flabel], 'searchobj': fdict})
                            fdict['fullname'] = "%s/%s" % (path, fdict['filename'])
                        elif len(dictcurr) == 1:
                            path = config.get_filepath('runtime', None, {PF_CURRVALS: dictcurr.values()[0], 'searchobj': fdict})
                            fdict['fullname'] = "%s/%s" % (path, fdict['filename'])
                        else:
                            print "dictcurr: ", dictcurr.keys()
                            fwdie("Error: Looking at lines - could not find %s def in dictcurr" % flabel, PF_EXIT_FAILURE)
                            
                     
            else:  # file
                currvals = copy.deepcopy(sdict) 
                currvals['curr_module'] = modname

                for llabel,ldict in master['list'][PF_LISTENTRY].items():
                    for flabel,fdict in ldict['file'].items():
                        path = config.get_filepath('runtime', None, {PF_CURRVALS: currvals, 'searchobj': fdict})
                        fdict['fullname'] = "%s/%s" % (path, fdict['filename'])
        else:
            print "\t%s-%s: no masterlist...skipping" % (modname, sname)

    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def create_sublists(config, modname):
    """ break master lists into sublists based upon match or divide_by """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    dataset = config.combine_lists_files(modname)

    for (sname, sdict) in dataset:
        if 'master' in sdict:
            master = sdict['master']
            numlines = len(master['list'][PF_LISTENTRY])
            print "\t%s-%s: number of lines in master = %s" % (modname, sname, numlines)
            if numlines == 0:
                fwdie("Error: 0 lines in master list", PF_EXIT_FAILURE)

            sublists = {}
            keys = get_match_keys(sdict)

            if len(keys) > 0: 
                sdict['keyvals'] = {} 
                print "\t%s-%s: dividing by %s" % (modname, sname, keys)
                for linenick, linedict in master['list'][PF_LISTENTRY].items():
                    index = ""
                    listkeys = []
                    for key in keys:
                        fwdebug(3, "PFWBLOCK_DEBUG", "key = %s" % key)
                        fwdebug(3, "PFWBLOCK_DEBUG", "linedict = %s" % linedict)
                        val = get_value_from_line(linedict, key, None, 1)
                        index += val + '_'
                        listkeys.append(val)
                    sdict['keyvals'][index] = listkeys
                    if index not in sublists:
                        sublists[index] = {'list': {PF_LISTENTRY: {}}}
                    sublists[index]['list'][PF_LISTENTRY][linenick] = linedict
            else:
                sublists['onlyone'] = master

            del sdict['master']
            sdict['sublists'] = sublists
            print "\t%s-%s: number of sublists = %s" % (modname, sname, len(sublists))
            fwdebug(3, "PFWBLOCK_DEBUG", "sublist.keys()=%s" % sublists.keys())
            fwdebug(4, "PFWBLOCK_DEBUG", "sublists[sublists.keys()[0]]=%s" % sublists[sublists.keys()[0]])
            print ""
            print ""
        else:
            print "\t%s-%s: no masterlist...skipping" % (modname, sname)

    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def get_wrap_iter_obj_key(config, moddict):
    iter_obj_key = None
    if 'loopobj' in moddict:
        iter_obj_key = moddict['loopobj'].lower()
    else:
        fwdebug(0, "PFWBLOCK_DEBUG", "Could not find loopobj. moddict keys = %s" % moddict.keys())
        fwdebug(6, "PFWBLOCK_DEBUG", "Could not find loopobj in modict %s" % moddict)
    return iter_obj_key


#######################################################################
def get_wrapper_loopvals(config, modname):
    """ get the values for the wrapper loop keys """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    loopvals = []

    moddict = config[SW_MODULESECT][modname]
    (found, loopkeys) = config.search('wrapperloop', 
                   {PF_CURRVALS: {'curr_module': modname},
                    'required': False, 'interpolate': True})
    if found:
        fwdebug(0,"PFWBLOCK_DEBUG", "\tloopkeys = %s" % loopkeys)
        loopkeys = fwsplit(loopkeys.lower())
        loopkeys.sort()  # sort so can make same key easily


        ## determine which list/file would determine loop values
        iter_obj_key = get_wrap_iter_obj_key(config, moddict)
        fwdebug(0, "PFWBLOCK_DEBUG", "iter_obj_key=%s" % iter_obj_key)

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
                fwdebug(0, "PFWBLOCK_DEBUG", "key=%s" % key)
                (found, val) = config.search(key, 
                            {PF_CURRVALS: {'curr_module': modname},
                            'required': False, 
                            'interpolate': True})
                fwdebug(0, "PFWBLOCK_DEBUG", "found=%s" % found)
                if found:
                    fwdebug(0, "PFWBLOCK_DEBUG", "val=%s" % val)
                    val = fwsplit(val)
                    loopvals.append(val)
            loopvals = itertools.product(*loopvals)
        
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return loopvals


#############################################################
def get_value_from_line(line, key, nickname=None, numvals=None):
    """ Return value from a line in master list """
    fwdebug(1, "PFWBLOCK_DEBUG", "BEG: key = %s, nickname = %s, numvals = %s" % (key, nickname, numvals))
    # returns None if 0 matches
    #         scalar value if 1 match
    #         array if > 1 match

    # since values could be repeated across files in line, 
    # create hash of values to get unique values
    valhash = {}

    if '.' in key:
        fwdebug(1, "PFWBLOCK_DEBUG", "Found nickname")
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
        fwdie("Error: number found (%s) doesn't match requested (%s)" % (len(valarr), numvals), PF_EXIT_FAILURE)

    if len(valarr) == 0:
        retval = None
    elif numvals == 1 or len(valarr) == 1:
        retval = valarr[0].strip()
    else:
        retval = valarr.strip()

    fwdebug(1, "PFWBLOCK_DEBUG", "END\n\n")
    return retval


#######################################################################
# Assumes currvals includes specific values (e.g., band, ccd)
def create_single_wrapper_wcl(config, modname, wrapinst):
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s %s" % (modname, wrapinst[PF_WRAPNUM]))

    fwdebug(3, "PFWBLOCK_DEBUG", "\twrapinst=%s" % wrapinst)

    currvals = {'curr_module': modname, PF_WRAPNUM: wrapinst[PF_WRAPNUM]}


    wrapperwcl = {'modname': modname}



    # file is optional
    if IW_FILESECT in wrapinst:
        wrapperwcl[IW_FILESECT] = copy.deepcopy(wrapinst[IW_FILESECT])
        fwdebug(3, "PFWBLOCK_DEBUG", "\tfile=%s" % wrapperwcl[IW_FILESECT])
        for (sectname, sectdict) in wrapperwcl[IW_FILESECT].items():
            sectdict['sectname'] = sectname

    # list is optional
    if IW_LISTSECT in wrapinst:
        wrapperwcl[IW_LISTSECT] = copy.deepcopy(wrapinst[IW_LISTSECT])
        fwdebug(3, "PFWBLOCK_DEBUG", "\tlist=%s" % wrapperwcl[IW_LISTSECT])


    # do we want exec_list variable?
    fwdebug(3, "PFWBLOCK_DEBUG", "\tSW_EXECPREFIX=%s" % SW_EXECPREFIX)
    numexec = 0
    modname = currvals['curr_module']
    moddict = config[SW_MODULESECT][modname]
    execs = pfwutils.get_exec_sections(moddict, SW_EXECPREFIX)
    for execkey in execs:
        fwdebug(3, 'PFWBLOCK_DEBUG', "Working on exec section (%s)"% execkey)
        numexec += 1
        iwkey = execkey.replace(SW_EXECPREFIX, IW_EXECPREFIX)
        wrapperwcl[iwkey] = {}
        execsect = moddict[execkey]
        fwdebug(3, 'PFWBLOCK_DEBUG', "\t\t(%s)" % (execsect))
        for key, val in execsect.items():
            fwdebug(5, 'PFWBLOCK_DEBUG', "\t\t%s (%s)" % (key, val))
            if key == SW_INPUTS:
                iwexkey = IW_INPUTS
            elif key == SW_OUTPUTS:
                iwexkey = IW_OUTPUTS
            elif key == SW_ANCESTRY:
                iwexkey = IW_ANCESTRY
            else:
                iwexkey = key

            if key != 'cmdline':
                wrapperwcl[iwkey][iwexkey] = config.interpolate(val, {PF_CURRVALS: currvals, 'searchobj': val,
                                                            'required': True, 'interpolate': True})
            else:
                wrapperwcl[iwkey]['cmdline'] = copy.deepcopy(val)
        if 'execnum' not in wrapperwcl[execkey]:
            result = re.match('%s(\d+)' % IW_EXECPREFIX, execkey)
            if not result:
                fwdie('Error:  Could not determine execnum from exec label %s' % execkey, PF_EXIT_FAILURE)
            wrapperwcl[execkey]['execnum'] = result.group(1)

    if SW_WRAPSECT in config[SW_MODULESECT][modname]:
        fwdebug(3, 'PFWBLOCK_DEBUG', "Copying wrapper section (%s)"% SW_WRAPSECT)
        wrapperwcl[IW_WRAPSECT] = copy.deepcopy(config[SW_MODULESECT][modname][SW_WRAPSECT])

    if IW_WRAPSECT not in wrapperwcl:
        fwdebug(3, 'PFWBLOCK_DEBUG', "%s (%s): Initializing wrapper section (%s)"% (modname, wrapinst[PF_WRAPNUM], IW_WRAPSECT))
        wrapperwcl[IW_WRAPSECT] = {}
    wrapperwcl[IW_WRAPSECT]['pipeline'] = config['pipeprod']
    wrapperwcl[IW_WRAPSECT]['pipever'] = config['pipever']

    wrapperwcl[IW_WRAPSECT]['wrappername'] = wrapinst['wrappername']
    wrapperwcl[IW_WRAPSECT]['outputwcl'] = wrapinst['outputwcl']
    wrapperwcl[IW_WRAPSECT]['tmpfile_prefix'] =  config.search('tmpfile_prefix',
                                {PF_CURRVALS: currvals,
                                 'required': True, 'interpolate': True})[1]
    wrapperwcl['log'] = wrapinst['log'] 
    wrapperwcl['log_archive_path'] = wrapinst['log_archive_path']


    if numexec == 0:
        wclutils.write_wcl(config[SW_MODULESECT][modname])
        raise Exception("Error:  Could not find an exec section for module %s" % modname)
        

    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")

    return wrapperwcl


# translate sw terms to iw terms in values if needed
def translate_sw_iw(config, wrapperwcl, modname, winst):
    fwdebug(1, "PFWBLOCK_DEBUG", "BEG %s" % modname)


    if ( (SW_FILESECT == IW_FILESECT) and 
         (SW_LISTSECT == IW_LISTSECT) ):
        print "Skipping translation SW to IW"
    else:
        translation = [(SW_FILESECT, IW_FILESECT),
                       (SW_LISTSECT, IW_LISTSECT)]
        wrappervars = {}
        wcltodo = [wrapperwcl]
        while len(wcltodo) > 0:
            wcl = wcltodo.pop()
            for key,val in wcl.items():
                fwdebug(4, 'PFWBLOCK_DEBUG', "key = %s" % (key))
                if type(val) is dict or type(val) is OrderedDict:
                    wcltodo.append(val)
                elif type(val) is str:
                    fwdebug(4, 'PFWBLOCK_DEBUG', "val = %s, %s" % (val, type(val)))
                    for (sw, iw) in translation:
                        val = val.replace(sw+'.', iw+'.')
                    fwdebug(4, 'PFWBLOCK_DEBUG', "final value = %s" % (val))
                    wcl[key] = val

    #print "new wcl = ", wclutils.write_wcl(wrapperwcl, sys.stdout, True, 4)
    fwdebug(1, "PFWBLOCK_DEBUG", "END\n\n")
               


#######################################################################
def create_module_wrapper_wcl(config, modname, wrapinst):
    """ Create wcl for wrapper instances for a module """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    tasks = []

    if modname not in config[SW_MODULESECT]:
        raise Exception("Error: Could not find module description for module %s\n" % (modname))




    for inst in wrapinst.values():
        wrapperwcl = create_single_wrapper_wcl(config, modname, inst)
        #fix_globalvars(config, wrapperwcl, modname, inst)
        #wclutils.write_wcl(wrapperwcl)
        
        translate_sw_iw(config, wrapperwcl, modname, inst)
        add_needed_values(config, modname, inst, wrapperwcl)
        write_wrapper_wcl(config, inst['inputwcl'], wrapperwcl) 

        (exists, val) = config.search(SW_WRAPPER_DEBUG, {PF_CURRVALS: {'curr_module': modname}})
        if exists:
            inst['wrapdebug'] = val
        else:
            inst['wrapdebug'] = 0

        ####tasks.append([inst[PF_WRAPNUM], wrappername, inputwcl, wrapdebug, logfile])
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    ####return tasks



#######################################################################
def divide_into_jobs(config, modname, wrapinst, joblist):
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    if SW_DIVIDE_JOBS_BY not in config and len(joblist) > 1:
        fwdie("Error: no %s in config, but already > 1 job" % SW_DIVIDE_JOBS_BY)

    fwdebug(0, "PFWBLOCK_DEBUG", "number of wrapinst = %s" % len(wrapinst))

    for inst in wrapinst.values():
        #wclutils.write_wcl(inst, sys.stdout, True, 4)
        #print "inputwcl =", inst['inputwcl']
        #print "logfile =", inst['log']
        key = '_nokey'
        if SW_DIVIDE_JOBS_BY in config:
            #print "divide_jobs_by:", config[SW_DIVIDE_JOBS_BY]
            key = ""
            for divb in fwsplit(config[SW_DIVIDE_JOBS_BY], ','):
                key += "_"+config.get(divb, None, {PF_CURRVALS: {'curr_module':modname}, 'searchobj': inst, 'interpolate': True, 'required':True})
                
        #print "inst key =", key
        if key not in joblist:
            joblist[key] = {'tasks':[], 'inlist':[], 'wrapinputs':{}}
        joblist[key]['tasks'].append([inst[PF_WRAPNUM], inst['wrappername'], inst['inputwcl'], inst['wrapdebug'], inst['log']])
        joblist[key]['inlist'].append(inst['inputwcl'])
        #print inst[PF_WRAPNUM], inst['wrapinputs'].keys()
        if inst['wrapinputs'] is not None and len(inst['wrapinputs']) > 0:
            joblist[key]['wrapinputs'][inst[PF_WRAPNUM]] = inst['wrapinputs']
        if IW_LISTSECT in inst:
            for linfo in inst[IW_LISTSECT].values():
                joblist[key]['inlist'].append(linfo['fullname'])

    fwdebug(0, "PFWBLOCK_DEBUG", "number of job lists = %s " % len(joblist.keys()))
    fwdebug(0, "PFWBLOCK_DEBUG", "\tkeys = %s " % ','.join(joblist.keys()))
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n")
            

def write_runjob_script(config):
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")

    jobdir = config.get_filepath('runtime', 'jobdir', {PF_CURRVALS: {PF_JOBNUM:"$padjnum"}})
    print "The jobdir =", jobdir

    #      Since wcl's variable syntax matches shell variable syntax and 
    #      underscores are used to separate name parts, have to use place 
    #      holder for jobnum and replace later with shell variable
    #      Otherwise, get_filename fails to substitute for padjnum
    envfile = config.get_filename('envfile', {PF_CURRVALS: {PF_JOBNUM:"9999"}})
    envfile = envfile.replace("j9999", "j${padjnum}")

    scriptstr = """#!/bin/sh
echo "Current args: $@";
if [ $# -ne 6 ]; then
    echo "Usage: <jobnum> <input tar> <job wcl> <tasklist> <env file> <output tar>";
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
echo ""
echo "Cmdline given: " $@
echo ""
echo ""
echo -n "job shell script starttime: " 
/bin/date
echo -n "job exec host: "
/bin/hostname
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
setup --nolock %(pipe)s %(ver)s
mystat=$?
d2=`/bin/date "+%%s"` 
echo "\t$((d2-d1)) secs"
echo "DESDMTIME: eups_setup $((d2-d1))"
if [ $mystat != 0 ]; then
    echo "Error: eups setup had non-zero exit code ($mystat)"
    exit $mystat 
fi
""" % ({'eups': config['setupeups'], 
        'pipe':config['pipeprod'],
        'ver':config['pipever']})

    # add any job environment from submit wcl
    scriptstr += 'echo ""\n'
    if SW_JOB_ENVIRONMENT in config:
        for name,value in config[SW_JOB_ENVIRONMENT].items():
            scriptstr += 'export %s="%s"\n' % (name.upper(), value)
    scriptstr += 'echo ""\n'


    # print start of job information 

    scriptstr +="""
echo "Saving environment after setting up meta package to $envfile"
env | sort > $envfile
pwd
ls -l $envfile
""" 
   
    if SW_JOB_BASE_DIR in config and config[SW_JOB_BASE_DIR] is not None:
        full_job_dir = config[SW_JOB_BASE_DIR] + '/' + jobdir
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
        print "%s wasn't specified.   Running job in condor job directory" % SW_JOB_BASE_DIR

    # untar file containing input wcl files
    scriptstr += """
echo ""
echo "Untaring input tar: $intar"
d1=`/bin/date "+%s"` 
tar -xzf $initdir/$intar
d2=`/bin/date "+%s"` 
echo "\t$((d2-d1)) secs"
echo "DESDMTIME: untar_input_tar $((d2-d1))"
"""

    # copy files so can test by hand after job
    # save initial directory to job wcl file
    scriptstr += """
echo "Copying job wcl and task list to job working directory"
d1=`/bin/date "+%s"`
cp $initdir/$jobwcl $jobwcl
cp $initdir/$tasklist $tasklist
d2=`/bin/date "+%s"`
echo "DESDMTIME: copy_jobwcl_tasklist $((d2-d1))"
echo "condor_job_init_dir = " $initdir >> $jobwcl
"""

    # call the job workflow program
    scriptstr += """
echo ""
echo "Calling pfwrunjob.py"
echo "cmd> ${PROCESSINGFW_DIR}/libexec/pfwrunjob.py --config $jobwcl $tasklist"
d1=`/bin/date "+%s"`
${PROCESSINGFW_DIR}/libexec/pfwrunjob.py --config $jobwcl $tasklist
rjstat=$?
d2=`/bin/date "+%s"`
echo "DESDMTIME: pfwrunjob.py $((d2-d1))"
shd2=`/bin/date "+%s"`
echo "DESDMTIME: job_shell_script $((shd2-shd1))"
echo "Exiting with status $rjstat"
exit $rjstat
""" 

    # write shell script to file
    scriptfile = config.get_filename('runjob') 
    with open(scriptfile, 'w') as scriptfh:
        scriptfh.write(scriptstr)

    os.chmod(scriptfile, stat.S_IRWXU | stat.S_IRWXG)

    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return scriptfile



#######################################################################
def create_jobmngr_dag(config, dagfile, scriptfile, joblist):
    """ Write job manager DAG file """

    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    config['numjobs'] = len(joblist)
    condorfile = create_runjob_condorfile(config, scriptfile)

    pfwdir = config['processingfw_dir']
    blockname = config['curr_block']


    with open("../%s/%s" % (blockname, dagfile), 'w') as dagfh:
        for jobkey,jobdict in joblist.items(): 
            jobnum = jobdict['jobnum']
            tjpad = "%04d" % (int(jobnum))

            dagfh.write('JOB %s %s\n' % (tjpad, condorfile))
            dagfh.write('VARS %s jobnum="%s"\n' % (tjpad, tjpad))
            dagfh.write('VARS %s exec="%s"\n' % (tjpad, scriptfile))
            dagfh.write('VARS %s args="%s %s %s %s %s %s"\n' % (tjpad, jobnum, jobdict['inputwcltar'], jobdict['jobwclfile'], jobdict['tasksfile'], jobdict['envfile'], jobdict['outputwcltar']))
            dagfh.write('VARS %s transinput="%s,%s,%s"\n' % (tjpad, jobdict['inputwcltar'], jobdict['jobwclfile'], jobdict['tasksfile']))
            dagfh.write('VARS %s transoutput="%s,%s"\n' % (tjpad, jobdict['outputwcltar'], jobdict['envfile']))
            # no pre script for job.   Job inserted into DB at beginning of job running
#jobpost.py configfile block jobnum inputtar outputtar retval
            dagfh.write('SCRIPT post %s %s/libexec/jobpost.py config.des %s $JOB %s %s $RETURN\n' % (tjpad, pfwdir, blockname, jobdict['inputwcltar'], jobdict['outputwcltar'])) 

    uberdagfile = "../uberctrl/%s" % (dagfile)
    if os.path.exists(uberdagfile):
        os.unlink(uberdagfile)
    os.symlink("../%s/%s" % (blockname, dagfile), uberdagfile)

#    pfwcondor.add2dag(dagfile, config.get_dag_cmd_opts(), config.get_condor_attributes("jobmngr"), None, sys.stdout)
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def tar_inputfiles(config, jobnum, inlist):
    """ Tar the input wcl files for a single job """
    inputtar = config.get_filename('inputwcltar', {PF_CURRVALS:{'jobnum': jobnum}})
    pfwutils.tar_list(inputtar, inlist)
    return inputtar


#######################################################################
def create_runjob_condorfile(config, scriptfile):
    """ Write runjob condor description file for target job """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")

    blockbase = config.get_filename('block', {PF_CURRVALS: {'flabel': 'runjob', 'fsuffix':''}})
#    initialdir = "../%s_tjobs" % config['blockname']
    initialdir = "../%s" % config['blockname']
#    condorfile = '%s/%scondor' % (initialdir, condorbase)

    #condorfile = '%scondor' % (blockbase)
    condorfile = '../%s/%scondor' % (config['blockname'], blockbase)
    
    jobbase = config.get_filename('job', {PF_CURRVALS: {PF_JOBNUM:'$(jobnum)', 'flabel': 'runjob', 'fsuffix':''}})
    jobattribs = { 
                'executable':'../%s/%s' % (config['blockname'], scriptfile), 
                'arguments':'$(args)',
#               'remote_initialdir':remote_initialdir, 
                'initialdir':initialdir,
#               'transfer_output_files': '$(jobnum).pipeline.log',
#                'should_transfer_files': 'IF_NEEDED',
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


#    jobattribs.update(config.get_grid_info())
    userattribs = config.get_condor_attributes('$(jobnum)')
    targetinfo = config.get_grid_info()
    print "targetinfo=",targetinfo
    if 'gridtype' not in targetinfo:
        fwdie("Error:  Missing gridtype", PF_EXIT_FAILURE)
    else:
        targetinfo['gridtype'] = targetinfo['gridtype'].lower()
        print 'GRIDTYPE =', targetinfo['gridtype']

    reqs = []
    if targetinfo['gridtype'] == 'condor':
        jobattribs['universe'] = 'vanilla'

        if 'concurrency_limits' in config:
            jobattribs['concurrency_limits'] = config['concurrency_limits']

        if 'batchtype' not in targetinfo:
            fwdie("Error: Missing batchtype", PF_EXIT_FAILURE)
        else:
            targetinfo['batchtype'] = targetinfo['batchtype'].lower()

        if targetinfo['batchtype'] == 'glidein':
            if 'uiddomain' not in config:
                fwdie("Error: Cannot determine uiddomain for matching to a glidein", PF_EXIT_FAILURE)
            reqs.append('(UidDomain == "%s")' % config['uiddomain'])
            if 'glidein_name' in config and config['glidein_name'].lower() != 'none':
                reqs.append('(GLIDEIN_NAME == "%s")' % config.interpolate(config['glidein_name']))

            reqs.append('(FileSystemDomain != "")')
            reqs.append('(Arch != "")')
            reqs.append('(OpSys != "")')
            reqs.append('(Disk != -1)')
            reqs.append('(Memory != -1)')
    
            if 'glidein_use_wall' in config and convertBool(config['glidein_use_wall']):
                reqs.append("(TimeToLive > \$(wall)*60)")   # wall is in mins, TimeToLive is in secs

        elif targetinfo['batchtype'] == 'local':
            jobattribs['universe'] = 'vanilla'
            if 'loginhost' in config:
                machine = config['loginhost']
            elif 'gridhost' in config:
                machine = config['gridhost']
            else:
                fwdie("Error:  Cannot determine machine name (missing loginhost and gridhost)", PF_EXIT_FAILURE)

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

    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return condorfile



#######################################################################
def stage_inputs(config, inputfiles):
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    fwdebug(0, "PFWBLOCK_DEBUG", "number of input files needed at target = %s" % len(inputfiles))
    fwdebug(6, "PFWBLOCK_DEBUG", "input files %s" % inputfiles)

    if (USE_HOME_ARCHIVE_INPUT in config and 
        (config[USE_HOME_ARCHIVE_INPUT].lower() == TARGET_ARCHIVE or
        config[USE_HOME_ARCHIVE_INPUT].lower() == 'all')):

        fwdebug(0, "PFWBLOCK_DEBUG", "home_archive = %s" % config[HOME_ARCHIVE])
        fwdebug(0, "PFWBLOCK_DEBUG", "target_archive = %s" % config[TARGET_ARCHIVE])
        sys.stdout.flush()
        archive_transfer_utils.archive_copy(config['archive'][config[HOME_ARCHIVE]], 
                                            config['archive'][config[TARGET_ARCHIVE]],
                                            config['archive_transfer'],
                                            inputfiles, config)

    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def write_output_list(config, outputfiles):
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    fwdebug(0, "PFWBLOCK_DEBUG", "output files %s" % outputfiles)

    if 'block_outputlist' not in config:
        fwdie("Error:  Could not find block_outputlist in config.   Internal Error.", PF_EXIT_FAILURE)

    with open(config['block_outputlist'], 'w') as fh:
        for f in outputfiles:
            fh.write("%s\n" % parse_fullname(f, 2))
    
    fwdebug(0, "PFWBLOCK_DEBUG", "END")
    
