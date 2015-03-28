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
import json

import processingfw.pfwdefs as pfwdefs
import despymisc.miscutils as miscutils
import intgutils.metadefs as imetadefs
import despydmdb.dbsemaphore as dbsem
import filemgmt.archive_transfer_utils as archive_transfer_utils
import intgutils.wclutils as wclutils
import intgutils.intgdefs as intgdefs
import intgutils.metautils as imetautils
import processingfw.pfwutils as pfwutils
import processingfw.pfwcondor as pfwcondor
import intgutils.queryutils as queryutils
from processingfw.pfwwrappers import write_wrapper_wcl

#######################################################################
def add_runtime_path(config, currvals, fname, finfo, filename):
    """ Add runtime path to filename """

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "creating path for %s" % fname)
    miscutils.fwdebug(6, "PFWBLOCK_DEBUG", "finfo = %s" % finfo)

    path = config.get_filepath('runtime', None, {pfwdefs.PF_CURRVALS: currvals,
                                                 'searchobj': finfo,
                                                 'interpolate': True,
                                                 'expand': True})

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "\tpath = %s" % path)

    #filename = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals,
    #                                      'searchobj': finfo,
    #                                      'interpolate': True,
    #                                      'expand': True})

    cmpext = ''
    if ('compression' in finfo and
         finfo['compression'] is not None and
         finfo['compression'] != 'None'):
        #print "compression: %s, %s" % (finfo['compression'], type(finfo['compression']))
        cmpext = finfo['compression']

    fullname = None
    if type(filename) is list:
        fullname = []
        miscutils.fwdebug(3, "PFWBLOCK_DEBUG",
                          "%s has multiple names, number of names = %s" % (fname, len(filename)))
        for name in filename:
            miscutils.fwdebug(6, "PFWBLOCK_DEBUG", "path + filename = %s/%s" % (path, name))
            fullname.append("%s/%s%s" % (path, name, cmpext))
    else:
        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "Adding path to filename for %s" % filename)
        fullname = ["%s/%s%s" % (path, filename, cmpext)]
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END fullname = %s" % fullname)
    return fullname


#######################################################################
def create_simple_list(config, lname, ldict, currvals):
    """ Create simple filename list file based upon patterns """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    listname = config.search('listname',
                            {pfwdefs.PF_CURRVALS: currvals,
                             'searchobj': ldict,
                             'required': True,
                             'interpolate': True})[1]

    filename = config.get_filename(None,
                            {pfwdefs.PF_CURRVALS: currvals,
                             'searchobj': ldict,
                             'required': True,
                             'expand': True,
                             'interpolate': False})

    pfwutils.search_wcl_for_variables(config)


    if type(filename) is list:
        listcontents = '\n'.join(filename)
    else:
        listcontents = filename

    listdir = os.path.dirname(listname)
    if len(listdir) > 0 and not os.path.exists(listdir):
        miscutils.coremakedirs(listdir)

    with open(listname, 'w', 0) as listfh:
        listfh.write(listcontents+"\n")
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


###########################################################
def create_simple_sublist(config, moddict, lname, ldict, currvals):
    """ create a simple sublist of files for a list without query """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")

    # grab file section names from columns value in list def
    filesects = {}
    if 'columns' in ldict:
        columns = miscutils.fwsplit(ldict['columns'].lower(), ',')
        for col in columns:
            filesects[col.split('.')[0]] = True

    if len(filesects) > 1:
        miscutils.fwdie('The framework currently does not support multiple file-column lists without query', pfwdefs.PF_EXIT_FAILURE)

    fname = filesects.keys()[0]
    finfo = moddict[pfwdefs.SW_FILESECT][fname]

    #filename = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals,
    #                                      'searchobj': finfo,
    #                                      'interpolate': False,
    #                                      'expand': False})

    searchopts = {pfwdefs.PF_CURRVALS: currvals,
                  'searchobj': finfo,
                  'interpolate': False,
                  'expand': False}

    # first check for filename pattern override
    (found, filenamepat) = config.search('filename', searchopts)
    if not found:
        # get filename pattern from global settings:
        (found, filepat) = config.search(pfwdefs.SW_FILEPAT, searchopts)

    if not found:
        miscutils.fwdie("Error: Could not find file pattern %s" % pfwdefs.SW_FILEPAT,
                        pfwdefs.PF_EXIT_FAILURE)

    if pfwdefs.SW_FILEPATSECT not in config:
        wclutils.write_wcl(config)
        miscutils.fwdie("Error: Could not find filename pattern section (%s)" % \
                        pfwdefs.SW_FILEPATSECT, pfwdefs.PF_EXIT_FAILURE)
    elif filepat in config[pfwdefs.SW_FILEPATSECT]:
        filenamepat = config[pfwdefs.SW_FILEPATSECT][filepat]
    else:
        print pfwdefs.SW_FILEPATSECT, " keys: ", config[pfwdefs.SW_FILEPATSECT].keys()
        miscutils.fwdie("Error: Could not find filename pattern for %s" % filepat,
                        pfwdefs.PF_EXIT_FAILURE, 2)

    # get list of pairs (filename, filedict) by expanding variables in the filename pattern
    filepairs = config.interpolateKeep(filenamepat, {pfwdefs.PF_CURRVALS: currvals,
                                          'searchobj': finfo,
                                          'interpolate': True,
                                          'expand': True})

    # convert to same format as if read from file created by query
    filelist_wcl = None
    if len(filepairs) > 0:
        miscutils.fwdebug(6, "PFWBLOCK_DEBUG", "filepairs = %s" % str(filepairs))
        filedict_list = []
        for pair in filepairs:
            miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "pair = %s" % str(pair))
            file1 = pair[1]
            file1['filename'] = pair[0]

            # merge particular file information with file definition
            sinfo = copy.deepcopy(finfo)
            sinfo.update(file1)

            file1['fullname'] = add_runtime_path(config, currvals, fname, sinfo, pair[0])[0]
            filedict_list.append(file1)
        filelist_wcl = queryutils.convert_single_files_to_lines(filedict_list)
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END")
    return filelist_wcl


#######################################################################
def get_match_keys(sdict):
    """ Get keys on which to match files """
    mkeys = []

    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "keys in sdict: %s " % sdict.keys())
    if 'loopkey' in sdict:
        mkeys = miscutils.fwsplit(sdict['loopkey'].lower())
        mkeys.sort()
    elif 'match' in sdict:
        mkeys = miscutils.fwsplit(sdict['match'].lower())
        mkeys.sort()
    elif 'divide_by' in sdict:
        mkeys = miscutils.fwsplit(sdict['divide_by'].lower())
        mkeys.sort()

    return mkeys


#######################################################################
def find_sublist(objdef, objinst):
    """ Find sublist """

    if len(objdef['sublists'].keys()) > 1:
        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "sublist keys: %s" % (objdef['sublists'].keys()))
        matchkeys = get_match_keys(objdef)
        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "matchkeys: %s" % (matchkeys))
        index = ""
        for mkey in matchkeys:
            if mkey not in objinst:
                miscutils.fwdie("Error: Cannot find match key %s in inst %s" % (mkey, objinst),
                                pfwdefs.PF_EXIT_FAILURE)
            index += objinst[mkey] + '_'
        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "sublist index = "+index)
        if index not in objdef['sublists']:
            miscutils.fwdie("Error: Cannot find sublist matching "+index, pfwdefs.PF_EXIT_FAILURE)
        sublist = objdef['sublists'][index]
    else:
        sublist = objdef['sublists'].values()[0]

    return sublist

#######################################################################
def which_are_inputs(config, modname):
    """ Return dict of files/lists that are inputs for given module """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    inputs = {pfwdefs.SW_FILESECT: [], pfwdefs.SW_LISTSECT: []}
    outfiles = {}

    # For wrappers with more than 1 exec section, the inputs of one exec can
    #     be the inputs of a 2nd exec the framework should not attempt to stage
    #     these intermediate files
    execs = pfwutils.get_exec_sections(config[pfwdefs.SW_MODULESECT][modname],
                                       pfwdefs.SW_EXECPREFIX)
    for ekey, einfo in sorted(execs.items()):
        if pfwdefs.SW_OUTPUTS in einfo:
            for outfile in miscutils.fwsplit(einfo[pfwdefs.OW_OUTPUTS]):
                outfiles[outfile] = True

        if pfwdefs.SW_INPUTS in einfo:
            inarr = miscutils.fwsplit(einfo[pfwdefs.SW_INPUTS].lower())
            for inname in inarr:
                if inname not in outfiles:
                    parts = miscutils.fwsplit(inname, '.')
                    inputs[parts[0]].append('.'.join(parts[1:]))

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", inputs)
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END")
    return inputs


#######################################################################
def which_are_outputs(config, modname):
    """ Return dict of files that are outputs for given module """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    outfiles = {}

    execs = pfwutils.get_exec_sections(config[pfwdefs.SW_MODULESECT][modname],
                                       pfwdefs.SW_EXECPREFIX)
    for ekey, einfo in sorted(execs.items()):
        if pfwdefs.SW_OUTPUTS in einfo:
            for outfile in miscutils.fwsplit(einfo[pfwdefs.OW_OUTPUTS]):
                parts = miscutils.fwsplit(outfile, '.')
                outfiles['.'.join(parts[1:])] = True

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", outfiles.keys())
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END")
    return outfiles.keys()





#######################################################################
def assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict,
                                currvals, winst, fname, finfo, is_iter_obj=False):
    """ Assign files to wrapper instance """



    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "BEG: Working on file %s" % fname)
    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "theinputs: %s" % theinputs)
    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "outputs: %s" % theoutputs)

    if 'listonly' in finfo and miscutils.convertBool(finfo['listonly']):
        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "Skipping %s due to listonly key" % fname)
        return

    if pfwdefs.IW_FILESECT not in winst:
        winst[pfwdefs.IW_FILESECT] = {}

    winst[pfwdefs.IW_FILESECT][fname] = {}
    if 'sublists' in finfo:  # files came from query
        sublist = find_sublist(finfo, winst)
        if len(sublist['list'][intgdefs.LISTENTRY]) > 1:
            miscutils.fwdie("Error: more than 1 line to choose from for file (%s)" % \
                            sublist['list'][intgdefs.LISTENTRY], pfwdefs.PF_EXIT_FAILURE)
        line = sublist['list'][intgdefs.LISTENTRY].values()[0]
        if 'file' not in line:
            miscutils.fwdie("Error: 0 file in line" + str(line), pfwdefs.PF_EXIT_FAILURE)

        if len(line['file']) > 1:
            raise Exception("more than 1 file to choose from for file" + line['file'])
        finfo = line['file'].values()[0]
        miscutils.fwdebug(6, "PFWBLOCK_DEBUG", "finfo = %s" % finfo)

        fullname = finfo['fullname']
        winst[pfwdefs.IW_FILESECT][fname]['fullname'] = fullname

        # save input and output filenames (with job scratch path)
        # In order to preserve capitalization, put on right side of =,
        #    using dummy count for left side
        if fname in theinputs[pfwdefs.SW_FILESECT]:
            winst['wrapinputs'][len(winst['wrapinputs'])+1] = fullname
        elif fname in theoutputs:
            winst['wrapoutputs'][len(winst['wrapoutputs'])+1] = fullname

        miscutils.fwdebug(6, "PFWBLOCK_DEBUG", "Assigned filename for fname %s (%s)" % \
                          (fname, finfo['filename']))
    elif 'fullname' in moddict[pfwdefs.SW_FILESECT][fname]:
        winst[pfwdefs.IW_FILESECT][fname]['fullname'] = moddict[pfwdefs.SW_FILESECT][fname]['fullname']
        miscutils.fwdebug(6, "PFWBLOCK_DEBUG", "Copied fullname for %s = %s" % \
                          (fname, winst[pfwdefs.IW_FILESECT][fname]))
        if fname in theinputs[pfwdefs.SW_FILESECT]:
            winst['wrapinputs'][len(winst['wrapinputs'])+1] = moddict[pfwdefs.SW_FILESECT][fname]['fullname']
        elif fname in theoutputs:
            winst['wrapoutputs'][len(winst['wrapoutputs'])+1] = moddict[pfwdefs.SW_FILESECT][fname]['fullname']
    else:
        sobj = copy.deepcopy(winst)
        sobj.update(finfo)   # order matters file values must override winst values
        if 'filename' in moddict[pfwdefs.SW_FILESECT][fname]:
            miscutils.fwdebug(6, "PFWBLOCK_DEBUG", "filename in %s" % fname)
        
            winst[pfwdefs.IW_FILESECT][fname]['filename'] = config.search('filename', {pfwdefs.PF_CURRVALS: currvals,
                                                                          'searchobj': moddict[pfwdefs.SW_FILESECT][fname],
                                                                          'expand': True,
                                                                          'required': True,
                                                                          'interpolate':True})[1]
            miscutils.fwdebug(6, "PFWBLOCK_DEBUG", "filename = %s" % winst[pfwdefs.IW_FILESECT][fname]['filename'])
        else:
            miscutils.fwdebug(6, "PFWBLOCK_DEBUG", "creating filename for %s" % fname)
            miscutils.fwdebug(6, "PFWBLOCK_DEBUG", "\tsobj = %s" % sobj)
            winst[pfwdefs.IW_FILESECT][fname]['filename'] = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals,
                                                                'searchobj': sobj,
                                                                'expand': True,
                                                                'interpolate': True})

        # Add runtime path to filename
        fullname = add_runtime_path(config, currvals, fname, sobj, winst[pfwdefs.IW_FILESECT][fname]['filename'])
        if fname in theinputs[pfwdefs.SW_FILESECT]:
            for name in fullname:
                winst['wrapinputs'][len(winst['wrapinputs'])+1] = name
        elif fname in theoutputs:
            for name in fullname:
                winst['wrapoutputs'][len(winst['wrapinputs'])+1] = name

        winst[pfwdefs.IW_FILESECT][fname]['fullname'] = ', '.join(fullname)
        del winst[pfwdefs.IW_FILESECT][fname]['filename']

    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "is_iter_obj = %s %s" % (is_iter_obj, finfo))
    if finfo is not None and is_iter_obj:
        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "is_iter_obj = true")
        for key, val in finfo.items():
            if key not in ['fullname', 'filename', 'dirpat', 'filetype', 'compression']:
                miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "is_iter_obj: saving %s" % key)
                winst[key] = val

    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "END: Done working on file %s" % fname)



#######################################################################
def assign_list_to_wrapper_inst(config, moddict, currvals, winst, lname, ldict):
    """ Assign list to wrapper instance """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG: Working on list %s from %s" % (lname, moddict['modulename']))
    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "currvals = %s" % (currvals))
    miscutils.fwdebug(6, "PFWBLOCK_DEBUG", "ldict = %s" % (ldict))

    if pfwdefs.IW_LISTSECT not in winst:
        winst[pfwdefs.IW_LISTSECT] = {}

    winst[pfwdefs.IW_LISTSECT][lname] = {}

    ### create an object that has values from ldict and winst

    # don't deepcopy sublists
    savesublists = None
    if 'sublists' in ldict:
        savesublists = ldict['sublists']
        ldict['sublists'] = None

    sobj = copy.deepcopy(ldict)

    # put sublists back after deepcopy
    if savesublists is not None:
        ldict['sublists'] = savesublists

    sobj.update(winst)
    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "sobj = %s" % (sobj))

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "creating listdir and listname")

    # list dir and filename must use current attempt values
    currvals2 = copy.deepcopy(currvals)
    currvals2[pfwdefs.REQNUM] = config[pfwdefs.REQNUM]
    currvals2[pfwdefs.UNITNAME] = config[pfwdefs.UNITNAME]
    currvals2[pfwdefs.ATTNUM] = config[pfwdefs.ATTNUM]

    listdir = config.get_filepath('runtime', 'list', {pfwdefs.PF_CURRVALS: currvals2,
                         'required': True, 'interpolate': True,
                         'searchobj': sobj})

    listname = config.get_filename(None, {pfwdefs.PF_CURRVALS: currvals2,
                                   'searchobj': sobj, 'required': True, 'interpolate': True})
    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "listname = %s" % (listname))
    listname = "%s/%s" % (listdir, listname)

    winst[pfwdefs.IW_LISTSECT][lname]['fullname'] = listname
    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "full listname = %s" % (winst[pfwdefs.IW_LISTSECT][lname]['fullname']))

    sublist = None
    if 'sublists' not in ldict:
        sublist = create_simple_sublist(config, moddict, lname, ldict, currvals)
    else:
        sublist = find_sublist(ldict, winst)


    if sublist is not None:
        for llabel, lldict in sublist['list'][intgdefs.LISTENTRY].items():
            for flabel, fdict in lldict['file'].items():
                winst['wrapinputs'][len(winst['wrapinputs'])+1] = fdict['fullname']
        output_list(config, winst[pfwdefs.IW_LISTSECT][lname]['fullname'], sublist, lname, ldict, currvals)
    else:
        print "Warning: Couldn't find files to put in list %s in %s" % (lname, moddict['modulename'])

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END")




#######################################################################
def assign_data_wrapper_inst(config, modname, wrapperinst):
    """ Assign data like files and lists to wrapper instances """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    moddict = config[pfwdefs.SW_MODULESECT][modname]
    currvals = {'curr_module': modname}
    (found, loopkeys) = config.search('wrapperloop',
                       {pfwdefs.PF_CURRVALS: currvals,
                        'required': False, 'interpolate': True})
    if found:
        loopkeys = miscutils.fwsplit(loopkeys.lower())
    else:
        loopkeys = []

    # figure out which lists/files are input files
    theinputs = which_are_inputs(config, modname)
    theoutputs = which_are_outputs(config, modname)

    for winst in wrapperinst.values():
        winst['wrapinputs'] = {}
        winst['wrapoutputs'] = {}

        # create currvals
        currvals = {'curr_module': modname, pfwdefs.PF_WRAPNUM: winst[pfwdefs.PF_WRAPNUM]}
        for key in loopkeys:
            currvals[key] = winst[key]
        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "currvals " + str(currvals))

        # do wrapper loop object first, if exists, to provide keys for filenames
        iter_obj_key = get_wrap_iter_obj_key(config, moddict)

        if iter_obj_key is not None or pfwdefs.SW_FILESECT in moddict:
            miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "%s: Assigning files to wrapper inst" % winst[pfwdefs.PF_WRAPNUM])

        if iter_obj_key is not None:
            (iter_obj_sect, iter_obj_name) = miscutils.fwsplit(iter_obj_key, '.')
            iter_obj_dict = pfwutils.get_wcl_value(iter_obj_key, moddict)
            miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "iter_obj %s %s" % (iter_obj_name, iter_obj_sect))
            if iter_obj_sect.lower() == pfwdefs.SW_FILESECT.lower():
                assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals, winst, iter_obj_name, iter_obj_dict, True)
            elif iter_obj_sect.lower() == pfwdefs.SW_LISTSECT.lower():
                assign_list_to_wrapper_inst(config, moddict, currvals, winst, iter_obj_name, iter_obj_dict)
            else:
                miscutils.fwdie("Error: unknown iter_obj_sect (%s)" % iter_obj_sect, pfwdefs.PF_EXIT_FAILURE)


        if pfwdefs.SW_FILESECT in moddict:
            for fname, fdict in moddict[pfwdefs.SW_FILESECT].items():
                if iter_obj_key is not None and \
                   iter_obj_sect.lower() == pfwdefs.SW_FILESECT.lower() and \
                   iter_obj_name.lower() == fname.lower():
                    continue    # already did iter_obj
                assign_file_to_wrapper_inst(config, theinputs, theoutputs, moddict, currvals, winst, fname, fdict)

        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "currvals " + str(currvals))

        if pfwdefs.SW_LISTSECT in moddict:
            miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "%s: Assigning lists to wrapper inst" % winst[pfwdefs.PF_WRAPNUM])
            for lname, ldict in moddict[pfwdefs.SW_LISTSECT].items():
                if iter_obj_key is not None and \
                   iter_obj_sect.lower() == pfwdefs.SW_LISTSECT.lower() and \
                   iter_obj_name.lower() == lname.lower():
                    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "skipping list %s as already did for it as iter_obj" % lname)
                    continue    # already did iter_obj
                assign_list_to_wrapper_inst(config, moddict, currvals, winst, lname, ldict)
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def output_list(config, listname, sublist, lname, ldict, currvals):
    """ Output list """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG: %s (%s)" % (lname, listname))
    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "list dict: %s" % ldict)

    listdir = os.path.dirname(listname)
    miscutils.coremakedirs(listdir)

    lineformat = 'textsp'
    if 'format' in ldict:
        lineformat = ldict['format']

    if 'columns' in ldict:
        columns = ldict['columns'].lower()
    else:
        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "columns not in ldict, so defaulting to fullname")
        columns = 'fullname'
    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "columns = %s" % columns)


    lines = sublist['list'][intgdefs.LISTENTRY].values()
    if 'sortkey' in ldict and ldict['sortkey'] is not None:
        # (key, numeric, reverse)
        sort_reverse = False
        sort_numeric = False
        
        if ldict['sortkey'].strip().startswith('('):
            rmatch = re.match('\(([^)]+)', ldict['sortkey'])
            if rmatch:
                sortinfo = miscutils.fwsplit(rmatch.group(1))
                sort_key = sortinfo[0]
                if len(sortinfo) > 1:
                    sort_numeric = miscutils.convertBool(sortinfo[1])
                if len(sortinfo) > 2:
                    sort_reverse = miscutils.convertBool(sortinfo[2])
            else:
                miscutils.fwdie("Error: problems parsing sortkey...\n%s" % (ldict['sortkey']), pfwdefs.PF_EXIT_FAILURE)
        else:
            sort_key = ldict['sortkey']

        sort_key = sort_key.lower()

        if sort_numeric:
            lines = sorted(lines, reverse=sort_reverse, key=lambda k: float(get_value_from_line(k, sort_key, None, 1)))
        else:
            lines = sorted(lines, reverse=sort_reverse, key=lambda k: get_value_from_line(k, sort_key, None, 1))

    allow_missing = False
    if 'allow_missing' in ldict:
        allow_missing = miscutils.convertBool(ldict['allow_missing'])
        

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "Writing list to file %s" % listname)
    with open(listname, "w") as listfh:
        for linedict in lines:
            output_line(listfh, linedict, lineformat, allow_missing, miscutils.fwsplit(columns))
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")




#####################################################################
def output_line(listfh, line, lineformat, allow_missing, keyarr):
    """ output line into input list for science code"""
    miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "BEG line=%s  keyarr=%s" % (line, keyarr))

    lineformat = lineformat.lower()

    if lineformat == 'config' or lineformat == 'wcl':
        listfh.write("<file>\n")

    numkeys = len(keyarr)
    for i in range(0, numkeys):
        key = keyarr[i]
        value = None
        miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "key: %s" % key)

        if '.' in  key:
            miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "Found period in key")
            [nickname, key2] = key.replace(' ', '').split('.')
            miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "\tnickname = %s, key2 = %s" % (nickname, key2))
            value = get_value_from_line(line, key2, nickname, None)
            if value == None:
                miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "Didn't find value in line with nickname %s" % (nickname))
                miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "Trying to find %s without nickname" % (key2))
                value = get_value_from_line(line, key2, None, 1)
                if value == None:
                    if allow_missing: 
                        value = ""
                    else:
                        miscutils.fwdie("Error: could not find value %s for line...\n%s" % (key, line), pfwdefs.PF_EXIT_FAILURE)
                else: # assume nickname was really table name
                    miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "\tassuming nickname (%s) was really table name" % (nickname))
                    key = key2
        else:
            value = get_value_from_line(line, key, None, 1)

        # handle last field (separate to avoid trailing comma)
        miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "printing key=%s value=%s" % (key, value))
        if i == numkeys - 1:
            print_value(listfh, key, value, lineformat, True)
        else:
            print_value(listfh, key, value, lineformat, False)

    if lineformat == "config" or lineformat == 'wcl':
        listfh.write("</file>\n")
    else:
        listfh.write("\n")


#####################################################################
def print_value(outfh, key, value, lineformat, last):
    """ output value to input list in correct format """

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s=%s (%s)" % (key,value,type(value)))
    lineformat = lineformat.lower()
    if lineformat == 'config' or lineformat == 'wcl':
        outfh.write("     %s=%s\n" % (key, str(value)))
    else:
        outfh.write(str(value))
        if not last:
            if lineformat == 'textcsv':
                outfh.write(', ')
            elif lineformat == 'texttab':
                outfh.write('\t')
            else:
                outfh.write(' ')



#######################################################################
def finish_wrapper_inst(config, modname, wrapperinst):
    """ Finish creating wrapper instances with tasks like making input and output filenames """

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    moddict = config[pfwdefs.SW_MODULESECT][modname]
    outputfiles = which_are_outputs(config, modname)

    input_filenames = []
    output_filenames = []
    for winst in wrapperinst.values():
        for fname in winst['wrapinputs'].values():
            input_filenames.append(miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME))

        for fname in winst['wrapoutputs'].values():
            output_filenames.append(miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME))



        # create searching options
        currvals = {'curr_module': modname, pfwdefs.PF_WRAPNUM: winst[pfwdefs.PF_WRAPNUM]}
        searchopts = {pfwdefs.PF_CURRVALS: currvals,
                      'searchobj': winst,
                      'interpolate': True,
                      'required': True}


        if pfwdefs.SW_FILESECT in moddict:
            for fname, fdict in moddict[pfwdefs.SW_FILESECT].items():
                if 'listonly' in fdict and miscutils.convertBool(fdict['listonly']):
                    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "Skipping %s due to listonly key" % fname)
                    continue

                miscutils.fwdebug(3, 'PFWBLOCK_DEBUG', '%s: working on file: %s' % (winst[pfwdefs.PF_WRAPNUM], fname))
                miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "fullname = %s" % (winst[pfwdefs.IW_FILESECT][fname]['fullname']))


                for k in ['filetype', imetadefs.WCL_META_REQ, imetadefs.WCL_META_OPT, pfwdefs.SAVE_FILE_ARCHIVE, pfwdefs.DIRPAT]:
                    if k in fdict:
                        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "%s copying %s" % (fname, k))
                        winst[pfwdefs.IW_FILESECT][fname][k] = copy.deepcopy(fdict[k])
                    else:
                        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "%s: no %s" % (fname, k))

                if pfwdefs.SW_OUTPUT_OPTIONAL in fdict:
                    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "%s copying %s " % (fname, pfwdefs.SW_OUTPUT_OPTIONAL))

                    winst[pfwdefs.IW_FILESECT][fname][pfwdefs.IW_OUTPUT_OPTIONAL] = miscutils.convertBool(fdict[pfwdefs.SW_OUTPUT_OPTIONAL])

                hdrups = pfwutils.get_hdrup_sections(fdict, imetadefs.WCL_UPDATE_HEAD_PREFIX)
                for hname, hdict in hdrups.items():
                    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "%s copying %s" % (fname, hname))
                    winst[pfwdefs.IW_FILESECT][fname][hname] = copy.deepcopy(hdict)

                # save OPS path for archive
                miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "Is fname (%s) in outputfiles? %s" % (fname, fname in outputfiles))
                filesave = miscutils.checkTrue(pfwdefs.SAVE_FILE_ARCHIVE, fdict, True)
                miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "Is save_file_archive true? %s" % (filesave))
                mastersave = config[pfwdefs.MASTER_SAVE_FILE]
                if fname in outputfiles:
                    winst[pfwdefs.IW_FILESECT][fname][pfwdefs.SAVE_FILE_ARCHIVE] = filesave  # canonicalize
                    if pfwdefs.DIRPAT not in fdict:
                        print "Warning: Could not find %s in %s's section" % (pfwdefs.DIRPAT, fname)
                    else:
                        searchobj = copy.deepcopy(fdict)
                        searchobj.update(winst)
                        searchopts['searchobj'] = searchobj
                        winst[pfwdefs.IW_FILESECT][fname]['archivepath'] = config.get_filepath('ops',
                                                                        fdict[pfwdefs.DIRPAT], searchopts)

            miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "fdict = %s" % fdict)
            miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "winst[%s] = %s" % (pfwdefs.IW_FILESECT, winst[pfwdefs.IW_FILESECT]))

        if pfwdefs.SW_LISTSECT in moddict:
            for lname, ldict in moddict[pfwdefs.SW_LISTSECT].items():
                for k in ['columns']:
                    if k in ldict:
                        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "%s copying %s" % (lname, k))
                        winst[pfwdefs.IW_LISTSECT][lname][k] = copy.deepcopy(ldict[k])
                    else:
                        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "%s: no %s" % (lname, k))

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


    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return input_filenames, output_filenames


#######################################################################
def add_file_metadata(config, modname):
    """ add file metadata sections to a single file section from a module"""

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "Working on module " + modname)
    moddict = config[pfwdefs.SW_MODULESECT][modname]
    execs = pfwutils.get_exec_sections(moddict, pfwdefs.SW_EXECPREFIX)

    if pfwdefs.SW_FILESECT in moddict:
        filemgmt = None
        try:
            filemgmt_class = miscutils.dynamically_load_class(config['filemgmt'])
            paramdict = config.get_param_info(filemgmt_class.requested_config_vals(),
                                              {pfwdefs.PF_CURRVALS: {'curr_module': modname}})
            filemgmt = filemgmt_class(config=paramdict)
        except:
            print "Error:  Problems dynamically loading class (%s) in order to get metadata specs" % config['filemgmt']
            raise

        for k in execs:
            if pfwdefs.SW_OUTPUTS in moddict[k]:
                for outfile in miscutils.fwsplit(moddict[k][pfwdefs.SW_OUTPUTS]):
                    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "Working on output file " + outfile)
                    match = re.match(r'%s.(\w+)' % pfwdefs.SW_FILESECT, outfile)
                    if match:
                        fname = match.group(1)
                        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "Working on file " + fname)
                        if fname not in moddict[pfwdefs.SW_FILESECT]:
                            msg = "Error: file %s listed in %s, but not defined in %s section" % \
                                (fname, pfwdefs.SW_OUTPUTS, pfwdefs.SW_FILESECT)
                            miscutils.fwdie(msg, pfwdefs.PF_EXIT_FAILURE)

                        fdict = moddict[pfwdefs.SW_FILESECT][fname]
                        filetype = fdict['filetype'].lower()
                        wclsect = "%s.%s" % (pfwdefs.IW_FILESECT, fname)

                        print "len(config[FILE_HEADER_INFO]) =", len(config['FILE_HEADER_INFO'])
                        meta_specs = imetautils.get_metadata_specs(filetype, config['FILETYPE_METADATA'], config['FILE_HEADER'],
                                                        wclsect, updatefits=True)

                        if meta_specs == None:
                            msg = "Error: Could not find metadata specs for filetype '%s'" % filetype
                            print msg
                            print "Minimum metadata specs for a filetype are defs for filetype and filename."
                            miscutils.fwdie("Aborting", pfwdefs.PF_EXIT_FAILURE)
                        miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "meta_specs = %s" % meta_specs)
                        miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "fdict = %s" % fdict)
                        fdict.update(meta_specs)


                        # add descriptions/types to submit-wcl specified updates if missing
                        hdrups = pfwutils.get_hdrup_sections(fdict, imetadefs.WCL_UPDATE_HEAD_PREFIX)
                        for hname, hdict in sorted(hdrups.items()):
                            for key, val in hdict.items():
                                if key != imetadefs.WCL_UPDATE_WHICH_HEAD:
                                    valparts = miscutils.fwsplit(val, '/')
                                    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "hdrup: key, valparts = %s, %s" % (key, valparts))
                                    if len(valparts) == 1:
                                        if 'COPY{' not in valparts[0]:  # wcl specified value, look up rest from config
                                            newvaldict = imetautils.create_update_items('V', [key], config['file_header'], header_value={key:val})
                                            hdict.update(newvaldict)
                                    elif len(valparts) != 3:  # 3 is valid full spec of update header line
                                        miscutils.fwdie('Error:  invalid header update line (%s = %s)\nNeeds value[/descript/type]' % (key, val), pfwdefs.PF_EXIT_FAILURE)


                        # add some fields needed by framework for processing output wcl (not stored in database)
                        if imetadefs.WCL_META_WCL not in fdict[imetadefs.WCL_META_REQ]:
                            fdict[imetadefs.WCL_META_REQ][imetadefs.WCL_META_WCL] = ''
                        else:
                            fdict[imetadefs.WCL_META_REQ][imetadefs.WCL_META_WCL] += ', '

                        fdict[imetadefs.WCL_META_REQ][imetadefs.WCL_META_WCL] += '%(sect)s.fullname,%(sect)s.sectname' % ({'sect':wclsect})
                    else:
                        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "output file %s doesn't have definition (%s) " % (k, pfwdefs.SW_FILESECT))

                miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "output file dictionary for %s = %s" % (outfile, fdict))

            else:
                miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "No was_generated_by for %s" % (k))

    else:
        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "No file section (%s)" % pfwdefs.SW_FILESECT)

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")





#######################################################################
def init_use_archive_info(config, jobwcl, which_use_input, which_use_output, which_archive):
    """ Initialize use archive info """
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
    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "BEG jobnum=%s jobkey=%s" % (jobdict['jobnum'], jobkey))

    jobdict['jobwclfile'] = config.get_filename('jobwcl', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM: jobdict['jobnum']}, 'required': True, 'interpolate': True})
    jobdict['outputwcltar'] = config.get_filename('outputwcltar', {pfwdefs.PF_CURRVALS:{'jobnum': jobdict['jobnum']}, 'required': True, 'interpolate': True})

    jobdict['envfile'] = config.get_filename('envfile')

    jobwcl = {pfwdefs.REQNUM: config.search(pfwdefs.REQNUM, {'required': True,
                                                             'interpolate': True})[1],
              pfwdefs.UNITNAME:config.search(pfwdefs.UNITNAME, {'required': True,
                                                                'interpolate': True})[1],
              pfwdefs.ATTNUM: config.search(pfwdefs.ATTNUM, {'required': True,
                                                             'interpolate': True})[1],
              pfwdefs.PF_BLKNUM: config.search(pfwdefs.PF_BLKNUM, {'required': True,
                                    'interpolate': True})[1],
              pfwdefs.PF_JOBNUM: jobdict['jobnum'],
              'numexpwrap': len(jobdict['tasks']),
              'save_md5sum': config['save_md5sum'],
              'usedb': config.search(pfwdefs.PF_USE_DB_OUT, {'required': True,
                                    'interpolate': True})[1],
              'useqcf': config.search(pfwdefs.PF_USE_QCF, {'required': True,
                                    'interpolate': True})[1],
              'pipeprod': config.search('pipeprod', {'required': True,
                                    'interpolate': True})[1],
              'pipever': config.search('pipever', {'required': True,
                                    'interpolate': True})[1],
              'jobkeys': jobkey[1:].replace('_', ','),
              'archive': config['archive'],
              'output_wcl_tar': jobdict['outputwcltar'],
              'envfile': jobdict['envfile'],
              'junktar': config.get_filename('junktar', {pfwdefs.PF_CURRVALS:{'jobnum': jobdict['jobnum']}}),
              'junktar_archive_path': config.get_filepath('ops', 'junktar', {pfwdefs.PF_CURRVALS:{'jobnum': jobdict['jobnum']}}),
              'task_id': {'attempt': config['task_id']['attempt'],
                          'job':{jobdict['jobnum']: config['task_id']['job'][jobdict['jobnum']]}}
            }

    if pfwdefs.CREATE_JUNK_TARBALL in config and miscutils.convertBool(config[pfwdefs.CREATE_JUNK_TARBALL]):
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
            filemgmt_class = miscutils.dynamically_load_class(config['archive'][target_archive]['filemgmt'])
            valdict = config.get_param_info(filemgmt_class.requested_config_vals())
            jobwcl.update(valdict)
        except Exception as err:
            print "ERROR\nError: creating loading job_file_mvmt class\n%s" % err
            raise

    # include variables needed by home archive's file mgmt class
    if jobwcl[pfwdefs.HOME_ARCHIVE] is not None:
        print "jobwcl[HOME_ARCHIVE] = ", jobwcl[pfwdefs.HOME_ARCHIVE]
        try:
            filemgmt_class = miscutils.dynamically_load_class(config['archive'][home_archive]['filemgmt'])
            valdict = config.get_param_info(filemgmt_class.requested_config_vals(),
                                        {pfwdefs.PF_CURRVALS: config['archive'][home_archive]})
            jobwcl.update(valdict)
        except Exception as err:
            print "ERROR\nError: creating loading job_file_mvmt class\n%s" % err
            raise

    try:
        jobwcl['job_file_mvmt'] = config['job_file_mvmt'][config['curr_site']][home_archive][target_archive]
    except:
        print "\n\n\nError: Problem trying to find: config['job_file_mvmt'][%s][%s][%s]" % (config['curr_site'], home_archive, target_archive)
        print "USE_HOME_ARCHIVE_INPUT =", jobwcl[pfwdefs.USE_HOME_ARCHIVE_INPUT]
        print "USE_HOME_ARCHIVE_OUTPUT =", jobwcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT]
        print "site =", config['curr_site']
        print "home_archive =", home_archive
        print "target_archive =", target_archive
        print 'job_file_mvmt ='
        miscutils.pretty_print_dict(config['job_file_mvmt'])
        print "\n"
        raise

    # include variables needed by job_file_mvmt class
    try:
        jobfilemvmt_class = miscutils.dynamically_load_class(jobwcl['job_file_mvmt']['mvmtclass'])
        valdict = config.get_param_info(jobfilemvmt_class.requested_config_vals(),
                                        {pfwdefs.PF_CURRVALS: jobwcl['job_file_mvmt']})
        jobwcl.update(valdict)
    except Exception as err:
        print "ERROR\nError: creating loading job_file_mvmt class\n%s" % err
        raise


    if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]):
        if 'target_des_services' in config and config['target_des_services'] is not None:
            jobwcl['des_services'] = config['target_des_services']
        jobwcl['des_db_section'] = config['target_des_db_section']


    jobwcl['filetype_metadata'] = config['filetype_metadata']
    jobwcl[pfwdefs.IW_EXEC_DEF] = config[pfwdefs.SW_EXEC_DEF]
    jobwcl['wrapinputs'] = jobdict['wrapinputs']

    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "jobwcl.keys() = %s" % jobwcl.keys())

    tjpad = pfwutils.pad_jobnum(jobdict['jobnum'])
    miscutils.coremakedirs(tjpad)
    with open("%s/%s" % (tjpad, jobdict['jobwclfile']), 'w') as wclfh:
        wclutils.write_wcl(jobwcl, wclfh, True, 4)

    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def add_needed_values(config, modname, wrapinst, wrapwcl):
    """ Make sure all variables in the wrapper instance have values in the wcl """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

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
        for rval in miscutils.fwsplit(config[pfwdefs.SW_MODULESECT][modname]['req_vals']):
            neededvals[rval] = True

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
            miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "nval = %s" % nval)
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


                miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "val = %s" % val)

                neededvals[nval] = val
                viter = [m.group(1) for m in re.finditer(r'(?i)\$\{([^}]+)\}', val)]
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
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def create_wrapper_inst(config, modname, loopvals):
    """ Create set of empty wrapper instances """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    wrapperinst = {}
    (found, loopkeys) = config.search('wrapperloop',
                   {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                    'required': False, 'interpolate': True})
    wrapperinst = {}
    if found:
        miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "loopkeys = %s" % loopkeys)
        loopkeys = miscutils.fwsplit(loopkeys.lower())
        loopkeys.sort()  # sort so can make same key easily

        for instvals in loopvals:
            miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "creating instance for %s" % str(instvals))

            config.inc_wrapnum()
            winst = {pfwdefs.PF_WRAPNUM: config[pfwdefs.PF_WRAPNUM]}

            if len(instvals) != len(loopkeys):
                miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "Error: invalid number of values for instance")
                miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "\t%d loopkeys (%s)" % (len(loopkeys), loopkeys))
                miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "\t%d instvals (%s)" % (len(instvals), instvals))
                raise IndexError("Invalid number of values for instance")

            try:
                instkey = ""
                for k in range(0, len(loopkeys)):
                    winst[loopkeys[k]] = instvals[k]
                    instkey += instvals[k] + '_'
            except:
                miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "Error: problem trying to create wrapper instance")
                miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "\tWas creating instance for %s" % str(instvals))
                miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "\tloopkeys = %s" % loopkeys)
                raise

            wrapperinst[instkey] = winst
    else:
        config.inc_wrapnum()
        wrapperinst['noloop'] = {pfwdefs.PF_WRAPNUM: config[pfwdefs.PF_WRAPNUM]}

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "Number wrapper inst: %s" % len(wrapperinst))
    if len(wrapperinst) == 0:
        miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "Error: 0 wrapper inst")
        raise Exception("Error: 0 wrapper instances")

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return wrapperinst


#####################################################################
def read_master_lists(config, modname, modules_prev_in_list):
    """ Read master lists and files from files created earlier """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    # create python list of files and lists for this module
    searchobj = config.combine_lists_files(modname)

    for (sname, sdict) in searchobj:
        # get filename for file containing dataset
        if 'qoutfile' in sdict:
            qoutfile = sdict['qoutfile']
            print "\t\t%s: reading master dataset from %s" % (sname, qoutfile)

            qouttype = intgdefs.DEFAULT_QUERY_OUTPUT_FORMAT
            if 'qouttype' in sdict:
                qouttype = sdict['qouttype']
            
            # read dataset file
            starttime = time.time()
            print "\t\t\tReading file - start ", starttime
            if qouttype == 'json':
                master = None 
                with open(qoutfile, 'r') as jsonfh:
                    master = json.load(jsonfh)
            elif qouttype == 'xml':
                raise Exception("xml datasets not supported yet")
            elif qouttype == 'wcl':
                with open(qoutfile, 'r') as wclfh:
                    master = wclutils.read_wcl(wclfh, filename=qoutfile)
                    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "master.keys() = " % master.keys())
            else:
                raise Exception("Unsupported dataset format in qoutfile for object %s in module %s (%s) " % (sname, modname, qoutfile))
            endtime = time.time()
            print "\t\t\tReading file - end ", endtime
            print "\t\t\tReading file took %s seconds" % (endtime - starttime)

            numlines = len(master['list'][intgdefs.LISTENTRY])
            print "\t\t\tNumber of lines in dataset %s: %s\n" % (sname, numlines)

            if numlines == 0:
                raise Exception("ERROR: 0 lines in dataset %s in module %s" % (sname, modname))

            sdict['master'] = master

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def create_fullnames(config, modname):
    """ add paths to filenames """    # what about compression extension

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    dataset = config.combine_lists_files(modname)
    moddict = config[pfwdefs.SW_MODULESECT][modname]

    for (sname, sdict) in dataset:
        if 'master' in sdict:
            master = sdict['master']
            numlines = len(master['list'][intgdefs.LISTENTRY])
            print "\t%s-%s: number of lines in master = %s" % (modname, sname, numlines)
            if numlines == 0:
                miscutils.fwdie("Error: 0 lines in master list", pfwdefs.PF_EXIT_FAILURE)

            if 'columns' in sdict:   # list
                colarr = miscutils.fwsplit(sdict['columns'])
                dictcurr = {}
                for col in colarr:
                    match = re.search(r"(\S+).fullname", col)
                    if match:
                        flabel = match.group(1)
                        if flabel in moddict[pfwdefs.SW_FILESECT]:
                            dictcurr[flabel] = copy.deepcopy(moddict[pfwdefs.SW_FILESECT][flabel])
                            dictcurr[flabel]['curr_module'] = modname
                        else:
                            print "list files = ", moddict[pfwdefs.SW_FILESECT].keys()
                            miscutils.fwdie("Error: Looking at list columns - could not find %s def in dataset" % flabel, pfwdefs.PF_EXIT_FAILURE)

                for llabel, ldict in master['list'][intgdefs.LISTENTRY].items():
                    for flabel, fdict in ldict['file'].items():
                        if flabel in dictcurr:
                            fdict['fullname'] = add_runtime_path(config, dictcurr[flabel], 
                                                                 flabel, fdict, 
                                                                 fdict['filename'])
                        elif len(dictcurr) == 1:
                            fdict['fullname'] = add_runtime_path(config, dictcurr.values()[0], 
                                                                 flabel, fdict, 
                                                                 fdict['filename'])[0]
                        else:
                            print "dictcurr: ", dictcurr.keys()
                            miscutils.fwdie("Error: Looking at lines - could not find %s def in dictcurr" % flabel, pfwdefs.PF_EXIT_FAILURE)


            else:  # file
                currvals = copy.deepcopy(sdict)
                currvals['curr_module'] = modname

                for llabel, ldict in master['list'][intgdefs.LISTENTRY].items():
                    for flabel, fdict in ldict['file'].items():
                        fdict['fullname'] = add_runtime_path(config, currvals, flabel, 
                                                             fdict, fdict['filename'])[0]
        else:
            print "\t%s-%s: no masterlist...skipping" % (modname, sname)

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def create_sublists(config, modname):
    """ break master lists into sublists based upon match or divide_by """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    dataset = config.combine_lists_files(modname)

    for (sname, sdict) in dataset:
        if 'master' in sdict:
            master = sdict['master']
            numlines = len(master['list'][intgdefs.LISTENTRY])
            print "\t%s-%s: number of lines in master = %s" % (modname, sname, numlines)
            if numlines == 0:
                miscutils.fwdie("Error: 0 lines in master list", pfwdefs.PF_EXIT_FAILURE)

            sublists = {}
            keys = get_match_keys(sdict)

            if len(keys) > 0:
                sdict['keyvals'] = {}
                print "\t%s-%s: dividing by %s" % (modname, sname, keys)
                for linenick, linedict in master['list'][intgdefs.LISTENTRY].items():
                    index = ""
                    listkeys = []
                    for key in keys:
                        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "key = %s" % key)
                        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "linedict = %s" % linedict)
                        val = get_value_from_line(linedict, key, None, 1)
                        index += val + '_'
                        listkeys.append(val)
                    sdict['keyvals'][index] = listkeys
                    if index not in sublists:
                        sublists[index] = {'list': {intgdefs.LISTENTRY: {}}}
                    sublists[index]['list'][intgdefs.LISTENTRY][linenick] = linedict
            else:
                sublists['onlyone'] = master

            del sdict['master']
            sdict['sublists'] = sublists
            print "\t%s-%s: number of sublists = %s" % (modname, sname, len(sublists))
            miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "sublist.keys()=%s" % sublists.keys())
            miscutils.fwdebug(4, "PFWBLOCK_DEBUG", "sublists[sublists.keys()[0]]=%s" % sublists[sublists.keys()[0]])
            print ""
            print ""
        else:
            print "\t%s-%s: no masterlist...skipping" % (modname, sname)

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def get_wrap_iter_obj_key(config, moddict):
    """ get wrapper iter object key """
    iter_obj_key = None
    if 'loopobj' in moddict:
        iter_obj_key = moddict['loopobj'].lower()
    else:
        miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "Could not find loopobj. moddict keys = %s" % moddict.keys())
        miscutils.fwdebug(6, "PFWBLOCK_DEBUG", "Could not find loopobj in modict %s" % moddict)
    return iter_obj_key


#######################################################################
def get_wrapper_loopvals(config, modname):
    """ get the values for the wrapper loop keys """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    loopvals = []

    moddict = config[pfwdefs.SW_MODULESECT][modname]
    (found, loopkeys) = config.search('wrapperloop',
                   {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                    'required': False, 'interpolate': True})
    if found:
        miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "\tloopkeys = %s" % loopkeys)
        loopkeys = miscutils.fwsplit(loopkeys.lower())
        loopkeys.sort()  # sort so can make same key easily


        ## determine which list/file would determine loop values
        iter_obj_key = get_wrap_iter_obj_key(config, moddict)
        miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "iter_obj_key=%s" % iter_obj_key)

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
                miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "key=%s" % key)
                (found, val) = config.search(key,
                            {pfwdefs.PF_CURRVALS: {'curr_module': modname},
                            'required': False,
                            'interpolate': True})
                miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "found=%s" % found)
                if found:
                    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "val=%s" % val)
                    val = miscutils.fwsplit(val)
                    loopvals.append(val)
            loopvals = itertools.product(*loopvals)

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return loopvals


#############################################################
def get_value_from_line(line, key, nickname=None, numvals=None):
    """ Return value from a line in master list """
    miscutils.fwdebug(1, "PFWBLOCK_DEBUG", "BEG: key = %s, nickname = %s, numvals = %s" % (key, nickname, numvals))
    # returns None if 0 matches
    #         scalar value if 1 match
    #         array if > 1 match

    # since values could be repeated across files in line,
    # create hash of values to get unique values
    valhash = {}

    if '.' in key:
        miscutils.fwdebug(1, "PFWBLOCK_DEBUG", "Found nickname")
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
        miscutils.fwdie("Error: number found (%s) doesn't match requested (%s)" % \
                        (len(valarr), numvals), pfwdefs.PF_EXIT_FAILURE)

    if len(valarr) == 0:
        retval = None
    elif numvals == 1 or len(valarr) == 1:
        retval = str(valarr[0])
    else:
        retval = str(valarr)

    if hasattr(retval, "strip"): 
        retval = retval.strip()

    miscutils.fwdebug(1, "PFWBLOCK_DEBUG", "END\n\n")
    return retval


#######################################################################
# Assumes currvals includes specific values (e.g., band, ccd)
def create_single_wrapper_wcl(config, modname, wrapinst):
    """ create single wrapper wcl """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s %s" % (modname, wrapinst[pfwdefs.PF_WRAPNUM]))

    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "\twrapinst=%s" % wrapinst)

    currvals = {'curr_module': modname, pfwdefs.PF_WRAPNUM: wrapinst[pfwdefs.PF_WRAPNUM]}


    wrapperwcl = {'modname': modname}



    # file is optional
    if pfwdefs.IW_FILESECT in wrapinst:
        wrapperwcl[pfwdefs.IW_FILESECT] = copy.deepcopy(wrapinst[pfwdefs.IW_FILESECT])
        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "\tfile=%s" % wrapperwcl[pfwdefs.IW_FILESECT])
        for (sectname, sectdict) in wrapperwcl[pfwdefs.IW_FILESECT].items():
            sectdict['sectname'] = sectname

    # list is optional
    if pfwdefs.IW_LISTSECT in wrapinst:
        wrapperwcl[pfwdefs.IW_LISTSECT] = copy.deepcopy(wrapinst[pfwdefs.IW_LISTSECT])
        miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "\tlist=%s" % wrapperwcl[pfwdefs.IW_LISTSECT])


    # do we want exec_list variable?
    miscutils.fwdebug(3, "PFWBLOCK_DEBUG", "\tpfwdefs.SW_EXECPREFIX=%s" % pfwdefs.SW_EXECPREFIX)
    numexec = 0
    modname = currvals['curr_module']
    moddict = config[pfwdefs.SW_MODULESECT][modname]
    execs = pfwutils.get_exec_sections(moddict, pfwdefs.SW_EXECPREFIX)
    for execkey in execs:
        miscutils.fwdebug(3, 'PFWBLOCK_DEBUG', "Working on exec section (%s)"% execkey)
        numexec += 1
        iwkey = execkey.replace(pfwdefs.SW_EXECPREFIX, pfwdefs.IW_EXECPREFIX)
        wrapperwcl[iwkey] = {}
        execsect = moddict[execkey]
        miscutils.fwdebug(3, 'PFWBLOCK_DEBUG', "\t\t(%s)" % (execsect))
        for key, val in execsect.items():
            miscutils.fwdebug(5, 'PFWBLOCK_DEBUG', "\t\t%s (%s)" % (key, val))
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
            result = re.match(r'%s(\d+)' % pfwdefs.IW_EXECPREFIX, execkey)
            if not result:
                miscutils.fwdie('Error:  Could not determine execnum from exec label %s' % execkey, pfwdefs.PF_EXIT_FAILURE)
            wrapperwcl[execkey]['execnum'] = result.group(1)

    if pfwdefs.SW_WRAPSECT in config[pfwdefs.SW_MODULESECT][modname]:
        miscutils.fwdebug(3, 'PFWBLOCK_DEBUG', "Copying wrapper section (%s)"% pfwdefs.SW_WRAPSECT)
        wrapperwcl[pfwdefs.IW_WRAPSECT] = copy.deepcopy(config[pfwdefs.SW_MODULESECT][modname][pfwdefs.SW_WRAPSECT])

    if pfwdefs.IW_WRAPSECT not in wrapperwcl:
        miscutils.fwdebug(3, 'PFWBLOCK_DEBUG', "%s (%s): Initializing wrapper section (%s)"% (modname, wrapinst[pfwdefs.PF_WRAPNUM], pfwdefs.IW_WRAPSECT))
        wrapperwcl[pfwdefs.IW_WRAPSECT] = {}
    wrapperwcl[pfwdefs.IW_WRAPSECT]['pipeline'] = config['pipeprod']
    wrapperwcl[pfwdefs.IW_WRAPSECT]['pipever'] = config['pipever']

    wrapperwcl[pfwdefs.IW_WRAPSECT]['wrappername'] = wrapinst['wrappername']
    wrapperwcl[pfwdefs.IW_WRAPSECT]['outputwcl'] = wrapinst['outputwcl']
    wrapperwcl[pfwdefs.IW_WRAPSECT]['tmpfile_prefix'] = config.search('tmpfile_prefix',
                                {pfwdefs.PF_CURRVALS: currvals,
                                 'required': True, 'interpolate': True})[1]
    wrapperwcl['log'] = wrapinst['log']
    wrapperwcl['log_archive_path'] = wrapinst['log_archive_path']


    if numexec == 0:
        wclutils.write_wcl(config[pfwdefs.SW_MODULESECT][modname])
        raise Exception("Error:  Could not find an exec section for module %s" % modname)


    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")

    return wrapperwcl


# translate sw terms to iw terms in values if needed
def translate_sw_iw(config, wrapperwcl, modname, winst):
    """ Translate submit wcl keys to input wcl keys """
    miscutils.fwdebug(1, "PFWBLOCK_DEBUG", "BEG %s" % modname)


    if ((pfwdefs.SW_FILESECT == pfwdefs.IW_FILESECT) and
         (pfwdefs.SW_LISTSECT == pfwdefs.IW_LISTSECT)):
        print "Skipping translation SW to IW"
    else:
        translation = [(pfwdefs.SW_FILESECT, pfwdefs.IW_FILESECT),
                       (pfwdefs.SW_LISTSECT, pfwdefs.IW_LISTSECT)]
        wrappervars = {}
        wcltodo = [wrapperwcl]
        while len(wcltodo) > 0:
            wcl = wcltodo.pop()
            for key, val in wcl.items():
                miscutils.fwdebug(4, 'PFWBLOCK_DEBUG', "key = %s" % (key))
                if type(val) is dict or type(val) is OrderedDict:
                    wcltodo.append(val)
                elif type(val) is str:
                    miscutils.fwdebug(4, 'PFWBLOCK_DEBUG', "val = %s, %s" % (val, type(val)))
                    for (swkey, iwkey) in translation:
                        miscutils.fwdebug(6, 'PFWBLOCK_DEBUG', "\tbefore swkey = %s, iwkey = %s, val = %s" % (swkey, iwkey, val))
                        val = re.sub(r'^%s\.' % swkey, '%s.' % iwkey, val)
                        val = val.replace(r'{%s.' % swkey, '{%s.' % iwkey)
                        val = val.replace(r' %s.' % swkey, ' %s.' % iwkey)
                        val = val.replace(r',%s.' % swkey, ',%s.' % iwkey)
                        val = val.replace(r':%s.' % swkey, ':%s.' % iwkey)

                        miscutils.fwdebug(6, 'PFWBLOCK_DEBUG', "\tafter val = %s" % (val))
                    miscutils.fwdebug(4, 'PFWBLOCK_DEBUG', "final value = %s" % (val))
                    wcl[key] = val

    #print "new wcl = ", wclutils.write_wcl(wrapperwcl, sys.stdout, True, 4)
    miscutils.fwdebug(1, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def create_module_wrapper_wcl(config, modname, wrapinst):
    """ Create wcl for wrapper instances for a module """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
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

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def divide_into_jobs(config, modname, wrapinst, joblist):
    """ Divide wrapper instances into jobs """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    if pfwdefs.SW_DIVIDE_JOBS_BY not in config and len(joblist) > 1:
        miscutils.fwdie("Error: no %s in config, but already > 1 job" % pfwdefs.SW_DIVIDE_JOBS_BY, pfwdefs.PF_EXIT_FAILURE)

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "number of wrapinst = %s" % len(wrapinst))

    for inst in wrapinst.values():
        key = '_nokey'
        if pfwdefs.SW_DIVIDE_JOBS_BY in config:
            key = ""
            for divb in miscutils.fwsplit(config[pfwdefs.SW_DIVIDE_JOBS_BY], ','):
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

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "number of job lists = %s " % len(joblist.keys()))
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "\tkeys = %s " % ', '.join(joblist.keys()))
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n")


def write_runjob_script(config):
    """ Write runjob script """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")

    jobdir = config.get_filepath('runtime', 'jobdir', {pfwdefs.PF_CURRVALS: {pfwdefs.PF_JOBNUM:"$padjnum"}})
    print "The jobdir =", jobdir

    usedb = miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT])
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

lenjobnum=`expr length "$jobnum"`
if [ $lenjobnum == 4 ]; then
    padjnum=$jobnum
else
    jobnum=$(echo $jobnum | sed 's/^0*//')
    padjnum=`/usr/bin/printf %04d $jobnum`
fi
echo "jobnum = '$jobnum'"
echo "padjnum = '$padjnum'"

intar=$2
jobwcl=$3
tasklist=$4
envfile=$5
outputtar=$6
initdir=`/bin/pwd`

"""

    max_eups_tries = 3
    if 'max_eups_tries' in config:
        max_eups_tries = config['max_eups_tries']
        

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
   BATCHID=`echo $LOADL_STEP_ID | /bin/awk -F "." '{print $(NF-1) "." $(NF) }'`
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

if [ ! -r %(eups)s ]; then
    echo "Error: eups setup script is not readable (%(eups)s)"
    shd2=`/bin/date "+%%s"`
    echo "PFW: job_shell_script endtime: $shd2"
    echo "PFW: job_shell_script exit_status: %(eupsfail)s"
    exit $mystat    # note exit code not passed back through grid universe jobs
fi

echo "Sourcing script to set up EUPS (%(eups)s)"
source %(eups)s

echo "Using eups to setup up %(pipe)s %(ver)s"
d1=`/bin/date "+%%s"`
echo "PFW: eups_setup starttime: $d1"
cnt=0
maxtries=%(max_eups_tries)s
mydelay=300
mystat=1
while [ $mystat -ne 0 -a $cnt -lt $maxtries ]; do
    let cnt=cnt+1
    setup --nolock %(pipe)s %(ver)s
    mystat=$?
    if [ $mystat -ne 0 ]; then
        echo "Warning: eups setup had non-zero exit code ($mystat)"
        if [ $cnt -lt $maxtries ]; then
            echo "Sleeping then retrying..."
            sleep $mydelay
        fi
    fi
done
d2=`/bin/date "+%%s"`
echo "PFW: eups_setup endtime: $d2"
if [ $mystat != 0 ]; then
    echo "Error: eups setup had non-zero exit code ($mystat)"
    shd2=`/bin/date "+%%s"`
    echo "PFW: job_shell_script endtime: $shd2"
    echo "PFW: job_shell_script exit_status: %(eupsfail)s"
    exit $mystat    # note exit code not passed back through grid universe jobs
fi
""" % ({'eups': config['setupeups'],
        'max_eups_tries': max_eups_tries,
        'pipe':config['pipeprod'],
        'ver':config['pipever'],
        'eupsfail': pfwdefs.PF_EXIT_EUPS_FAILURE})

    if not usedb:
        scriptstr += 'echo "DESDMTIME: eups_setup $((d2-d1)) secs"'

    # add any job environment from submit wcl
    scriptstr += 'echo ""\n'
    if pfwdefs.SW_JOB_ENVIRONMENT in config:
        for name, value in config[pfwdefs.SW_JOB_ENVIRONMENT].items():
            scriptstr += 'export %s="%s"\n' % (name.upper(), value)
    scriptstr += 'echo ""\n'


    # print start of job information

    scriptstr += """
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
jobdir=%(full_job_dir)s
echo "Making target job's directory ($jobdir)"
if [ ! -e $jobdir ]; then
    mkdir -p $jobdir
fi
cd $jobdir
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

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return scriptfile



#######################################################################
def create_jobmngr_dag(config, dagfile, scriptfile, joblist):
    """ Write job manager DAG file """

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    config['numjobs'] = len(joblist)
    condorfile = create_runjob_condorfile(config, scriptfile)

    pfwdir = config['processingfw_dir']
    blockname = config['curr_block']
    blkdir = config['block_dir']

    use_condor_transfer_output = True
    if 'use_condor_transfer_output' in config:
        use_condor_transfer_output = miscutils.convertBool(config['use_condor_transfer_output'])


    with open("%s/%s" % (blkdir, dagfile), 'w') as dagfh:
        for jobkey, jobdict in joblist.items():
            jobnum = jobdict['jobnum']
            tjpad = pfwutils.pad_jobnum(jobnum)

            dagfh.write('JOB %s %s\n' % (tjpad, condorfile))
            dagfh.write('VARS %s jobnum="%s"\n' % (tjpad, tjpad))
            dagfh.write('VARS %s exec="../%s"\n' % (tjpad, scriptfile))
            dagfh.write('VARS %s args="%s %s %s %s %s %s"\n' % (tjpad, jobnum, jobdict['inputwcltar'], jobdict['jobwclfile'], jobdict['tasksfile'], jobdict['envfile'], jobdict['outputwcltar']))
            dagfh.write('VARS %s transinput="%s,%s,%s"\n' % (tjpad, jobdict['inputwcltar'], jobdict['jobwclfile'], jobdict['tasksfile']))

            if use_condor_transfer_output:
                dagfh.write('VARS %s transoutput="%s,%s"\n' % (tjpad, jobdict['outputwcltar'], jobdict['envfile']))
            dagfh.write('SCRIPT pre %s %s/libexec/jobpre.py ../uberctrl/config.des $JOB\n' % (tjpad, pfwdir))
            dagfh.write('SCRIPT post %s %s/libexec/jobpost.py ../uberctrl/config.des %s $JOB %s %s $RETURN\n' % (tjpad, pfwdir, blockname, jobdict['inputwcltar'], jobdict['outputwcltar']))

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def tar_inputfiles(config, jobnum, inlist):
    """ Tar the input wcl files for a single job """
    inputtar = config.get_filename('inputwcltar', {pfwdefs.PF_CURRVALS:{'jobnum': jobnum}})
    tjpad = pfwutils.pad_jobnum(jobnum)
    miscutils.coremakedirs(tjpad)

    pfwutils.tar_list("%s/%s" % (tjpad, inputtar), inlist)
    return inputtar


#######################################################################
def create_runjob_condorfile(config, scriptfile):
    """ Write runjob condor description file for target job """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")

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
                  #'periodic_release': '((CurrentTime - EnteredCurrentStatus) > 1800) && (HoldReason =!= "via condor_hold (by user %s)")' % config['operator'],
                  #'periodic_remove' : '((JobStatus == 1) && (JobRunCount =!= Undefined))'
                  'periodic_remove': '((JobStatus == 5) && (HoldReason =!= "via condor_hold (by user %s)"))' % config['operator'],
                  'periodic_hold': '((NumJobStarts > 0) && (JobStatus == 1))'   # put jobs that have run once and are back in idle on hold
                  }


    userattribs = config.get_condor_attributes('$(jobnum)')
    targetinfo = config.get_grid_info()
    print "targetinfo=", targetinfo
    if 'gridtype' not in targetinfo:
        miscutils.fwdie("Error:  Missing gridtype", pfwdefs.PF_EXIT_FAILURE)
    else:
        targetinfo['gridtype'] = targetinfo['gridtype'].lower()
        print 'GRIDTYPE =', targetinfo['gridtype']

    reqs = ['NumJobStarts == 0']   # don't want to rerun any job
    if targetinfo['gridtype'] == 'condor':
        jobattribs['universe'] = 'vanilla'

        if 'concurrency_limits' in config:
            jobattribs['concurrency_limits'] = config['concurrency_limits']

        if 'batchtype' not in targetinfo:
            miscutils.fwdie("Error: Missing batchtype", pfwdefs.PF_EXIT_FAILURE)
        else:
            targetinfo['batchtype'] = targetinfo['batchtype'].lower()

        if 'glidein' in targetinfo['batchtype']:
            if 'uiddomain' not in config:
                miscutils.fwdie("Error: Cannot determine uiddomain for matching to a glidein", pfwdefs.PF_EXIT_FAILURE)
            reqs.append('(UidDomain == "%s")' % config['uiddomain'])
            if 'glidein_name' in config and config['glidein_name'].lower() != 'none':
                reqs.append('(GLIDEIN_NAME == "%s")' % config.interpolate(config['glidein_name']))

            reqs.append('(FileSystemDomain != "")')
            reqs.append('(Arch != "")')
            reqs.append('(OpSys != "")')
            reqs.append('(Disk != -1)')
            reqs.append('(Memory != -1)')

            if 'glidein_use_wall' in config and miscutils.convertBool(config['glidein_use_wall']):
                reqs.append(r"(TimeToLive > \$(wall)*60)")   # wall is in mins, TimeToLive is in secs

        elif targetinfo['batchtype'] == 'local':
            jobattribs['universe'] = 'vanilla'
            if 'loginhost' in config:
                machine = config['loginhost']
            elif 'gridhost' in config:
                machine = config['gridhost']
            else:
                miscutils.fwdie("Error:  Cannot determine machine name (missing loginhost and gridhost)", pfwdefs.PF_EXIT_FAILURE)

            reqs.append('(machine == "%s")' % machine)

        if 'dynslots' in targetinfo['batchtype']:
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
        use_condor_transfer_output = True
        if 'use_condor_transfer_output' in config:
            use_condor_transfer_output = miscutils.convertBool(config['use_condor_transfer_output'])
        if use_condor_transfer_output:
            jobattribs['transfer_output_files'] = '$(transoutput)'
        globus_rsl = pfwcondor.create_rsl(targetinfo)
        if len(globus_rsl) > 0:
            jobattribs['globus_rsl'] = globus_rsl
        print "jobattribs=", jobattribs

    if len(reqs) > 0:
        jobattribs['requirements'] = ' && '.join(reqs)
    print "jobattribs=", jobattribs
    pfwcondor.write_condor_descfile('runjob', condorfile, jobattribs, userattribs)

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return condorfile



#######################################################################
def stage_inputs(config, inputfiles):
    """ Transfer inputs to target archive if using one """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "number of input files needed at target = %s" % len(inputfiles))
    miscutils.fwdebug(6, "PFWBLOCK_DEBUG", "input files %s" % inputfiles)

    if (pfwdefs.USE_HOME_ARCHIVE_INPUT in config and
            (config[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() == pfwdefs.TARGET_ARCHIVE.lower() or
             config[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() == 'all')):

        miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "home_archive = %s" % config[pfwdefs.HOME_ARCHIVE])
        miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "target_archive = %s" % config[pfwdefs.TARGET_ARCHIVE])
        sys.stdout.flush()
        sem = None
        if config['use_db']:
            sem = dbsem.DBSemaphore('filetrans')
            print "Semaphore info:\n", sem
        archive_transfer_utils.archive_copy(config['archive'][config[pfwdefs.HOME_ARCHIVE]],
                                            config['archive'][config[pfwdefs.TARGET_ARCHIVE]],
                                            config['archive_transfer'],
                                            inputfiles, config)
        if sem is not None:
            del sem

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def write_output_list(config, outputfiles):
    """ Write output list """
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "output files %s" % outputfiles)

    if 'block_outputlist' not in config:
        miscutils.fwdie("Error:  Could not find block_outputlist in config.   Internal Error.", pfwdefs.PF_EXIT_FAILURE)

    with open(config['block_outputlist'], 'w') as outfh:
        for fname in outputfiles:
            outfh.write("%s\n" % miscutils.parse_fullname(fname, miscutils.CU_PARSE_FILENAME))

    miscutils.fwdebug(0, "PFWBLOCK_DEBUG", "END")

