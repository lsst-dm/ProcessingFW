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
from processingfw.fwutils import *
#import processingfw.pfwxml as pfwxml
import intgutils.wclutils as wclutils
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

    print sdict.keys()
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
        print objDef['sublists'].keys()
        matchkeys = get_match_keys(objDef)
        print matchkeys
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
def assign_file_to_wrapper_inst(config, theinputs, moddict, currvals, winst, fname, finfo, is_iter_obj=False):
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG: Working on file %s" % fname)

    if 'listonly' in finfo and convertBool(finfo['listonly']):
        fwdebug(0, "PFWBLOCK_DEBUG", "Skipping %s due to listonly key" % fname)
        return

    if IW_FILESECT not in winst:
        winst[IW_FILESECT] = {}

    winst[IW_FILESECT][fname] = {}
    if 'sublists' in finfo:  # files came from query
        sublist = find_sublist(finfo, winst)
        if len(sublist['list'][PF_LISTENTRY]) > 1:
            fwdie("Error: more than 1 line to choose from for file" + sublist['list'][PF_LISTENTRY], PW_EXIT_FAILURE)
        line = sublist['list'][PF_LISTENTRY].values()[0]
        if 'file' not in line:
            fwdie("Error: 0 file in line" + str(line), PW_EXIT_FAILURE)
            
        if len(line['file']) > 1:
            raise Exception("more than 1 file to choose from for file" + line['file'])
        finfo = line['file'].values()[0]
        print "finfo =", finfo

        fullname = finfo['fullname']
        winst[IW_FILESECT][fname]['fullname'] = fullname

        if fname in theinputs[SW_FILESECT]:
            winst['wrapinputs'][len(winst['wrapinputs'])+1] = fullname

        print "Assigned filename for fname %s (%s)" % (fname, finfo['filename'])
    elif 'fullname' in moddict[SW_FILESECT][fname]:
        winst[IW_FILESECT][fname]['fullname'] = moddict[SW_FILESECT][fname]['fullname']
        print "Copied fullname for ", fname
        print winst[IW_FILESECT][fname]
        if fname in theinputs[SW_FILESECT]:
            winst['wrapinputs'][len(winst['wrapinputs'])+1] = moddict[SW_FILESECT][fname]['fullname']
    else:
        if 'filename' in moddict[SW_FILESECT][fname]:
            winst[IW_FILESECT][fname]['filename'] = config.search('filename', {PF_CURRVALS: currvals, 
                                                                               'searchobj': moddict[SW_FILESECT][fname], 
                                                                               'expand': True, 
                                                                               'required': True,
                                                                               'interpolate':True})[1]
        else:
            print "creating filename for", fname 
            sobj = copy.deepcopy(finfo)
            sobj.update(winst)
            winst[IW_FILESECT][fname]['filename'] = config.get_filename(None, {PF_CURRVALS: currvals, 
                                                                'searchobj': sobj,
                                                                'expand': True}) 

        # Add runtime path to filename
        print "creating path for", fname #, winst[IW_FILESECT][fname]['filename']
        path = config.get_filepath('runtime', None, {PF_CURRVALS: currvals, 'searchobj': finfo})
        print "\tpath = ", path
        if type(winst[IW_FILESECT][fname]['filename']) is list:
            print fname,"filename is a list"
            winst[IW_FILESECT][fname]['fullname'] = []
            print fname,"number of names = ", len(winst[IW_FILESECT][fname]['filename'])
            for f in winst[IW_FILESECT][fname]['filename']:
            #    print path,"+",f
                winst[IW_FILESECT][fname]['fullname'].append("%s/%s" % (path, f))
                if fname in theinputs[SW_FILESECT]:
                    winst['wrapinputs'][len(winst['wrapinputs'])+1] = "%s/%s" % (path,f)

            winst[IW_FILESECT][fname]['fullname'] = ','.join(winst[IW_FILESECT][fname]['fullname'])
        else:
            print "Adding path to filename for", fname
            winst[IW_FILESECT][fname]['fullname'] = "%s/%s" % (path, winst[IW_FILESECT][fname]['filename'])
            if fname in theinputs[SW_FILESECT]:
                winst['wrapinputs'][len(winst['wrapinputs'])+1] = winst[IW_FILESECT][fname]['fullname']


#        winst[IW_FILESECT][fname]['filename'] = config.get_filename(None, {PF_CURRVALS: currvals, 'searchobj': finfo}) 
#        if type(winst[IW_FILESECT][fname]['filename']) is list:
#            winst[IW_FILESECT][fname]['filename'] = ','.join(winst[IW_FILESECT][fname]['filename'])
#    if IW_REQ_META in finfo:
#        winst[IW_FILESECT][fname][IW_REQ_META] = copy.deepcopy(finfo[IW_REQ_META])
        del winst[IW_FILESECT][fname]['filename']

    fwdebug(0, "PFWBLOCK_DEBUG", "is_iter_obj = %s %s" % (is_iter_obj, finfo))
    if finfo is not None and is_iter_obj:
        fwdebug(0, "PFWBLOCK_DEBUG", "is_iter_obj = true")
        for key,val in finfo.items():
            if key not in ['fullname','filename']:
                fwdebug(0, "PFWBLOCK_DEBUG", "is_iter_obj: saving %s" % key)
                winst[key] = val
        
    fwdebug(0, "PFWBLOCK_DEBUG", "END: Done working on file %s" % fname)



#######################################################################
def assign_list_to_wrapper_inst(config, moddict, currvals, winst, lname, ldict):
    fwdebug(0, "PFWBLOCK_DEBUG", "Working on list %s from %s" % (lname, moddict['modulename']))
    if IW_LISTSECT not in winst:
        winst[IW_LISTSECT] = {}

    winst[IW_LISTSECT][lname] = {}

    sobj = copy.deepcopy(ldict)
    sobj.update(winst)

    fwdebug(0, "PFWBLOCK_DEBUG", "creating listdir and listname")
    if moddict['modulename'] == 'mkdflatcor':
        print 'w band = %s' % winst['band']
        print 'w ccd = %s' % winst['ccd']
        print 's band = %s' % sobj['band']
        print 's ccd = %s' % sobj['ccd']
        if 'band' in currvals:
            print 'c band = %s' % currvals['band']
        if 'ccd' in currvals:
            print 'c ccd = %s' % currvals['ccd']

    listdir = config.get_filepath('runtime', 'list', {PF_CURRVALS: currvals,
                         'required': True, 'interpolate': True,
                         'searchobj': sobj})
    
    listname = config.get_filename(None, {PF_CURRVALS: currvals,
                                    'searchobj': sobj, 'required': True, 'interpolate': True})
    fwdebug(0, "PFWBLOCK_DEBUG", "listname = %s" % (listname))
    listname = "%s/%s" % (listdir, listname)

    winst[IW_LISTSECT][lname]['fullname'] = listname
    fwdebug(0, "PFWBLOCK_DEBUG", "listname = %s" % (winst[IW_LISTSECT][lname]['fullname']))
    print ldict.keys()
    print ldict['sublists'].keys()
    if 'sublists' in ldict:
        sublist = find_sublist(ldict, winst)
        for llabel,ldict in sublist['list'][PF_LISTENTRY].items():
            for flabel,fdict in ldict['file'].items():
                winst['wrapinputs'][len(winst['wrapinputs'])+1] = fdict['fullname']
        output_list(config, winst[IW_LISTSECT][lname]['fullname'], sublist, lname, ldict, currvals)
#    else:
#        create_simple_list(config, lname, ldict, currvals)

                        
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

    for winst in wrapperinst.values():
        winst['wrapinputs'] = {}

        # create currvals
        currvals = { 'curr_module': modname, PF_WRAPNUM: winst[PF_WRAPNUM]}
        for key in loopkeys:
            currvals[key] = winst[key]
        fwdebug(0, "PFWBLOCK_DEBUG", "currvals " + str(currvals))

        fwdebug(0, "PFWBLOCK_DEBUG", "currvals " + str(currvals))
        # do wrapper loop object first, if exists, to provide keys for filenames
        iter_obj_key = get_wrap_iter_obj_key(config, moddict)
        if iter_obj_key is not None:
            (iter_obj_sect, iter_obj_name) = fwsplit(iter_obj_key, '.')
            iter_obj_dict = pfwutils.get_wcl_value(iter_obj_key, moddict) 
            fwdebug(0, "PFWBLOCK_DEBUG", "iter_obj %s %s" % (iter_obj_name, iter_obj_sect))
            print SW_FILESECT
            if iter_obj_sect.lower() == SW_FILESECT.lower():
                assign_file_to_wrapper_inst(config, theinputs, moddict, currvals, winst, iter_obj_name, iter_obj_dict, True)
            elif iter_obj_sect.lower() == SW_LISTSECT.lower():
                assign_list_to_wrapper_inst(config, moddict, currvals, winst, iter_obj_name, iter_obj_dict)
            else:
                fwdie("Error: unknown iter_obj_sect (%s)" % iter_obj_sect, PF_EXIT_FAILURE)
        print winst

        
        if SW_FILESECT in moddict:
            for fname, fdict in moddict[SW_FILESECT].items(): 
                if iter_obj_key is not None and \
                   iter_obj_sect.lower() == SW_FILESECT.lower() and \
                   iter_obj_name.lower() == fname.lower():
                    continue    # already did iter_obj
                assign_file_to_wrapper_inst(config, theinputs, moddict, currvals, winst, fname, fdict)

        fwdebug(0, "PFWBLOCK_DEBUG", "currvals " + str(currvals))

        if SW_LISTSECT in moddict:
            for lname, ldict in moddict[SW_LISTSECT].items():
                if iter_obj_key is not None and \
                   iter_obj_sect.lower() == SW_LISTSECT.lower() and \
                   iter_obj_name.lower() == lname.lower():
                    fwdebug(0, "PFWBLOCK_DEBUG", "skipping list %s as already did for it as iter_obj")
                    continue    # already did iter_obj
                assign_list_to_wrapper_inst(config, moddict, currvals, winst, lname, ldict)
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")



#######################################################################
def output_list(config, listname, sublist, lname, ldict, currvals):
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG: %s" % listname)

    listdir = os.path.dirname(listname)
    if len(listdir) > 0 and not os.path.exists(listdir):  # some parallel filesystems really don't like
                                                          # trying to make directory if it already exists
        try:
            os.makedirs(listdir)
        except OSError as exc:      # go ahead and check for race condition
            if exc.errno == errno.EEXIST:
                pass
            else:
                fwdie("Error: problems making directory listdir: %s" % exc, PF_EXIT_FAILURE)

    format = 'textsp'
    if 'format' in ldict:
        format = ldict['format']

    if 'columns' in ldict:
        columns = ldict['columns'].lower()
    else:
        columns = 'fullname'
    
    with open(listname, "w") as listfh:
        for linenick, linedict in sublist['list'][PF_LISTENTRY].items():
            output_line(listfh, linedict, format, fwsplit(columns))
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")




#####################################################################
def output_line(listfh, line, format, keyarr):
    """ output line into fo input list for science code"""

    format = format.lower()

    if format == 'config' or format == 'wcl':
        fh.write("<file>\n")

    numkeys = len(keyarr)
    for i in range(0, numkeys):
        key = keyarr[i]
        value = None

        if '.' in  key:
            [nickname, key2] = key.replace(' ','').split('.')
            value = get_value_from_line(line, key2, nickname, None)
            if value == None:
                value = get_value_from_line(line, key2, None, 1)
                if value == None:
                    fwdie("Error: could not find value %s for line...\n%s" % (key, line), PF_EXIT_FAILURE)
                else: # assume nickname was really table name
                    key = key2
        else:
            value = get_value_from_line(line, key, None, 1)

        # handle last field (separate to avoid trailing comma)
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
    for winst in wrapperinst.values():
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
                    fwdebug(0, "PFWBLOCK_DEBUG", "Skipping %s due to listonly key" % fname)
                    continue

                fwdebug(0, 'PFWBLOCK_DEBUG', '%s: working on file: %s' % (winst[PF_WRAPNUM], fname))
                fwdebug(3, "PFWBLOCK_DEBUG", "fullname = %s" % (winst[IW_FILESECT][fname]['fullname']))

                
                for k in ['filetype', IW_REQ_META, IW_OPT_META, COPY_CACHE, DIRPAT]:
                    if k in fdict:
                        fwdebug(0, "PFWBLOCK_DEBUG", "%s copying %s" % (fname, k))
                        winst[IW_FILESECT][fname][k] = copy.deepcopy(fdict[k])
                    else:
                        fwdebug(0, "PFWBLOCK_DEBUG", "%s: no %s" % (fname, k))

                hdrups = pfwutils.get_hdrup_sections(fdict, IW_UPDATE_HEAD_PREFIX)
                for hname, hdict in hdrups.items():
                    fwdebug(0, "PFWBLOCK_DEBUG", "%s copying %s" % (fname, hname))
                    winst[IW_FILESECT][fname][hname] = copy.deepcopy(hdict)

                # save OPS path for cache
                if USE_CACHE in config:
                    if (config[USE_CACHE].lower() == 'filetransfer' or
                        (config[USE_CACHE].lower() != 'never' and COPY_CACHE in fdict and fdict[COPY_CACHE])): 
                        if DIRPAT not in fdict:
                            print "Warning: Could not find %s in %s's section" % (DIRPAT,fname)
                        else:
                            searchobj = copy.deepcopy(fdict)
                            searchobj.update(winst)
                            searchopts['searchobj'] = searchobj
                            winst[IW_FILESECT][fname]['cachepath'] = config.get_filepath('ops', 
                                                                            fdict[DIRPAT], searchopts)

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


        # output wcl fullname
        outputwcl_name = config.get_filename('outputwcl', searchopts)
        outputwcl_path = config.get_filepath('runtime', 'outputwcl', searchopts)
        winst['outputwcl'] = outputwcl_path + '/' + outputwcl_name


    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def add_file_metadata(config, modname):
    """ add file metadata sections to a single file section from a module"""

    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    fwdebug(0, "PFWBLOCK_DEBUG", "Working on module " + modname)
    moddict = config[SW_MODULESECT][modname]
    
    execs = pfwutils.get_exec_sections(moddict, SW_EXECPREFIX)
    if SW_FILESECT in moddict:
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
             
                        (reqmeta, optmeta, updatemeta) = pfwutils.create_file_metadata_dict(filetype, config['filetype_metadata'], wclsect, config['file_header']) 
                        if reqmeta is not None:
                            fdict[IW_REQ_META] = reqmeta
                        else:
                            fdict[IW_REQ_META] = OrderedDict()  # framework fields addition below assume dict exists, so can just create empty one here
            
                        if optmeta is not None:
                            fdict[IW_OPT_META] = optmeta

                        if updatemeta is not None:
                            updatemeta[IW_UPDATE_WHICH_HEAD] = '0'  # framework always updates primary header
                            headsectnum = 0
                            if IW_UPDATE_HEAD_PREFIX+'0' in fdict:
                                fwdie("Error: %s is reserved for PFW updates but was specified in file %s" % (IW_UPDATE_HEAD_PREFIX+'0', fname), PF_EXIT_FAILURE)

                            fdict[IW_UPDATE_HEAD_PREFIX+'0'] = updatemeta
                         
                        # add descriptions/types to submit-wcl specified updates if missing
                        hdrups = pfwutils.get_hdrup_sections(fdict, IW_UPDATE_HEAD_PREFIX)
                        for hname, hdict in sorted(hdrups.items()):
                            for key,val in hdict.items():
                                if key != IW_UPDATE_WHICH_HEAD:
                                    valparts = fwsplit(val, '/')
                                    print key, valparts
                                    if len(valparts) == 1:  # wcl specified value, look up rest from config
                                        newvaldict = pfwutils.create_update_items('V', [key], config['file_header'], header_value={key:val}) 
                                        hdict.update(newvaldict)
                                    elif len(valparts) != 3:  # 3 is valid full spec of update header line
                                        fwdie('Error:  invalid header update line (%s = %s)\nNeeds value[/descript/type]' % (key,val), PF_EXIT_FAILURE)


                        # add some fields needed by framework for processing output wcl (not stored in database)
                        if IW_META_WCL not in fdict[IW_REQ_META]:
                            fdict[IW_REQ_META][IW_META_WCL] = ''
                        else:
                            fdict[IW_REQ_META][IW_META_WCL] += ','

                        fdict[IW_REQ_META][IW_META_WCL] += '%(sect)s.fullname,%(sect)s.sectname' % ({'sect':wclsect})
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
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")

    jobwclfile = config.get_filename('jobwcl', {PF_CURRVALS: {PF_JOBNUM: jobnum}, 'required': True, 'interpolate': True})

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
              'cachename': config.search('cachename', {'required': True,
                                    'interpolate': True})[1],
              'jobkeys': jobkey[1:].replace('_',',')
            }
    if convertBool(config[PF_USE_DB_OUT]):
        if 'des_services' in config and config['des_services'] is not None: 
            jobwcl['des_services'] = config['des_services']
        jobwcl['des_db_section'] = config['des_db_section']

    (exists, value) =  config.search(USE_CACHE, {'interpolate': True})
    if exists:
        jobwcl[USE_CACHE]=value

    jobwcl['filetype_metadata'] = config['filetype_metadata']
    jobwcl[IW_EXEC_DEF] = config[SW_EXEC_DEF]
    jobwcl[DATA_DEF] = config[DATA_DEF]
    #jobwcl[DIRPATSECT] = config[DIRPATSECT]
    jobwcl['wrapinputs'] = wrapinputs

    print jobwcl.keys()
   
    with open(jobwclfile, 'w') as wclfh:
        wclutils.write_wcl(jobwcl, wclfh, True, 4)

    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return jobwclfile
    

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
                  'wrapname': config.search('wrapname',
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
    neededvals.update(pfwutils.traverse_wcl(wrapwcl))

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

        fwdebug(0, "PFWBLOCK_DEBUG", "loopvals = %s" % (loopvals))
        for instvals in loopvals:
            fwdebug(0, "PFWBLOCK_DEBUG", "instvals = %s" % str(instvals) )
            
            config.inc_wrapnum()
            winst = {PF_WRAPNUM: config[PF_WRAPNUM],
                     'wrapname':  config.search('wrappername',
                            {PF_CURRVALS: {'curr_module': modname},
                             'required': True, 'interpolate': True})[1]
                    }
            instkey = ""
            for k in range(0, len(loopkeys)):
                winst[loopkeys[k]] = instvals[k] 
                instkey += instvals[k] + '_'

            wrapperinst[instkey] = winst
    else:
        config.inc_wrapnum()
        wrapperinst['noloop'] = {PF_WRAPNUM: config[PF_WRAPNUM],
                                 'wrapname':  config.search('wrappername',
                            {PF_CURRVALS: {'curr_module': modname},
                             'required': True, 'interpolate': True})[1]
                                }

    fwdebug(0, "PFWBLOCK_DEBUG", "Number wrapper inst: %s" % len(wrapperinst))
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
    return wrapperinst


#####################################################################
def read_master_lists(config, modname, modules_prev_in_list):
    """ Read master lists and files from files created earlier """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    print "\tModule %s" % (modname)

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
                    master = wclutils.read_wcl(wclfh)
                    print master.keys()
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
    print "\tModule %s" % (modname)
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
            print sublists.keys()
            print sublists[sublists.keys()[0]]
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
        fwdebug(0, "PFWBLOCK_DEBUG", "Could not find loopobj in %s" % moddict)
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
        print "\tloopkeys = ", loopkeys
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
                print "Couldn't find keyvals for loopobj", moddict['loopobj']     
        else:
            print "\tdefaulting to wcl values"
            loopvals = []
            for key in loopkeys:
                (found, val) = config.search(key, 
                            {PF_CURRVALS: {'curr_module': modname},
                            'required': False, 
                            'interpolate': True})
                if found:
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
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

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
        fwdebug(0, 'PFWBLOCK_DEBUG', "Working on exec section (%s)"% execkey)
        numexec += 1
        iwkey = execkey.replace(SW_EXECPREFIX, IW_EXECPREFIX)
        wrapperwcl[iwkey] = {}
        execsect = moddict[execkey]
        fwdebug(0, 'PFWBLOCK_DEBUG', "\t\t(%s)" % (execsect))
        for key, val in execsect.items():
            fwdebug(0, 'PFWBLOCK_DEBUG', "\t\t%s (%s)" % (key, val))
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

    if SW_WRAPSECT in config[SW_MODULESECT][modname]:
        fwdebug(0, 'PFWBLOCK_DEBUG', "Copying wrapper section (%s)"% SW_WRAPSECT)
        wrapperwcl[IW_WRAPSECT] = copy.deepcopy(config[SW_MODULESECT][modname][SW_WRAPSECT])

    if IW_WRAPSECT not in wrapperwcl:
        fwdebug(1, 'PFWBLOCK_DEBUG', "%s (%s): Initializing wrapper section (%s)"% (modname, wrapinst[PF_WRAPNUM], IW_WRAPSECT))
        wrapperwcl[IW_WRAPSECT] = {}
    wrapperwcl[IW_WRAPSECT]['pipeline'] = config['pipeprod']
    wrapperwcl[IW_WRAPSECT]['pipever'] = config['pipever']

    wrapperwcl[IW_WRAPSECT]['wrappername'] = wrapinst['wrappername']
    wrapperwcl[IW_WRAPSECT]['outputwcl'] = wrapinst['outputwcl']
    wrapperwcl[IW_WRAPSECT]['tmpfile_prefix'] =  config.search('tmpfile_prefix',
                                {PF_CURRVALS: currvals,
                                 'required': True, 'interpolate': True})[1]


    if numexec == 0:
        wclutils.write_wcl(config[SW_MODULESECT][modname])
        raise Exception("Error:  Could not find an exec section for module %s" % modname)
        

    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")

    return wrapperwcl


# Early version of wrapper code could not handle global variables
#   So the following code changed all global variables to 
#       variables in the wrapper section
#def fix_globalvars(config, wrapperwcl, modname, winst):
#    fwdebug(1, "PFWBLOCK_DEBUG", "BEG %s" % modname)
#
#    wrappervars = {}
#    wcltodo = [wrapperwcl]
#    while len(wcltodo) > 0:
#        wcl = wcltodo.pop()
#        for key,val in wcl.items():
#            if type(val) is dict or type(val) is OrderedDict:
#                wcltodo.append(val)
#            else:
#                print "val = ", val, type(val)
#                varstr = [m.group(1) for m in re.finditer('(?i)\$\{([^}]+)\}', val)]
#                for vstr in varstr:
#                    if '.' not in vstr:
#                        oldvar = '\$\{%s\}' % vstr
#                        newvar = '${wrapper.%s}' % vstr
#                        print "before val = ", val, 'oldvar =', oldvar, 'newvar = ', newvar
#                        val = re.sub(oldvar, newvar, val)
#                        print "after val = ", val
#                        wrappervars[vstr.lower()] = True
#
#                print "final value = ", val
#                wcl[key] = val
#
#    for wk in wrappervars.keys():
#        wrapperwcl[IW_WRAPSECT][wk] =  config.search(wk,
#                                {PF_CURRVALS: {'curr_module': modname},
#                                 'searchobj': winst,
#                                 'required': True, 
#                                 'interpolate': True})[1]
#    fwdebug(1, "PFWBLOCK_DEBUG", "END\n\n")
#               


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
                else:
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
        wclutils.write_wcl(wrapperwcl)
        
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
        joblist[key]['wrapinputs'][inst[PF_WRAPNUM]] = inst['wrapinputs']
        if IW_LISTSECT in inst:
            for linfo in inst[IW_LISTSECT].values():
                joblist[key]['inlist'].append(linfo['fullname'])

    fwdebug(0, "PFWBLOCK_DEBUG", "number of job lists = %s " % len(joblist.keys()))
    fwdebug(0, "PFWBLOCK_DEBUG", "\tkeys = %s " % ','.join(joblist.keys()))
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n")
            


#######################################################################
#def create_wrapper_wcl(config, wrapinst):
#    """ Create wcl for single wrapper instance """
#    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
#    print "wrapinst.keys = ", wrapinst.keys()
#    modulelist = fwsplit(config[SW_MODULELIST].lower())
#    tasks = []
#
#    os.mkdir('wcl')
#    for modname in modulelist:
#        print "Creating wrapper wcl for module '%s'" % modname
#        if modname not in config[SW_MODULESECT]:
#            raise Exception("Error: Could not find module description for module %s\n" % (modname))
#
#        if modname not in wrapinst:
#            print "Error: module not in wrapinst"
#            print wrapinst.keys()
#
#        for inst in wrapinst[modname].values():
#            wrapperwcl = create_single_wrapper_wcl(config, modname, inst)
#            #(found, inputwcl) = config.search('inputwcl',
#            #        {PF_CURRVALS: {'curr_module': modname}, 'searchobj': inst,
#            #        'required': True, 'interpolate': True})
#
#            (found, wrappername) = config.search('wrappername',
#                    {PF_CURRVALS: {'curr_module': modname}, 'searchobj': inst,
#                    'required': True, 'interpolate': True})
#
#            (found, logfile) = config.search('log',
#                    {PF_CURRVALS: {'curr_module': modname}, 'searchobj': inst,
#                    'required': True, 'interpolate': True})
#
#            inputwcl = inst['inputwcl']
#            wrappername = inst['wrappername']
#            logfile = inst['log']
#
#            #fix_globalvars(config, wrapperwcl, modname, inst)
#            add_needed_values(config, modname, inst, wrapperwcl)
#
#            write_wrapper_wcl(config, inputwcl, wrapperwcl) 
#
#            # Add this wrapper execution to list
#            tasks.append((wrappername, inputwcl, logfile))
#    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")
#    return tasks


def write_runjob_script(config):
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")

    jobdir = config.get_filepath('runtime', 'jobdir', {PF_CURRVALS: {PF_JOBNUM:"$padjnum"}})
    print "The jobdir =", jobdir

    scriptstr = """#!/bin/sh
echo "Current args: $@";
if [ $# -ne 4 ]; then
    echo "Usage: <jobnum> <input tar> <job wcl> <tasklist> ";
    exit 1;
fi
shd1=`/bin/date "+%s"` 
jobnum=$1
padjnum=`/usr/bin/printf %04d $jobnum`
intar=$2
jobwcl=$3
tasklist=$4
initdir=`/bin/pwd`
"""

    # setup job environment
    scriptstr += """
export SHELL=/bin/bash    # needed for setup to work in Condor environment
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
    #      Since wcl's variable syntax matches shell variable syntax and 
    #      underscores are used to separate name parts, have to use place 
    #      holder for jobnum and replace later with shell variable
    #      Otherwise, get_filename fails to substitute for padjnum
    envfile = config.get_filename('envfile', {PF_CURRVALS: {PF_JOBNUM:"XXXXXXXX"}})
    envfile = envfile.replace("XXXXXXXX", "${padjnum}")

    scriptstr +="""
env > %s
echo ""
echo "Initial condor job directory = " $initdir
echo "Files copied over by condor:"
ls -l
""" % (envfile)
   
    if 'runroot' in config and config['runroot'] is not None:
        rdir = config['runroot'] + '/' + jobdir
        print "rdir =", rdir
        scriptstr += """
echo ""
echo "Making run directory in runroot: %(rdir)s"
if [ ! -e %(rdir)s ]; then
    mkdir -p %(rdir)s
fi
cd %(rdir)s
        """ % ({'rdir': rdir})

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
    scriptstr += """
echo "Copying job wcl and task list to job working directory"
d1=`/bin/date "+%s"` 
cp $initdir/$jobwcl $jobwcl
cp $initdir/$tasklist $tasklist
d2=`/bin/date "+%s"` 
echo "DESDMTIME: copy_jobwcl_tasklist $((d2-d1))"
"""

    # call the job workflow program
    scriptstr += """
echo ""
echo "Calling pfwrunjob.py"
echo "cmd> ${PROCESSINGFW_DIR}/libexec/pfwrunjob.py --config $jobwcl $tasklist"
d1=`/bin/date "+%s"` 
${PROCESSINGFW_DIR}/libexec/pfwrunjob.py --config $jobwcl $tasklist
d2=`/bin/date "+%s"` 
echo "DESDMTIME: pfwrunjob.py $((d2-d1))"
shd2=`/bin/date "+%s"` 
echo "DESDMTIME: job_shell_script $((shd2-shd1))"
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
            dagfh.write('VARS %s args="%s %s %s %s"\n' % (tjpad, jobnum, jobdict['tarfile'], jobdict['jobwclfile'], jobdict['tasksfile']))
            dagfh.write('VARS %s transinput="%s,%s,%s"\n' % (tjpad, jobdict['tarfile'], jobdict['jobwclfile'], jobdict['tasksfile']))
            # no pre script for job.   Job inserted into DB at beginning of job running
            #TODO dagfh.write('SCRIPT post %s %s/libexec/logpost.py config.des %s j $JOB $RETURN\n' % (tjpad, pfwdir, blockname)) 

    uberdagfile = "../uberctrl/%s" % (dagfile)
    if os.path.exists(uberdagfile):
        os.unlink(uberdagfile)
    os.symlink("../%s/%s" % (blockname, dagfile), uberdagfile)

#    pfwcondor.add2dag(dagfile, config.get_dag_cmd_opts(), config.get_condor_attributes("jobmngr"), None, sys.stdout)
    fwdebug(0, "PFWBLOCK_DEBUG", "END\n\n")


#######################################################################
def tar_inputfiles(config, jobnum, inlist):
    """ Tar the input wcl files for a single job """
    inputtar = config.get_filename('inputtar', {PF_CURRVALS:{'jobnum': jobnum}})
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


