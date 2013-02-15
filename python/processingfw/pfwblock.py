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

    inputlist = {}
    for winst in wrapperinst.values():
        # create currvals
        currvals = { 'curr_module': modname, PF_WRAPNUM: winst[PF_WRAPNUM]}
        for key in loopkeys:
            currvals[key] = winst[key]
        fwdebug(6, "PFWBLOCK_DEBUG", "currvals " + str(currvals))

        if SW_FILESECT in moddict:
            winst[IW_FILESECT] = {}
            for fname, fdict in moddict[SW_FILESECT].items(): 
                fwdebug(3, "PFWBLOCK_DEBUG", "Working on file "+fname)
                winst[IW_FILESECT][fname] = {}
                if 'sublists' in fdict:  # files came from query
                    if len(fdict['sublists'].keys()) > 1:
#                        print fdict['sublists'].keys()
                        matchkeys = fwsplit(fdict['match'])
                        matchkeys.sort()
                        index = ""
                        for mkey in matchkeys:
                            if mkey not in winst:
                                raise Exception("Cannot find match key %s in winst %s" % (mkey, winst))
                            index += winst[mkey] + '_'
                        fwdebug(3, "PFWBLOCK_DEBUG", "sublist index = "+index)
                        if index not in fdict['sublists']:
                            raise Exception("Cannot find sublist matching "+index)
                        sublist = fdict['sublists'][index]
                    else:
                        sublist = fdict['sublists'].values()[0]

                    if len(sublist['list'][PF_LISTENTRY]) > 1:
                        raise Exception("more than 1 line to choose from for file" + sublist['list'][PF_LISTENTRY])
                    line = sublist['list'][PF_LISTENTRY].values()[0]
                    if 'file' not in line:
                        raise Exception("0 file in line" + str(line))
                        
                    if len(line['file']) > 1:
                        raise Exception("more than 1 file to choose from for file" + line['file'])
                    finfo = line['file'].values()[0]
                    print "finfo =", finfo

                    # Add runtime path to filename
                    path = config.get_filepath('runtime', None, {PF_CURRVALS: currvals, 'searchobj': fdict})
                    winst[IW_FILESECT][fname]['fullname'] = "%s/%s" % (path, finfo['filename'])
                    inputlist[winst[IW_FILESECT][fname]['fullname']] = True

                    #winst[IW_FILESECT][fname]['filename'] = finfo['filename']
                    print "Assigned filename for fname %s (%s)" % (fname, finfo['filename'])
                elif 'fullname' in moddict[SW_FILESECT][fname]:
                    winst[IW_FILESECT][fname]['fullname'] = moddict[SW_FILESECT][fname]['fullname']
                    print "Copied fullname for ", fname
                    print winst[IW_FILESECT][fname]
                else:
                    if 'filename' in moddict[SW_FILESECT][fname]:
                        winst[IW_FILESECT][fname]['filename'] = config.search('filename', {PF_CURRVALS: currvals, 
                                                                                           'searchobj': moddict[SW_FILESECT][fname], 
                                                                                           'expand': True, 
                                                                                           'required': True,
                                                                                           'interpolate':True})[1]
                    else:
                        print "creating filename for", fname 
                        winst[IW_FILESECT][fname]['filename'] = config.get_filename(None, {PF_CURRVALS: currvals, 
                                                                            'searchobj': fdict,
                                                                            'expand': True}) 

                    # Add runtime path to filename
                    print "creating path for", fname #, winst[IW_FILESECT][fname]['filename']
                    path = config.get_filepath('runtime', None, {PF_CURRVALS: currvals, 'searchobj': fdict})
                    print "\tpath = ", path
                    if type(winst[IW_FILESECT][fname]['filename']) is list:
                        print fname,"filename is a list"
                        winst[IW_FILESECT][fname]['fullname'] = []
                        print fname,"number of names = ", len(winst[IW_FILESECT][fname]['filename'])
                        for f in winst[IW_FILESECT][fname]['filename']:
                        #    print path,"+",f
                            winst[IW_FILESECT][fname]['fullname'].append("%s/%s" % (path, f))
    
                        winst[IW_FILESECT][fname]['fullname'] = ','.join(winst[IW_FILESECT][fname]['fullname'])
                    else:
                        print "Adding path to filename for", fname
                        winst[IW_FILESECT][fname]['fullname'] = "%s/%s" % (path, winst[IW_FILESECT][fname]['filename'])


#                    winst[IW_FILESECT][fname]['filename'] = config.get_filename(None, {PF_CURRVALS: currvals, 'searchobj': fdict}) 
#                    if type(winst[IW_FILESECT][fname]['filename']) is list:
#                        winst[IW_FILESECT][fname]['filename'] = ','.join(winst[IW_FILESECT][fname]['filename'])
#                if 'req_metadata' in fdict:
#                    winst[IW_FILESECT][fname]['req_metadata'] = copy.deepcopy(fdict['req_metadata'])
                    del winst[IW_FILESECT][fname]['filename']
                print "Done with file", fname

        if SW_LISTSECT in moddict:
            winst[IW_LISTSECT] = {}
            for lname, ldict in moddict[SW_LISTSECT].items():
                winst[IW_LISTSECT][lname] = {}
                winst[IW_LISTSECT][lname]['listname'] = config.search('listname', {PF_CURRVALS: currvals, 
                                                                              'searchobj': ldict, 
                                                                              'required': True, 
                                                                              'interpolate':True})[1]
                create_simple_list(config, lname, ldict, currvals)
    fwdebug(0, "PFWBLOCK_DEBUG", "END")
    return inputlist



#######################################################################
def finish_wrapper_inst(config, modname, wrapperinst):
    """ Finish creating wrapper instances with tasks like making input and output filenames """
    
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    moddict = config[SW_MODULESECT][modname] 
    for winst in wrapperinst.values():
        # create currvals
        currvals = { 'curr_module': modname, PF_WRAPNUM: winst[PF_WRAPNUM]}

        (found, loopkeys) = config.search('wrapperloop', 
                           {PF_CURRVALS: {'curr_module': modname},
                            'required': False, 'interpolate': True})
        if found:
            loopkeys = fwsplit(loopkeys.lower())
            for key in loopkeys:
                currvals[key] = winst[key]

        if SW_FILESECT in moddict:
            for fname, fdict in moddict[SW_FILESECT].items(): 
                fwdebug(0, 'PFWBLOCK_DEBUG', '%s: working on file: %s' % (winst[PF_WRAPNUM], fname))
#                if fname not in winst[IW_FILESECT]:
#                    winst[IW_FILESECT][fname] = {}

                # if didn't get filename from query code, generate filename from pattern
#                if 'fullname' not in winst[IW_FILESECT][fname]:
#                    if 'filename' in moddict[SW_FILESECT][fname]:
#                        winst[IW_FILESECT][fname]['filename'] = config.search('filename', {PF_CURRVALS: currvals, 
#                                                                                           'searchobj': moddict[SW_FILESECT][fname], 
#                                                                                           'expand': True, 
#                                                                                           'required': True,
#                                                                                           'interpolate':True})[1]
#                    else:
#                        print "finish_wrapper_inst: creating filename for", fname 
#                        winst[IW_FILESECT][fname]['filename'] = config.get_filename(None, {PF_CURRVALS: currvals, 
#                                                                            'searchobj': fdict,
#                                                                            'expand': True}) 
#
#                    # Add runtime path to filename
#                    print "finish_wrapper_inst: creating path for", fname 
#                    path = config.get_filepath('runtime', None, {PF_CURRVALS: currvals, 'searchobj': fdict})
#                    if type(winst[IW_FILESECT][fname]['filename']) is list:
#                        print fname,"filename is a list"
#                        winst[IW_FILESECT][fname]['fullname'] = []
#                        print fname,"number of names = ", len(winst[IW_FILESECT][fname]['filename'])
#                        for f in winst[IW_FILESECT][fname]['filename']:
#                            print path,"+",f
#                            winst[IW_FILESECT][fname]['fullname'].append("%s/%s" % (path, f))
#    
#                        winst[IW_FILESECT][fname]['fullname'] = ','.join(winst[IW_FILESECT][fname]['filename'])
#                    else:
#                        print "Adding path to filename for", fname
#                        winst[IW_FILESECT][fname]['fullname'] = "%s/%s" % (path, winst[IW_FILESECT][fname]['filename'])

                fwdebug(3, "PFWBLOCK_DEBUG", "fullname = %s" % (winst[IW_FILESECT][fname]['fullname']))

                
                if 'filetype' in fdict:
                    winst[IW_FILESECT][fname]['filetype'] = fdict['filetype']

                if 'req_metadata' in fdict:
                    fwdebug(3, "PFWBLOCK_DEBUG", "copying req_metadata %s" % (fname))
                    winst[IW_FILESECT][fname]['req_metadata'] = copy.deepcopy(fdict['req_metadata'])
                else:
                    fwdebug(3, "PFWBLOCK_DEBUG", "no req_metadata %s" % (fname))

                if 'opt_metadata' in fdict:
                    fwdebug(3, "PFWBLOCK_DEBUG", "copying opt_metadata %s" % (fname))
                    winst[IW_FILESECT][fname]['opt_metadata'] = copy.deepcopy(fdict['opt_metadata'])
                else:
                    fwdebug(3, "PFWBLOCK_DEBUG", "no opt_metadata %s" % (fname))

            fwdebug(4, "PFWBLOCK_DEBUG", "fdict = %s" % fdict)
            fwdebug(4, "PFWBLOCK_DEBUG", "winst[%s] = %s" % (IW_FILESECT,  winst[IW_FILESECT]))

        if SW_LISTSECT in moddict:
            winst[IW_LISTSECT] = {}
            for lname, ldict in moddict[SW_LISTSECT].items():
                winst[IW_LISTSECT][lname] = {}
                winst[IW_LISTSECT][lname]['listname'] = config.search('listname', {PF_CURRVALS: currvals, 
                                                                              'searchobj': ldict, 
                                                                              'required': True, 
                                                                              'interpolate':True})[1]
                create_simple_list(config, lname, ldict, currvals)
    fwdebug(0, "PFWBLOCK_DEBUG", "END")


#######################################################################
def add_file_metadata(config, modname):
    """ add file metadata sections to a single file section from a module"""

    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    fwdebug(0, "PFWBLOCK_DEBUG", "Working on module " + modname)
    moddict = config[SW_MODULESECT][modname]
    
    if SW_FILESECT in moddict:
        for k in moddict:
            if re.search("^%s\d+$" % SW_EXECPREFIX, k) and SW_OUTPUTS in moddict[k]:
                for outfile in fwsplit(moddict[k][SW_OUTPUTS]):
                    fwdebug(3, "PFWBLOCK_DEBUG", "Working on output file " + outfile)
                    m = re.match('%s.(\w+)' % SW_FILESECT, outfile)
                    if m:
                        fname = m.group(1)
                        fwdebug(3, "PFWBLOCK_DEBUG", "Working on file " + fname)
                        fdict = moddict[SW_FILESECT][fname]
            
                        filetype = fdict['filetype'].lower()
                        fdict['req_metadata'] = OrderedDict()

                        if filetype in config['filetype_metadata']:
                            if 'r' in config['filetype_metadata'][filetype]:
                                if 'h' in config['filetype_metadata'][filetype]['r']:
                                    fdict['req_metadata']['headers'] = ','.join(config['filetype_metadata'][filetype]['r']['h'].keys())
                                if 'c' in config['filetype_metadata'][filetype]['r']:
                                    fdict['req_metadata']['compute'] = ','.join(config['filetype_metadata'][filetype]['r']['c'].keys())
#                                if 'w' in config['filetype_metadata'][filetype]['r']:
#                                    fdict['req_metadata']['wcl'] = ','.join(config['filetype_metadata'][filetype]['r']['w'].keys())

                            if 'o' in config['filetype_metadata'][filetype]:
                                fdict['opt_metadata'] = OrderedDict()
                                if 'h' in config['filetype_metadata'][filetype]['o']:
                                    fdict['opt_metadata']['headers'] = ','.join(config['filetype_metadata'][filetype]['o']['h'].keys())
                                if 'c' in config['filetype_metadata'][filetype]['o']:
                                    fdict['opt_metadata']['compute'] = ','.join(config['filetype_metadata'][filetype]['o']['c'].keys())
#                                if 'w' in config['filetype_metadata'][filetype]['o']:
#                                    fdict['opt_metadata']['wcl'] = ','.join(config['filetype_metadata'][filetype]['o']['w'].keys())
                
                        if 'wcl' not in fdict['req_metadata']:
                            fdict['req_metadata']['wcl'] = ''
                        else:
                            fdict['req_metadata']['wcl'] += ','

            
                        wclsect = "%s.%s" % (IW_FILESECT, fname)
                        fdict['req_metadata']['wcl'] += '%(sect)s.fullname,%(sect)s.filename,%(sect)s.filetype' % ({'sect':wclsect})
                    else:
                        fwdebug(3, "PFWBLOCK_DEBUG", "output file %s doesn't have definition (%s) " % (k, SW_FILESECT))

                fwdebug(3, "PFWBLOCK_DEBUG", "output file dictionary for %s = %s" % (outfile, fdict))
                
            else:
                fwdebug(3, "PFWBLOCK_DEBUG", "No was_generated_by for %s" % (k))

    else:
        fwdebug(3, "PFWBLOCK_DEBUG", "No file section (%s)" % SW_FILESECT)
        
    fwdebug(0, "PFWBLOCK_DEBUG", "END")

    #exit(0)
    
                




#######################################################################
def write_jobwcl(config, jobnum, numexpwrap):
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
            }
    if convertBool(config[PF_USE_DB_OUT]):
        if 'des_services' in config and config['des_services'] is not None: 
            jobwcl['des_services'] = config['des_services']
        jobwcl['des_db_section'] = config['des_db_section']

    jobwcl['filetype_metadata'] = config['filetype_metadata']
    jobwcl[IW_EXEC_DEF] = config[SW_EXEC_DEF]

    with open(jobwclfile, 'w') as wclfh:
        wclutils.write_wcl(jobwcl, wclfh, True, 4)

    fwdebug(0, "PFWBLOCK_DEBUG", "END")
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
    fwdebug(0, "PFWBLOCK_DEBUG", "END")


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
        print "\tloopkeys = ", loopkeys
        loopkeys = fwsplit(loopkeys.lower())
        loopkeys.sort()  # sort so can make same key easily

        valproduct = itertools.product(*loopvals)
        for instvals in valproduct:
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

    print "\tNumber wrapper inst: ", len(wrapperinst)
    fwdebug(0, "PFWBLOCK_DEBUG", "END")
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

    fwdebug(0, "PFWBLOCK_DEBUG", "END")





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
                raise Exception("Error: 0 lines in master list")

            sublists = {}
            keys = ()
            if 'loopkey' in sdict:
                keys = fwsplit(sdict['loopkey'].lower())
                keys.sort()
            elif 'match' in sdict:
                keys = fwsplit(sdict['match'].lower())
                keys.sort()

            if len(keys) > 0: 
                sdict['keyvals'] = {} 
                print "\t%s-%s: dividing by %s" % (modname, sname, keys)
                for linenick, linedict in master['list'][PF_LISTENTRY].items():
                    index = ""
                    for key in keys:
                        val = get_value_from_line(linedict, key, None, 1).strip()
                        index += val + '_'
                        if key not in sdict['keyvals']:
                            sdict['keyvals'][key] = []
                        sdict['keyvals'][key].append(val)
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

    fwdebug(0, "PFWBLOCK_DEBUG", "END")


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

        if 'loopobj' in moddict:
            print "\tloopobj =", moddict['loopobj']
            sdict = pfwutils.get_wcl_value(moddict['loopobj'], moddict) 
            for key in loopkeys:
                val = sdict['loopvals'][key]
                loopvals.append(val)
        else:
            print "\tdefaulting to wrapperloop"
            loopvals = []
            for key in loopkeys:
                (found, val) = config.search(key, 
                            {PF_CURRVALS: {'curr_module': modname},
                            'required': True, 
                            'interpolate': True})
                val = fwsplit(val)
                loopvals.append(val)

    return loopvals
    fwdebug(0, "PFWBLOCK_DEBUG", "END")


#############################################################
def get_value_from_line(line, key, nickname=None, numvals=None):
    """ Return value from a line in master list """
    fwdebug(1, "PFWBLOCK_DEBUG", "BEG")
    # returns None if 0 matches
    #         scalar value if 1 match
    #         array if > 1 match

    # since values could be repeated across files in line, 
    # create hash of values to get unique values
    valhash = {}

    if '.' in key:
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
        print "\tnumber found doesn't match requested (%s)\n" % (numvals)
        if nickname is not None:
            print "\tnickname =", nickname

        print "\tvalue to find:", key
        print "\tline:", line
        print "\tvalarr:", valarr
        raise Exception("Aborting\n")

    if len(valarr) == 0:
        retval = None
    elif numvals == 1 or len(valarr) == 1:
        retval = valarr[0]
    else:
        retval = valarr

    fwdebug(1, "PFWBLOCK_DEBUG", "END")
    return retval


#######################################################################
# Assumes currvals includes specific values (e.g., band, ccd)
def create_single_wrapper_wcl(config, modname, wrapinst):
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    fwdebug(3, "PFWBLOCK_DEBUG", "\twrapinst=%s" % wrapinst)

    currvals = {'curr_module': modname, PF_WRAPNUM: wrapinst[PF_WRAPNUM]}


    wrapperwcl = {}

    # file is optional
    if IW_FILESECT in wrapinst:
        wrapperwcl[IW_FILESECT] = copy.deepcopy(wrapinst[IW_FILESECT])
        fwdebug(3, "PFWBLOCK_DEBUG", "\tfile=%s" % wrapperwcl[IW_FILESECT])

    # list is optional
    if IW_LISTSECT in wrapinst:
        wrapperwcl[IW_LISTSECT] = copy.deepcopy(wrapinst[IW_LISTSECT])
        fwdebug(3, "PFWBLOCK_DEBUG", "\tlist=%s" % wrapperwcl[IW_LISTSECT])


    # do we want exec_list variable?
    fwdebug(3, "PFWBLOCK_DEBUG", "\tSW_EXECPREFIX=%s" % SW_EXECPREFIX)
    numexec = 0
    modname = currvals['curr_module']
    for mkey, mval in config[SW_MODULESECT][modname].items():
        fwdebug(3, "PFWBLOCK_DEBUG", "\tsearching for exec prefix in %s" % mkey)
        
        if re.search("^%s\d+$" % SW_EXECPREFIX, mkey):
            fwdebug(4, "PFWBLOCK_DEBUG", "\tFound exec prefex %s" % mkey)

            numexec += 1
            iwkey = mkey.replace(SW_EXECPREFIX, IW_EXECPREFIX)
            wrapperwcl[iwkey] = {}
            for exkey, exval in mval.items():
                if exkey == SW_INPUTS:
                    iwexkey = IW_INPUTS
                elif exkey == SW_OUTPUTS:
                    iwexkey = IW_OUTPUTS
                elif exkey == SW_ANCESTRY:
                    iwexkey = IW_ANCESTRY
                else:
                    iwexkey = exkey
    
                if exkey != 'cmdline':
                    wrapperwcl[iwkey][iwexkey] = config.search(exkey, {PF_CURRVALS: currvals, 'searchobj': mval,
                                                            'required': True, 'interpolate': True})[1]

            if 'cmdline' in mval:
                wrapperwcl[iwkey]['cmdline'] = copy.deepcopy(mval['cmdline'])

    if SW_WRAPSECT in config[SW_MODULESECT][modname]:
        fwdebug(0, 'PFWBLOCK_DEBUG', "Copying wrapper section (%s)"% SW_WRAPSECT)
        wrapperwcl[IW_WRAPSECT] = copy.deepcopy(config[SW_MODULESECT][modname][SW_WRAPSECT])

    if IW_WRAPSECT not in wrapperwcl:
        fwdebug(1, 'PFWBLOCK_DEBUG', "%s (%s): Initializing wrapper section (%s)"% (modname, wrapinst[PF_WRAPNUM], IW_WRAPSECT))
        wrapperwcl[IW_WRAPSECT] = {}
    wrapperwcl[IW_WRAPSECT]['pipeline'] = config['pipeline']
    wrapperwcl[IW_WRAPSECT]['pipever'] = config['pipever']

    outputwcl_file = config.get_filename('outputwcl', 
                                {PF_CURRVALS: currvals,
                                 'required': True, 'interpolate': True})
    outputwcl_path = config.get_filepath('runtime', 'outputwcl', {PF_CURRVALS: currvals,
                                     'required': True, 'interpolate': True})
    wrapperwcl[IW_WRAPSECT]['outputwcl'] = "%s/%s" % (outputwcl_path, outputwcl_file)


    wrapperwcl[IW_WRAPSECT]['tmpfile_prefix'] =  config.search('tmpfile_prefix',
                                {PF_CURRVALS: currvals,
                                 'required': True, 'interpolate': True})[1]


    if numexec == 0:
        wclutils.write_wcl(config[SW_MODULESECT][modname])
        raise Exception("Error:  Could not find an exec section for module %s" % modname)
        

    fwdebug(0, "PFWBLOCK_DEBUG", "END")


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
#    fwdebug(1, "PFWBLOCK_DEBUG", "END")
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
    fwdebug(1, "PFWBLOCK_DEBUG", "END")
               


#######################################################################
def create_module_wrapper_wcl(config, modname, wrapinst):
    """ Create wcl for wrapper instances for a module """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    tasks = []

    if modname not in config[SW_MODULESECT]:
        raise Exception("Error: Could not find module description for module %s\n" % (modname))

    inputwclfilepath = config.get_filepath('runtime', 'inputwcl', 
                                {PF_CURRVALS: {'curr_module': modname}})

    if not os.path.exists(inputwclfilepath):
        os.makedirs(inputwclfilepath)


    for inst in wrapinst.values():
        wrapperwcl = create_single_wrapper_wcl(config, modname, inst)

        inputwclfilename = config.get_filename('inputwcl', {PF_CURRVALS: 
                {'curr_module': modname}, 
                 'searchobj': inst})
        inputwcl = inputwclfilepath + '/' + inputwclfilename


        (found, wrappername) = config.search('wrappername',
                {PF_CURRVALS: {'curr_module': modname}, 'searchobj': inst,
                'required': True, 'interpolate': True})


        logfilename = config.get_filename('log', {PF_CURRVALS: 
                {'curr_module': modname}, 
                 'searchobj': inst})
        logfilepath = config.get_filepath('runtime', 'log', {PF_CURRVALS: 
                {'curr_module': modname}, 
                 'searchobj': inst})
        logfile = logfilepath + '/' + logfilename


        #fix_globalvars(config, wrapperwcl, modname, inst)
        
        translate_sw_iw(config, wrapperwcl, modname, inst)
        add_needed_values(config, modname, inst, wrapperwcl)

        write_wrapper_wcl(config, inputwcl, wrapperwcl) 

        # Add this wrapper execution to list
        tasks.append([inst[PF_WRAPNUM], wrappername, inputwcl, logfile])
    fwdebug(0, "PFWBLOCK_DEBUG", "END")

    return tasks

#######################################################################
def create_wrapper_wcl(config, wrapinst):
    """ Create wcl for single wrapper instance """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    print "wrapinst.keys = ", wrapinst.keys()
    modulelist = fwsplit(config[SW_MODULELIST].lower())
    tasks = []

    os.mkdir('wcl')
    for modname in modulelist:
        print "Creating wrapper wcl for module '%s'" % modname
        if modname not in config[SW_MODULESECT]:
            raise Exception("Error: Could not find module description for module %s\n" % (modname))

        if modname not in wrapinst:
            print "Error: module not in wrapinst"
            print wrapinst.keys()

        for inst in wrapinst[modname].values():
            wrapperwcl = create_single_wrapper_wcl(config, modname, inst)
            (found, inputwcl) = config.search('inputwcl',
                    {PF_CURRVALS: {'curr_module': modname}, 'searchobj': inst,
                    'required': True, 'interpolate': True})

            (found, wrappername) = config.search('wrappername',
                    {PF_CURRVALS: {'curr_module': modname}, 'searchobj': inst,
                    'required': True, 'interpolate': True})

            (found, logfile) = config.search('logfile',
                    {PF_CURRVALS: {'curr_module': modname}, 'searchobj': inst,
                    'required': True, 'interpolate': True})

            #fix_globalvars(config, wrapperwcl, modname, inst)
            add_needed_values(config, modname, inst, wrapperwcl)

            write_wrapper_wcl(config, inputwcl, wrapperwcl) 

            # Add this wrapper execution to list
            tasks.append((wrappername, inputwcl, logfile))
    fwdebug(0, "PFWBLOCK_DEBUG", "END")
    return tasks


def write_runjob_script(config):
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")

    jobnum = config[PF_JOBNUM]
    print "PF_JOBNUM = ", PF_JOBNUM
    print "jobnum = ", jobnum
    print "jobnum = ", int(jobnum)
    jobdir = '%s_j%04d' % (config['submit_run'], int(jobnum))
    print "The jobdir =", jobdir

    scriptstr = """#!/bin/sh
export SHELL=/bin/bash    # needed for setup to work in Condor environment
source %(eups)s 
echo "Using eups to setup up %(pipe)s %(ver)s"
d1=`/bin/date "+%%s"` 
setup %(pipe)s %(ver)s
mystat=$?
d2=`/bin/date "+%%s"` 
echo "\t$((d2-d1)) secs"
if [ $mystat != 0 ]; then
    echo "eups setup had non-zero exit code ($mystat)"
    exit $mystat 
fi
echo "PATH = " $PATH
echo "PYTHONPATH = " $PYTHONPATH


initdir=`/bin/pwd`
echo ""
echo "Initial condor job directory = " $initdir
echo "Files copied over by condor:"
ls -l
""" % ({'eups': config['setupeups'], 
        'pipe':config['pipeline'],
        'ver':config['pipever']})
   
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

    scriptstr += """

echo ""
echo "Untaring input wcl file: $1"
d1=`/bin/date "+%s"` 
tar -xzf $initdir/$1
d2=`/bin/date "+%s"` 
echo "\t$((d2-d1)) secs"

# copy file so I can test by hand after job
cp $initdir/$2 $2
cp $initdir/$3 $3

echo ""
echo "Calling pfwrunjob.py"
echo "cmd> ${PROCESSINGFW_DIR}/libexec/pfwrunjob.py --config $2 $3"
${PROCESSINGFW_DIR}/libexec/pfwrunjob.py --config $2 $3
""" 

    scriptfile = config.get_filename('runjob') 
    with open(scriptfile, 'w') as scriptfh:
        scriptfh.write(scriptstr)

    os.chmod(scriptfile, stat.S_IRWXU | stat.S_IRWXG)
    fwdebug(0, "PFWBLOCK_DEBUG", "END")
    return scriptfile



#######################################################################
def create_jobmngr_dag(config, dagfile, scriptfile, tarfile, tasksfile, jobwclfile):
    """ Write job manager DAG file """

    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")
    condorfile = create_runjob_condorfile(config)
    pfwdir = config['processingfw_dir']
    tjpad = "%04d" % (int(config[PF_JOBNUM]))
    blockname = config['curr_block']
    args = "%s %s %s" % (tarfile, jobwclfile, tasksfile)
    transinput = "%s,%s,%s" % (tarfile, tasksfile, jobwclfile)
    with open(dagfile, 'w') as dagfh:
        dagfh.write('JOB %s %s\n' % (tjpad, condorfile))
        dagfh.write('VARS %s jobnum="%s"\n' % (tjpad, tjpad))
        dagfh.write('VARS %s exec="%s"\n' % (tjpad, scriptfile))
        dagfh.write('VARS %s args="%s"\n' % (tjpad, args))
        dagfh.write('VARS %s transinput="%s"\n' % (tjpad, transinput))
        dagfh.write('SCRIPT pre %s %s/libexec/logpre.py config.des %s j $JOB\n' % (tjpad, pfwdir, blockname))
        dagfh.write('SCRIPT post %s %s/libexec/logpost.py config.des %s j $JOB $RETURN\n' % (tjpad, pfwdir, blockname)) 

#    pfwcondor.add2dag(dagfile, config.get_dag_cmd_opts(), config.get_condor_attributes("jobmngr"), None, sys.stdout)
    fwdebug(0, "PFWBLOCK_DEBUG", "END")


#######################################################################
def tar_inputwcl(config):
    """ Tar the input wcl directory """
    inputwcltar = config.get_filename('inputwcltar')
    inputwcldir = config.get_filepath('runtime', 'inputwcl', 
                      {PF_CURRVALS: {'modulename': ''}})
    pfwutils.tar_dir(inputwcltar, inputwcldir)
    return inputwcltar


#######################################################################
def create_runjob_condorfile(config):
    """ Write runjob condor description file for target job """
    fwdebug(0, "PFWBLOCK_DEBUG", "BEG")

    blockbase = config.get_filename('block', {PF_CURRVALS: {'flabel': 'runjob', 'fsuffix':''}})
#    initialdir = "../%s_tjobs" % config['blockname']
#    condorfile = '%s/%scondor' % (initialdir, condorbase)

    condorfile = '%scondor' % (blockbase)
    
    jobbase = config.get_filename('job', {PF_CURRVALS: {PF_JOBNUM:'$(jobnum)', 'flabel': 'runjob', 'fsuffix':''}})
    jobattribs = { 
                'executable':'$(exec)', 
                'arguments':'$(args)',
#               'remote_initialdir':remote_initialdir, 
#                'initialdir':initialdir,
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
        fwdie("Error:  Missing gridtype")
    else:
        targetinfo['gridtype'] = targetinfo['gridtype'].lower()
        print 'GRIDTYPE =', targetinfo['gridtype']

    reqs = []
    if targetinfo['gridtype'] == 'condor':
        jobattribs['universe'] = 'vanilla'

        if 'concurrency_limits' in config:
            jobattribs['concurrency_limits'] = config['concurrency_limits']

        if 'batchtype' not in targetinfo:
            fwdie("Error: Missing batchtype")
        else:
            targetinfo['batchtype'] = targetinfo['batchtype'].lower()

        if targetinfo['batchtype'] == 'glidein':
            if 'uiddomain' not in config:
                fwdie("Error: Cannot determine uiddomain for matching to a glidein")
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
                fwdie("Error:  Cannot determine machine name (missing loginhost and gridhost)")

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

    fwdebug(0, "PFWBLOCK_DEBUG", "END")
    return condorfile


