#!/usr/bin/env python
# $Id:$
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
#import processingfw.pfwxml as pfwxml
import intgutils.wclutils as wclutils
import processingfw.pfwutils as pfwutils
import processingfw.pfwcondor as pfwcondor
from processingfw.pfwwrappers import write_wrapper_wcl


def create_simple_list(config, lname, ldict, currvals):
    """ Create simple filename list file based upon patterns """
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "BEG")
    listname = config.search('listname', 
                            {'currentvals': currvals, 
                             'searchobj': ldict, 
                             'required': True, 
                             'interpolate': True})[1]

    filename = config.get_filename(None,
                            {'currentvals': currvals, 
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
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "END")

                        
def assign_data_wrapper_inst(config, modname, wrapperinst):
    """ Assign data like files and lists to wrapper instances """
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    moddict = config['module'][modname] 
    currvals = { 'curr_module': modname}
    (found, loopkeys) = config.search('wrapperloop', 
                       {'currentvals': currvals,
                        'required': False, 'interpolate': True})
    if found:
        loopkeys = pfwutils.pfwsplit(loopkeys.lower())
    else:
        loopkeys = []

    inputlist = {}
    for winst in wrapperinst.values():
        # create currvals
        currvals = { 'curr_module': modname, 'wrapnum': winst['wrapnum']}
        for key in loopkeys:
            currvals[key] = winst[key]
        pfwutils.debug(6, "PFWBLOCK_DEBUG", "currvals " + str(currvals))

        if 'file' in moddict:
            winst['file'] = {}
            for fname, fdict in moddict['file'].items(): 
                pfwutils.debug(3, "PFWBLOCK_DEBUG", "Working on file "+fname)
                winst['file'][fname] = {}
                if 'sublists' in fdict:  # files came from query
                    if len(fdict['sublists'].keys()) > 1:
#                        print fdict['sublists'].keys()
                        matchkeys = pfwutils.pfwsplit(fdict['match'])
                        matchkeys.sort()
                        index = ""
                        for mkey in matchkeys:
                            if mkey not in winst:
                                raise Exception("Cannot find match key %s in winst %s" % (mkey, winst))
                            index += winst[mkey] + '_'
                        pfwutils.debug(3, "PFWBLOCK_DEBUG", "sublist index = "+index)
                        if index not in fdict['sublists']:
                            raise Exception("Cannot find sublist matching "+index)
                        sublist = fdict['sublists'][index]
                    else:
                        sublist = fdict['sublists'].values()[0]

                    if len(sublist['list']['line']) > 1:
                        raise Exception("more than 1 line to choose from for file" + sublist['list']['line'])
                    line = sublist['list']['line'].values()[0]
                    if 'file' not in line:
                        raise Exception("0 file in line" + str(line))
                        
                    if len(line['file']) > 1:
                        raise Exception("more than 1 file to choose from for file" + line['file'])
                    finfo = line['file'].values()[0]
                    print "finfo =", finfo

                    # Add runtime path to filename
                    path = config.get_filepath('runtime', None, {'currentvals': currvals, 'searchobj': fdict})
                    winst['file'][fname]['filename'] = "%s/%s" % (path, finfo['filename'])
                    inputlist[winst['file'][fname]['filename']] = True

                    #winst['file'][fname]['filename'] = finfo['filename']
                    print "Assigned filename for fname %s (%s)" % (fname, finfo['filename'])
#                else:
#                    winst['file'][fname]['filename'] = config.get_filename(None, {'currentvals': currvals, 'searchobj': fdict}) 
#                    if type(winst['file'][fname]['filename']) is list:
#                        winst['file'][fname]['filename'] = ','.join(winst['file'][fname]['filename'])
#                if 'req_metadata' in fdict:
#                    winst['file'][fname]['req_metadata'] = copy.deepcopy(fdict['req_metadata'])

        if 'list' in moddict:
            winst['list'] = {}
            for lname, ldict in moddict['list'].items():
                winst['list'][lname] = {}
                winst['list'][lname]['listname'] = config.search('listname', {'currentvals': currvals, 
                                                                              'searchobj': ldict, 
                                                                              'required': True, 
                                                                              'interpolate':True})[1]
                create_simple_list(config, lname, ldict, currvals)
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "END")
    return inputlist



def finish_wrapper_inst(config, modname, wrapperinst):
    """ Finish creating wrapper instances with tasks like making input and output filenames """
    
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    moddict = config['module'][modname] 
    for winst in wrapperinst.values():
        # create currvals
        currvals = { 'curr_module': modname, 'wrapnum': winst['wrapnum']}

        (found, loopkeys) = config.search('wrapperloop', 
                           {'currentvals': {'curr_module': modname},
                            'required': False, 'interpolate': True})
        if found:
            loopkeys = pfwutils.pfwsplit(loopkeys.lower())
            for key in loopkeys:
                currvals[key] = winst[key]

        if 'file' in moddict:
            for fname, fdict in moddict['file'].items(): 
                print "working on file:", fname
                if fname not in winst['file']:
                    winst['file'][fname] = {}

                # if didn't get filename from query code, generate filename from pattern
                if 'filename' not in winst['file'][fname]:
                    winst['file'][fname]['filename'] = config.get_filename(None, {'currentvals': currvals, 
                                                                           'searchobj': fdict,
                                                                           'expand': True}) 
                    path = config.get_filepath('runtime', None, {'currentvals': currvals, 'searchobj': fdict})
                    if type(winst['file'][fname]['filename']) is list:
                        for i in range(0, len(winst['file'][fname]['filename'])):
                            # Add runtime path to filename
                            winst['file'][fname]['filename'][i] = "%s/%s" % (path, winst['file'][fname]['filename'][i])

                        winst['file'][fname]['filename'] = ','.join(winst['file'][fname]['filename'])
                    else:
                        winst['file'][fname]['filename'] = "%s/%s" % (path, winst['file'][fname]['filename'])

                if 'req_metadata' in fdict:
                    winst['file'][fname]['req_metadata'] = copy.deepcopy(fdict['req_metadata'])

        if 'list' in moddict:
            winst['list'] = {}
            for lname, ldict in moddict['list'].items():
                winst['list'][lname] = {}
                winst['list'][lname]['listname'] = config.search('listname', {'currentvals': currvals, 
                                                                              'searchobj': ldict, 
                                                                              'required': True, 
                                                                              'interpolate':True})[1]
                create_simple_list(config, lname, ldict, currvals)
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "END")

def add_needed_values(config, modname, wrapinst, wrapwcl):
    """ Make sure all variables in the wrapper instance have values in the wcl """
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    neededvals = {}

    # start with specified
    if 'req_vals' in config['module'][modname]: 
        for rv in pfwutils.pfwsplit(config['module'][modname]['req_vals']):
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
            if type(neededvals[nval]) is bool:
                if '.' not in nval:
                    if ':' in nval:
                        nval = nval.split(':')[0]
                    (found, val) = config.search(nval, 
                                   {'currentvals': {'curr_module': modname},
                                    'searchobj': wrapinst, 
                                    'required': True, 
                                    'interpolate': False})
                else:
                    val = pfwutils.get_wcl_value(nval, wrapwcl)

                neededvals[nval] = val
                viter = [m.group(1) for m in re.finditer('(?i)\$\{([^}]+)\}', val)]
                for vstr in viter:
                    if vstr not in neededvals:
                        neededvals[vstr] = True
                        done = False
                    
    if count >= maxtries:
        raise Exception("Error: exceeded maxtries")

    # add needed values to wrapper wcl
    for key, val in neededvals.items():
        pfwutils.set_wcl_value(key, val, wrapwcl)
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "END")


def create_wrapper_inst(config, modname, loopvals):
    """ Create set of empty wrapper instances """
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    wrapperinst = {}
    (found, loopkeys) = config.search('wrapperloop', 
                   {'currentvals': {'curr_module': modname},
                    'required': False, 'interpolate': True})
    wrapperinst = {}
    if found:
        print "\tloopkeys = ", loopkeys
        loopkeys = pfwutils.pfwsplit(loopkeys.lower())
        loopkeys.sort()  # sort so can make same key easily

        valproduct = itertools.product(*loopvals)
        for instvals in valproduct:
            config.inc_wrapnum()
            winst = {'wrapnum': config['wrapnum']}
            instkey = ""
            for k in range(0, len(loopkeys)):
                winst[loopkeys[k]] = instvals[k] 
                instkey += instvals[k] + '_'

                wrapperinst[instkey] = winst
    else:
        config.inc_wrapnum()
        wrapperinst['noloop'] = {'wrapnum': config['wrapnum']}

    print "\tNumber wrapper inst: ", len(wrapperinst)
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "END")
    return wrapperinst


#####################################################################
def read_master_lists(config, modname, modules_prev_in_list):
    """ Read master lists and files from files created earlier """
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "BEG %s" % modname)
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

            numlines = len(master['list']['line'])
            print "\t\t\tNumber of lines in dataset %s: %s\n" % (sname, numlines)

            if numlines == 0:
                raise Exception("ERROR: 0 lines in dataset %s in module %s" % (sname, modname))

            sdict['master'] = master
            sdict['depends'] = 0
        else:
            sdict['depends'] = 1

    pfwutils.debug(1, "PFWBLOCK_DEBUG", "END")





def create_sublists(config, modname):
    """ break master lists into sublists based upon match or divide_by """
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    print "\tModule %s" % (modname)
    dataset = config.combine_lists_files(modname)

    for (sname, sdict) in dataset:
        if 'master' in sdict:
            master = sdict['master']
            numlines = len(master['list']['line'])
            print "\t%s-%s: number of lines in master = %s" % (modname, sname, numlines)
            if numlines == 0:
                raise Exception("Error: 0 lines in master list")

            sublists = {}
            keys = ()
            if 'loopkey' in sdict:
                keys = pfwutils.pfwsplit(sdict['loopkey'].lower())
                keys.sort()
            elif 'match' in sdict:
                keys = pfwutils.pfwsplit(sdict['match'].lower())
                keys.sort()

            if len(keys) > 0: 
                sdict['keyvals'] = {} 
                print "\t%s-%s: dividing by %s" % (modname, sname, keys)
                for linenick, linedict in master['list']['line'].items():
                    index = ""
                    for key in keys:
                        val = get_value_from_line(linedict, key, None, 1).strip()
                        index += val + '_'
                        if key not in sdict['keyvals']:
                            sdict['keyvals'][key] = []
                        sdict['keyvals'][key].append(val)
                    if index not in sublists:
                        sublists[index] = {'list': {'line': {}}}
                    sublists[index]['list']['line'][linenick] = linedict
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

    pfwutils.debug(1, "PFWBLOCK_DEBUG", "END")


def get_wrapper_loopvals(config, modname):
    """ get the values for the wrapper loop keys """
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    loopvals = []

    moddict = config['module'][modname]
    (found, loopkeys) = config.search('wrapperloop', 
                   {'currentvals': {'curr_module': modname},
                    'required': False, 'interpolate': True})
    if found:
        print "\tloopkeys = ", loopkeys
        loopkeys = pfwutils.pfwsplit(loopkeys.lower())
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
                            {'currentvals': {'curr_module': modname},
                            'required': True, 
                            'interpolate': True})
                val = pfwutils.pfwsplit(val)
                loopvals.append(val)

    return loopvals
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "END")


#############################################################
def get_value_from_line(line, key, nickname=None, numvals=None):
    """ Return value from a line in master list """
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "BEG")
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

        print "\tvalue to find:", value
        print "\tline:", line
        print "\tvalarr:", valarr
        raise Exception("Aborting\n")

    if len(valarr) == 0:
        retval = None
    elif numvals == 1 or len(valarr) == 1:
        retval = valarr[0]
    else:
        retval = valarr

    pfwutils.debug(1, "PFWBLOCK_DEBUG", "END")
    return retval


# Assumes currvals includes specific values (e.g., band, ccd)
def create_single_wrapper_wcl(config, modname, wrapinst):
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "BEG %s" % modname)

    pfwutils.debug(3, "PFWBLOCK_DEBUG", "\twrapinst=%s" % wrapinst)

    currvals = {'curr_module': modname, 'wrapnum': wrapinst['wrapnum']}


    wrapperwcl = {}

    # file is optional
    if 'file' in wrapinst:
        wrapperwcl['file'] = copy.deepcopy(wrapinst['file'])
        pfwutils.debug(3, "PFWBLOCK_DEBUG", "\tfile=%s" % wrapperwcl['file'])

    # list is optional
    if 'list' in wrapinst:
        wrapperwcl['list'] = copy.deepcopy(wrapinst['list'])
        pfwutils.debug(3, "PFWBLOCK_DEBUG", "\tlist=%s" % wrapperwcl['list'])


    # do we want exec_list variable?
    modname = currvals['curr_module']
    for mkey, mval in config['module'][modname].items():
        if mkey.startswith('exec_'):
            wrapperwcl[mkey] = {}
            for exkey, exval in mval.items():
                if exkey != 'cmdline':
                    wrapperwcl[mkey][exkey] = config.search(exkey, {'currentvals': currvals, 'searchobj': mval,
                                                            'required': True, 'interpolate': True})[1]
            # copy cmdline as is???
            if 'cmdline' in mval:
                wrapperwcl[mkey]['cmdline'] = copy.deepcopy(mval['cmdline'])


    currvals['wcltype'] = 'output'
    wrapperwcl['wrapper'] = {}
    wrapperwcl['wrapper']['pipeline'] = config['pipeline']
    wrapperwcl['wrapper']['pipever'] = config['pipever']


    outputwcl_file = config.get_filename('wcl', 
                                {'currentvals': currvals,
                                 'required': True, 'interpolate': True})
    outputwcl_path = config.get_filepath('runtime', 'wcl', {'currentvals': currvals,
                                     'required': True, 'interpolate': True})
    wrapperwcl['wrapper']['outputfile'] = "%s/%s" % (outputwcl_path, outputwcl_file)


    wrapperwcl['wrapper']['tmpfile_prefix'] =  config.search('tmpfile_prefix',
                                {'currentvals': currvals,
                                 'required': True, 'interpolate': True})[1]

    pfwutils.debug(1, "PFWBLOCK_DEBUG", "END")

    return wrapperwcl


# Early version of wrapper code could not handle global variables
#   So the following code changed all global variables to 
#       variables in the wrapper section
#def fix_globalvars(config, wrapperwcl, modname, winst):
#    pfwutils.debug(1, "PFWBLOCK_DEBUG", "BEG %s" % modname)
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
#        wrapperwcl['wrapper'][wk] =  config.search(wk,
#                                {'currentvals': {'curr_module': modname},
#                                 'searchobj': winst,
#                                 'required': True, 
#                                 'interpolate': True})[1]
#    pfwutils.debug(1, "PFWBLOCK_DEBUG", "END")
#               



def create_module_wrapper_wcl(config, modname, wrapinst):
    """ Create wcl for wrapper instances for a module """
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "BEG %s" % modname)
    tasks = []

    if modname not in config['module']:
        raise Exception("Error: Could not find module description for module %s\n" % (modname))

    inputwclfilepath = config.get_filepath('runtime', 'wcl', 
                                {'currentvals': {'curr_module': modname, 
                                                 'wcltype': 'input'}})

    if not os.path.exists(inputwclfilepath):
        os.makedirs(inputwclfilepath)


    for inst in wrapinst.values():
        wrapperwcl = create_single_wrapper_wcl(config, modname, inst)

        inputwclfilename = config.get_filename('wcl', {'currentvals': 
                {'curr_module': modname, 'wcltype': 'input'}, 
                 'searchobj': inst})
        inputwcl = inputwclfilepath + '/' + inputwclfilename


        (found, wrappername) = config.search('wrappername',
                {'currentvals': {'curr_module': modname}, 'searchobj': inst,
                'required': True, 'interpolate': True})


        logfilename = config.get_filename('log', {'currentvals': 
                {'curr_module': modname}, 
                 'searchobj': inst})
        logfilepath = config.get_filepath('runtime', 'log', {'currentvals': 
                {'curr_module': modname}, 
                 'searchobj': inst})
        logfile = logfilepath + '/' + logfilename


        #fix_globalvars(config, wrapperwcl, modname, inst)
        add_needed_values(config, modname, inst, wrapperwcl)

        write_wrapper_wcl(config, inputwcl, wrapperwcl) 

        # Add this wrapper execution to list
        tasks.append([inst['wrapnum'], wrappername, inputwcl, logfile])
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "END")

    return tasks

def create_wrapper_wcl(config, wrapinst):
    """ Create wcl for single wrapper instance """
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "BEG")
    print "wrapinst.keys = ", wrapinst.keys()
    modulelist = pfwutils.pfwsplit(config['module_list'].lower())
    tasks = []

    os.mkdir('wcl')
    for modname in modulelist:
        print "Creating wrapper wcl for module '%s'" % modname
        if modname not in config['module']:
            raise Exception("Error: Could not find module description for module %s\n" % (modname))

        if modname not in wrapinst:
            print "Error: module not in wrapinst"
            print wrapinst.keys()

        for inst in wrapinst[modname].values():
            wrapperwcl = create_single_wrapper_wcl(config, modname, inst)
            (found, inputwcl) = config.search('inputwcl',
                    {'currentvals': {'curr_module': modname}, 'searchobj': inst,
                    'required': True, 'interpolate': True})

            (found, wrappername) = config.search('wrappername',
                    {'currentvals': {'curr_module': modname}, 'searchobj': inst,
                    'required': True, 'interpolate': True})

            (found, logfile) = config.search('logfile',
                    {'currentvals': {'curr_module': modname}, 'searchobj': inst,
                    'required': True, 'interpolate': True})

            #fix_globalvars(config, wrapperwcl, modname, inst)
            add_needed_values(config, modname, inst, wrapperwcl)

            write_wrapper_wcl(config, inputwcl, wrapperwcl) 

            # Add this wrapper execution to list
            tasks.append((wrappername, inputwcl, logfile))
    pfwutils.debug(1, "PFWBLOCK_DEBUG", "END")
    return tasks


def write_runjob_script(config):
    jobnum = 1
    jobdir = 'r%sp%02d_j%04d' % (config['reqnum'], 
                                 int(config['attnum']),
                                 int(jobnum))
    print "The jobdir =", jobdir

    scriptstr = """#!/bin/sh
source %(eups)s 
echo "Using eups to setup up %(pipe)s %(ver)s"
setup %(pipe)s %(ver)s
echo "PATH = " $PATH
echo "PYTHONPATH = " $PYTHONPATH

initdir=`/bin/pwd`
echo ""
echo "Initial condor job directory = " $initdir
echo "Files copied over by condor:"
ls -l
""" % ({'eups': config['eups_setup'], 
        'pipe':config['pipeline'],
        'ver':config['pipever']})
   
    if 'runtime_root' in config:
        rdir = config['runtime_root'] + '/' + jobdir
        print "rdir =", rdir
        scriptstr += """
echo ""
echo "Making run directory in runtime_root: %(rdir)s"
if [ ! -e %(rdir)s ]; then
    mkdir -p %(rdir)s
fi
cd %(rdir)s
        """ % ({'rdir': rdir})

    scriptstr += """

echo ""
echo "Untaring input wcl file: $1"
time tar -xzvf $initdir/$1

echo ""
echo "Calling pfwrunjob.py"
${PROCESSINGFW_DIR}/libexec/pfwrunjob.py $initdir/$2
""" 

    scriptfile = config.get_filename('jobscript')
    with open(scriptfile, 'w') as scriptfh:
        scriptfh.write(scriptstr)

    os.chmod(scriptfile, stat.S_IRWXU | stat.S_IRWXG)
    return scriptfile


def create_runjob_condorfile(config):
    """ Write runjob condor description file for target job """
    condorbase = config.get_filename('block', {'currentvals': {'filetype': 'runjob', 'suffix':''}})
    condorfile = '%scondor' % condorbase
    
    jobattribs = { 
                'executable':'$(exec)', 
                'arguments':'$(args)',
#               'remote_initialdir':remote_initialdir, 
#               'initialdir':initialdir,
#               'transfer_output_files': '$(jobnum).pipeline.log',
#                'should_transfer_files': 'IF_NEEDED',
                'when_to_transfer_output': 'ON_EXIT_OR_EVICT',
                'transfer_input_files': '$(transinput)', 
                'transfer_executable': 'True',
                'notification': 'Never',
                'output':'%sout' % condorbase,
                'error':'%serr' % condorbase,
                'log': '%slog' % condorbase,
                'getenv': 'true'
                 }
#    jobattribs.update(config.get_grid_info())
    userattribs = config.get_condor_attributes('$(jobnum)')
    gridinfo = config.get_grid_info()
    if 'localcondor' in gridinfo['batchtype'].lower():
        if 'login_host' in config:
            machine = config['login_host']
        elif 'grid_host' in config:
            machine = config['grid_host']
        else:
            raise Exception("Error:  Cannot determine machine name (missing login_host and grid_host)\n")

        jobattribs['requirements'] = 'machine == "%s"' % machine

    pfwcondor.write_condor_descfile('runjob', condorfile, jobattribs, userattribs)
#    pfwcondor.add2condor(condorfile, config.get_condor_attributes('$(jobnum)'), sys.stdout)

    return condorfile



def create_jobmngr_dag(config, dagfile, scriptfile, tarfile, tasksfile):
    """ Write job manager DAG file """

    condorfile = create_runjob_condorfile(config)
    pfwdir = config['processingfw_dir']
    tjpad = "TJ0001"
    blockname = config['curr_block']
    args = "%s %s" % (tarfile, tasksfile)
    transinput = "%s,%s" % (tarfile, tasksfile)
    with open(dagfile, 'w') as dagfh:
        dagfh.write('JOB %s %s\n' % (tjpad, condorfile))
        dagfh.write('VARS %s jobnum="%s"\n' % (tjpad, tjpad))
        dagfh.write('VARS %s exec="%s"\n' % (tjpad, scriptfile))
        dagfh.write('VARS %s args="%s"\n' % (tjpad, args))
        dagfh.write('VARS %s transinput="%s"\n' % (tjpad, transinput))
        dagfh.write('SCRIPT pre %s %s/libexec/logpre.py ../uberctrl/config.des %s j $JOB\n' % (tjpad, pfwdir, blockname))
        dagfh.write('SCRIPT post %s %s/libexec/logpost.py ../uberctrl/config.des %s j $JOB $RETURN\n' % (tjpad, pfwdir, blockname)) 

#    pfwcondor.add2dag(dagfile, config.get_dag_cmd_opts(), config.get_condor_attributes("jobmngr"), None, sys.stdout)


def tar_inputwcl(config):
    """ Tar the input wcl directory """
    inputwcltar = config.get_filename('block', 
                      {'currentvals': {'filetype': 'inputwcl', 
                                       'suffix':'tar.gz'}})

    inputwcldir = config.get_filepath('runtime', 'wcl', 
                      {'currentvals': {'wcltype': 'input', 
                                       'modulename': ''}})

    pfwutils.tar_dir(inputwcltar, inputwcldir)
    return inputwcltar

