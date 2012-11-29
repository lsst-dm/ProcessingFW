#!/usr/bin/env python

import re
import os
import inspect
import tarfile
from collections import OrderedDict
from collections import Mapping

""" Miscellaneous support functions for processing framework """

#######################################################################
def debug(msglvl, envdbgvar, msgstr):
    # environment debug variable overrides code set level
    if envdbgvar in os.environ:
        dbglvl = os.environ[envdbgvar]
    elif 'PFW_DEBUG' in os.environ:
        dbglvl = os.environ['PFW_DEBUG']
    else:
        dbglvl = 0

    if int(dbglvl) >= int(msglvl): 
        print "%s: %s" % (inspect.stack()[1][3], msgstr)

#######################################################################
def pfwsplit(fullstr, delim=','):
    """ Split by delim and trim substrs """
    fullstr = re.sub('[()]', '', fullstr) # delete parens if exist
    items = []
    for item in [x.strip() for x in fullstr.split(delim)]:
        if ':' in item:
            rangevals = pfwsplit(item, ':')
            items.extend(map(str, range(int(rangevals[0]), 
                                        int(rangevals[1])+1)))
        else:
            items.append(item)
    return items

#######################################################################
def traverse_wcl(wcl):
    debug(9, "PFWUTILS_DEBUG", "BEG")
    usedvars = {}
    for key, val in wcl.items():
        if type(val) is dict or type(val) is OrderedDict:
            uvars = traverse_wcl(val)
            if uvars is not None:
                usedvars.update(uvars)
        else:
            viter = [m.group(1) for m in re.finditer('(?i)\$\{([^}]+)\}', val)]
            for vstr in viter:
                if ':' in vstr:
                    vstr = vstr.split(':')[0]
                usedvars[vstr] = True
    debug(9, "PFWUTILS_DEBUG", "END")
    return usedvars

#######################################################################
def get_wcl_value(key, wcl):
    """ Return value of key from wcl, follows section notation """
    debug(9, "PFWUTILS_DEBUG", "BEG")
    val = wcl
    for k in key.split('.'):
        #print "get_wcl_value: k=", k
        val = val[k]
    debug(9, "PFWUTILS_DEBUG", "END")
    return val

#######################################################################
def set_wcl_value(key, val, wcl):
    """ sets value of key in wcl, follows section notation """
    debug(9, "PFWUTILS_DEBUG", "BEG")
    wclkeys = key.split('.')
    valkey = wclkeys.pop()
    wcldict = wcl
    for k in wclkeys:
        wcldict = wcldict[k]

    wcldict[valkey] = val
    debug(9, "PFWUTILS_DEBUG", "END")

#######################################################################
def tar_dir(filename, indir):
    """ Tars a directory """
    if filename.endswith('.gz'):
        mode = 'w:gz'
    else:
        mode = 'w'
    with tarfile.open(filename, mode) as tar:
        tar.add(indir)

#######################################################################
def untar_dir(filename, outputdir):
    """ Untars a directory """
    if filename.endswith('.gz'):
        mode = 'r:gz'
    else:
        mode = 'r'
    with tarfile.open(filename, mode) as tar:
       tar.extractall(outputdir)

