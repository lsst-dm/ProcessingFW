#!/usr/bin/env python

import re
import os
import inspect
import tarfile
from collections import OrderedDict
from collections import Mapping
from processingfw.pfwdefs import *
from processingfw.fwutils import *

""" Miscellaneous support functions for processing framework """

#######################################################################
def traverse_wcl(wcl):
    fwdebug(9, "PFWUTILS_DEBUG", "BEG")
    usedvars = {}
    for key, val in wcl.items():
        if type(val) is dict or type(val) is OrderedDict:
            uvars = traverse_wcl(val)
            if uvars is not None:
                usedvars.update(uvars)
        elif type(val) is str:
            viter = [m.group(1) for m in re.finditer('(?i)\$\{([^}]+)\}', val)]
            for vstr in viter:
                if ':' in vstr:
                    vstr = vstr.split(':')[0]
                usedvars[vstr] = True
        else:
            print "Error: wcl is not string.    key = %s, type(val) = %s, val = '%s'" % (key, type(val), val)
    
    fwdebug(9, "PFWUTILS_DEBUG", "END")
    return usedvars

#######################################################################
def get_wcl_value(key, wcl):
    """ Return value of key from wcl, follows section notation """
    fwdebug(9, "PFWUTILS_DEBUG", "BEG")
    val = wcl
    for k in key.split('.'):
        #print "get_wcl_value: k=", k
        val = val[k]
    fwdebug(9, "PFWUTILS_DEBUG", "END")
    return val

#######################################################################
def set_wcl_value(key, val, wcl):
    """ sets value of key in wcl, follows section notation """
    fwdebug(9, "PFWUTILS_DEBUG", "BEG")
    wclkeys = key.split('.')
    valkey = wclkeys.pop()
    wcldict = wcl
    for k in wclkeys:
        wcldict = wcldict[k]

    wcldict[valkey] = val
    fwdebug(9, "PFWUTILS_DEBUG", "END")

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

#######################################################################
def get_metadata_wcl(filetype, fsectname, dbwcl):
    fdict = OrderedDict()
    fdict['req_metadata'] = OrderedDict()
    #print 'filetype =', filetype
    #print 'fsetname =', fsectname
    if filetype in dbwcl:
        #print "Found filetype in dbwcl"
        if 'r' in dbwcl[filetype]:
            if 'h' in dbwcl[filetype]['r']:
                fdict['req_metadata']['headers'] = ','.join(dbwcl[filetype]['r']['h'].keys())
            if 'c' in dbwcl[filetype]['r']:
                fdict['req_metadata']['compute'] = ','.join(dbwcl[filetype]['r']['c'].keys())

        if 'o' in dbwcl[filetype]:
            fdict['opt_metadata'] = OrderedDict()
            if 'h' in dbwcl[filetype]['o']:
                fdict['opt_metadata']['headers'] = ','.join(dbwcl[filetype]['o']['h'].keys())
            if 'c' in dbwcl[filetype]['o']:
                fdict['opt_metadata']['compute'] = ','.join(dbwcl[filetype]['o']['c'].keys())
    else:
        print "Could not find filetype in dbwcl"
        print dbwcl
        exit(1)

    fdict['req_metadata']['wcl'] = 'filespecs.%(name)s.fullname,filespecs.%(name)s.filename,filespecs.%(name)s.filetype' % ({'name': fsectname})
    return fdict
