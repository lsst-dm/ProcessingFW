#!/usr/bin/env python

import re
import os
import inspect
import tarfile
from collections import OrderedDict
from collections import Mapping
from processingfw.pfwdefs import *
from processingfw.fwutils import *
import intgutils.wclutils as wclutils

""" Miscellaneous support functions for processing framework """

#######################################################################
def get_exec_sections(wcl, prefix):
    execs = {}
    for key, val in wcl.items():
        fwdebug(3, "PFWUTILS_DEBUG", "\tsearching for exec prefix in %s" % key)

        if re.search("^%s\d+$" % prefix, key):
            fwdebug(4, "PFWUTILS_DEBUG", "\tFound exec prefex %s" % key)
            execs[key] = val
    return execs

#######################################################################
def get_hdrup_sections(wcl, prefix):
    hdrups = {}
    for key, val in wcl.items():
        fwdebug(3, "PFWUTILS_DEBUG", "\tsearching for hdrup prefix in %s" % key)

        if re.search("^%s\d+$" % prefix, key):
            fwdebug(4, "PFWUTILS_DEBUG", "\tFound hdrup prefex %s" % key)
            hdrups[key] = val
    return hdrups

        

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
def tar_list(tarfilename, filelist):
    """ Tars a directory """

    if tarfilename.endswith('.gz'):
        mode = 'w:gz'
    else:
        mode = 'w'

    with tarfile.open(tarfilename, mode) as tar:
        for f in filelist:
            tar.add(f)



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
def create_update_items(metastatus, file_header_names, file_header_info, header_value=None):
    """ Create the update wcl for headers that should be updated """
    updateDict = OrderedDict()
    for name in file_header_names:
        if name not in file_header_info:
            fwdie('Error: Missing entry in file_header_info for %s' % name, FW_EXIT_FAILURE)

        # Example: $HDRFNC{BAND}/Filter identifier/str
        if header_value is not None and name in header_value: 
            updateDict[name] = header_value[name] 
        elif metastatus == META_REQUIRED:
            updateDict[name] = "$HDRFNC{%s}" % (name.upper())
        elif metastatus == META_OPTIONAL:
            updateDict[name] = "$OPTFNC{%s}" % (name.upper())
        else:
            fwdie('Error:  Unknown metadata metastatus (%s)' % (metastatus), PF_EXIT_FAILURE)

        if file_header_info[name]['fits_data_type'].lower() == 'none':
            fwdie('Error:  Missing fits_data_type for file header %s\nCheck entry in OPS_FILE_HEADER table' % name, PF_EXIT_FAILURE)

        # Requires 'none' to not be a valid description
        if file_header_info[name]['description'].lower() == 'none':
            fwdie('Error:  Missing description for file header %s\nCheck entry in OPS_FILE_HEADER table' % name, PF_EXIT_FAILURE)

        updateDict[name] += "/%s/%s" % (file_header_info[name]['description'], 
                                        file_header_info[name]['fits_data_type'])

    return updateDict
        
         

#####################################################################################################
def create_one_sect_metadata_info(derived_from, filetype_metadata, wclsect = None, file_header_info=None):

    metainfo = OrderedDict()
    updatemeta = None

    #print "create_one_sect_metadata_info:"
    #wclutils.write_wcl(filetype_metadata)
    #wclutils.write_wcl(file_header_info)

    if META_HEADERS in filetype_metadata:
        metainfo[IW_META_HEADERS] = ','.join(filetype_metadata[META_HEADERS].keys())

    if META_COMPUTE in filetype_metadata:
        if file_header_info is not None:   # if supposed to update headers and update DB
            updatemeta = create_update_items(derived_from, filetype_metadata[META_COMPUTE].keys(), file_header_info)
            if IW_META_HEADERS not in metainfo:
                metainfo[IW_META_HEADERS] = ""
            else:
                metainfo[IW_META_HEADERS] += ','

            metainfo[IW_META_HEADERS] += ','.join(filetype_metadata[META_COMPUTE].keys())
        else:  # just compute values for DB
            metainfo[IW_META_COMPUTE] = ','.join(filetype_metadata[META_COMPUTE].keys())

    if META_WCL in filetype_metadata:
        wclkeys = []
        for k in filetype_metadata[META_WCL].keys():
             if wclsect is not None:
                 wclkey = '%s.%s' % (wclsect, k)
             else:
                 wclkey = k
             wclkeys.append(wclkey)
        metainfo[IW_META_WCL] = ','.join(wclkeys)

    #print "create_one_sect_metadata_info:"
    #print "\tmetainfo = ", metainfo
    #print "\tupdatemeta = ", updatemeta
    return (metainfo, updatemeta)



##################################################################################################
def create_file_metadata_dict(filetype, filetype_metadata, wclsect = None, file_header_info=None):
    reqmeta = None
    optmeta = None
    updatemeta = None

    if filetype in filetype_metadata:
        # required
        if META_REQUIRED in filetype_metadata[filetype]:
            (reqmeta, updatemeta) = create_one_sect_metadata_info(META_REQUIRED, 
                                                                  filetype_metadata[filetype][META_REQUIRED],                                                                   wclsect, file_header_info)

        # optional
        if META_OPTIONAL in filetype_metadata[filetype]:
            (optmeta, tmp_updatemeta) = create_one_sect_metadata_info(META_OPTIONAL, 
                                                                  filetype_metadata[filetype][META_OPTIONAL],
                                                                  wclsect, file_header_info)
            if tmp_updatemeta is not None:
                if updatemeta is None:
                    updatemeta = tmp_updatemeta
                else:
                    updatemeta.update(tmp_updatemeta)

    return (reqmeta, optmeta, updatemeta)


#######################################################################
#def get_metadata_wcl(filetype, fsectname, dbwcl):
#    fdict = OrderedDict()
#    fdict['req_metadata'] = OrderedDict()
#    #print 'filetype =', filetype
#    #print 'fsetname =', fsectname
#    if filetype in dbwcl:
#        #print "Found filetype in dbwcl"
#        if 'r' in dbwcl[filetype]:
#            if 'h' in dbwcl[filetype]['r']:
#                fdict['req_metadata']['headers'] = ','.join(dbwcl[filetype]['r']['h'].keys())
#            if 'c' in dbwcl[filetype]['r']:
#                fdict['req_metadata']['compute'] = ','.join(dbwcl[filetype]['r']['c'].keys())
#
#        if 'o' in dbwcl[filetype]:
#            fdict['opt_metadata'] = OrderedDict()
#            if 'h' in dbwcl[filetype]['o']:
#                fdict['opt_metadata']['headers'] = ','.join(dbwcl[filetype]['o']['h'].keys())
#            if 'c' in dbwcl[filetype]['o']:
#                fdict['opt_metadata']['compute'] = ','.join(dbwcl[filetype]['o']['c'].keys())
#    else:
#        print "Could not find filetype (%s) in dbwcl" % filetype
#        print dbwcl
#        exit(1)
#
#    fdict['req_metadata']['wcl'] = 'filespecs.%(name)s.fullname,filespecs.%(name)s.filename,filespecs.%(name)s.filetype' % ({'name': fsectname})
#    return fdict
