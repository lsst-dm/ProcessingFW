#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import re
import os
import inspect
import tarfile
import time
import subprocess
from collections import OrderedDict
from collections import Mapping
from processingfw.pfwdefs import *
from coreutils.miscutils import *
import intgutils.wclutils as wclutils

""" Miscellaneous support functions for processing framework """

#######################################################################
def get_exec_sections(wcl, prefix):
    """ Returns exec sections appearing in given wcl """
    execs = {}
    for key, val in wcl.items():
        fwdebug(3, "PFWUTILS_DEBUG", "\tsearching for exec prefix in %s" % key)

        if re.search("^%s\d+$" % prefix, key):
            fwdebug(4, "PFWUTILS_DEBUG", "\tFound exec prefex %s" % key)
            execs[key] = val
    return execs

#######################################################################
def get_hdrup_sections(wcl, prefix):
    """ Returns header update sections appearing in given wcl """
    hdrups = {}
    for key, val in wcl.items():
        fwdebug(3, "PFWUTILS_DEBUG", "\tsearching for hdrup prefix in %s" % key)

        if re.search("^%s\d+$" % prefix, key):
            fwdebug(4, "PFWUTILS_DEBUG", "\tFound hdrup prefex %s" % key)
            hdrups[key] = val
    return hdrups

        

#######################################################################
def search_wcl_for_variables(wcl):
    fwdebug(9, "PFWUTILS_DEBUG", "BEG")
    usedvars = {}
    for key, val in wcl.items():
        if type(val) is dict or type(val) is OrderedDict:
            uvars = search_wcl_for_variables(val)
            if uvars is not None:
                usedvars.update(uvars)
        elif type(val) is str:
            viter = [m.group(1) for m in re.finditer('(?i)\$\{([^}]+)\}', val)]
            for vstr in viter:
                if ':' in vstr:
                    vstr = vstr.split(':')[0]
                usedvars[vstr] = True
        else:
            fwdebug(9, "PFWUTILS_DEBUG", "Note: wcl is not string.    key = %s, type(val) = %s, val = '%s'" % (key, type(val), val))
    
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
    """ Sets value of key in wcl, follows section notation """
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
    """ Create a dictionary containing instructions for a single section (req, opt) to be used by other code that retrieves metadata for a file """

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
    """ Create a dictionary containing instructions to be used by other code that retrieves metadata for a file """
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


###########################################################################
def next_tasknum(wcl, tasktype, step=1):
    """ Returns next tasknum for a specific task type """

    fwdebug(3, 'PFWUTILS_DEBUG', "INFO:  tasktype=%s, step=%s" % (tasktype, step))

    # note wcl stores numbers as strings
    if 'tasknums' not in wcl:
        wcl['tasknums'] = OrderedDict()
        fwdebug(3, 'PFWUTILS_DEBUG', "INFO:  added tasknums subdict")
    if tasktype not in wcl['tasknums']:
        wcl['tasknums'][tasktype] = '1'
        fwdebug(3, 'PFWUTILS_DEBUG', "INFO:  added subdict for tasktype")
    else:
        wcl['tasknums'][tasktype] = str(int(wcl['tasknums'][tasktype]) + step)
        fwdebug(3, 'PFWUTILS_DEBUG', "INFO:  incremented tasknum")

    return wcl['tasknums'][tasktype]


###########################################################################
# assumes exit code for version is 0
def get_version(execname, execdefs):
    """run command with version flag and parse output for version"""

    ver = None
    if ( execname.lower() in execdefs and
         'version_flag' in execdefs[execname.lower()] and 
         'version_pattern' in execdefs[execname.lower()] ):
        verflag = execdefs[execname.lower()]['version_flag']
        verpat = execdefs[execname.lower()]['version_pattern']

        cmd = "%s %s" % (execname, verflag)
        try:
            process = subprocess.Popen(cmd.split(),
                                       shell=False,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
        except:
            (type, value, traceback) = sys.exc_info()
            print "********************"
            print "Unexpected error: %s" % value
            print "cmd> %s" % cmd
            print "Probably could not find %s in path" % cmd.split()[0]
            print "Check for mispelled execname in submit wcl or"
            print "    make sure that the corresponding eups package is in the metapackage and it sets up the path correctly"
            raise

        process.wait()
        out = process.communicate()[0]
        if process.returncode != 0:
            fwdebug(0, 'PFWUTILS_DEBUG', "INFO:  problem when running code to get version")
            fwdebug(0, 'PFWUTILS_DEBUG', "\t%s %s %s" % (execname, verflag, verpat))
            fwdebug(0, 'PFWUTILS_DEBUG', "\tcmd> %s" % cmd)
            fwdebug(0, 'PFWUTILS_DEBUG', "\t%s" % out)
            ver = None
        else:
            # parse output with verpat
            try:
                m = re.search(verpat, out)
                if m:
                    ver = m.group(1)
                else:
                    fwdebug(1, 'PFWUTILS_DEBUG', "re.search didn't find version for exec %s" % execname)
                    fwdebug(3, 'PFWUTILS_DEBUG', "\tcmd output=%s" % out)
                    fwdebug(3, 'PFWUTILS_DEBUG', "\tcmd verpat=%s" % verpat)
            except Exception as err:
                #print type(err)
                ver = None
                print "Error: Exception from re.match.  Didn't find version: %s" % err
                raise
    else:
        fwdebug(1, 'PFWUTILS_DEBUG', "INFO: Could not find version info for exec %s" % execname)

    return ver


############################################################################
def run_cmd_qcf(cmd, logfilename, id, execnames, bufsize=5000, useQCF=False):
    """ Execute the command piping stdout/stderr to log and QCF """

    fwdebug(3, "PFWUTILS_DEBUG", "BEG")
    fwdebug(3, "PFWUTILS_DEBUG", "cmd = %s" % cmd)
    fwdebug(3, "PFWUTILS_DEBUG", "logfilename = %s" % logfilename)
    fwdebug(3, "PFWUTILS_DEBUG", "id = %s" % id)
    fwdebug(3, "PFWUTILS_DEBUG", "execnames = %s" % execnames)
    fwdebug(3, "PFWUTILS_DEBUG", "useQCF = %s" % useQCF)

    useQCF = convertBool(useQCF)

    starttime = time.time()
    logfh = open(logfilename, 'w', 0)

    sys.stdout.flush()
    try:
        processWrap = subprocess.Popen(cmd.split(),
                                       shell=False,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
    except:
        (type, value, traceback) = sys.exc_info()
        print "********************"
        print "Unexpected error: %s" % value
        print "cmd> %s" % cmd
        print "Probably could not find %s in path" % cmd.split()[0]
        print "Check for mispelled execname in submit wcl or"
        print "    make sure that the corresponding eups package is in the metapackage and it sets up the path correctly"
        raise

    if useQCF:
        cmdQCF = "qcf_controller.pl -wrapperInstanceId %s -execnames %s" % (id, execnames)
        try:
            processQCF = subprocess.Popen(cmdQCF.split(),
                                        shell=False,
                                        stdin=subprocess.PIPE,
                                        stderr=subprocess.STDOUT)
        except:
            (type, value, traceback) = sys.exc_info()
            print "********************"
            print "Unexpected error: %s" % value
            print "cmdQCF> %s" % cmdQCF
            print "use_qcf was true, but probably could not find QCF in path (%s)" % cmdQCF.split()[0]
            print "Either change submit wcl (use_qcf = False) or"
            print "    make sure that the QCFramework eups package is in the metapackage and it sets up the path correctly"
            raise


    try:
        buf = os.read(processWrap.stdout.fileno(), bufsize)
        while processWrap.poll() == None or len(buf) != 0:
            filtered_string = buf.replace("[1A", "")     # remove special characters present in AstrOmatic outputs
            filtered_string = filtered_string.replace(chr(27), "")
            filtered_string = filtered_string.replace("[1M", "")
            filtered_string = filtered_string.replace("[7m", "")

            logfh.write(filtered_string)   # write to log file
            if useQCF:
                processQCF.stdin.write(filtered_string) # pass to QCF
            buf = os.read(processWrap.stdout.fileno(), bufsize)

        logfh.close()
        if useQCF:
            processQCF.stdin.close()
            while processQCF.poll() == None:
                time.sleep(1)
            if processQCF.returncode != 0:
                print "\tWarning: QCF returned non-zero exit code"
    except IOError as e:
        (type, value, traceback) = sys.exc_info()
        print "\tI/O error({0}): {1}".format(e.errno, e.strerror)
        if useQCF:
            qcfpoll = processQCF.poll()
            if qcfpoll != None and qcfpoll != 0:
                if processWrap.poll() == None:
                    buf = os.read(processWrap.stdout.fileno(), bufsize)
                    while processWrap.poll() == None or len(buf) != 0:
                        logfh.write(buf)
                        buf = os.read(processWrap.stdout.fileno(), bufsize)

                    logfh.close()
            else:
                print "\tError: Unexpected error: %s" % value
                raise

    except:
        (type, value, traceback) = sys.exc_info()
        print "\tError: Unexpected error: %s" % value
        raise

    sys.stdout.flush()
    if processWrap.returncode != 0:
        fwdebug(3, "PFWUTILS_DEBUG", "\tInfo: cmd exited with non-zero exit code = %s" % processWrap.returncode)
        fwdebug(3, "PFWUTILS_DEBUG", "\tInfo: failed cmd = %s" % cmd)
    else:
        fwdebug(3, "PFWUTILS_DEBUG", "\tInfo: cmd exited with exit code = 0")

    print "DESDMTIME: run_cmd_qcf %0.3f" % (time.time()-starttime)

    fwdebug(3, "PFWUTILS_DEBUG", "END")
    return processWrap.returncode
