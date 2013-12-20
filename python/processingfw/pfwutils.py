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

#M #######################################################################
#M def create_copy_items(metastatus, file_header_names):
#M     """ Create the update wcl for headers that should be copied from another header """
#M     updateDict = OrderedDict()
#M     for name in file_header_names:
#M         if 
#M         if metastatus == META_REQUIRED:
#M             updateDict[name] = "$REQCOPY{%s}" % (name.upper())
#M         elif metastatus == META_OPTIONAL:
#M             updateDict[name] = "$OPTCOPY{%s}" % (name.upper())
#M         else:
#M             fwdie('Error:  Unknown metadata metastatus (%s)' % (metastatus), PF_EXIT_FAILURE)
#M 
#M     return updateDict
#M         
#M 
#M #######################################################################
#M def create_update_items(metastatus, file_header_names, file_header_info, header_value=None):
#M     """ Create the update wcl for headers that should be updated """
#M     updateDict = OrderedDict()
#M     for name in file_header_names:
#M         if name not in file_header_info:
#M             fwdie('Error: Missing entry in file_header_info for %s' % name, FW_EXIT_FAILURE)
#M 
#M         # Example: $HDRFNC{BAND}/Filter identifier/str
#M         if header_value is not None and name in header_value: 
#M             updateDict[name] = header_value[name] 
#M         elif metastatus == META_REQUIRED:
#M             updateDict[name] = "$HDRFNC{%s}" % (name.upper())
#M         elif metastatus == META_OPTIONAL:
#M             updateDict[name] = "$OPTFNC{%s}" % (name.upper())
#M         else:
#M             fwdie('Error:  Unknown metadata metastatus (%s)' % (metastatus), PF_EXIT_FAILURE)
#M 
#M         if file_header_info[name]['fits_data_type'].lower() == 'none':
#M             fwdie('Error:  Missing fits_data_type for file header %s\nCheck entry in OPS_FILE_HEADER table' % name, PF_EXIT_FAILURE)
#M 
#M         # Requires 'none' to not be a valid description
#M         if file_header_info[name]['description'].lower() == 'none':
#M             fwdie('Error:  Missing description for file header %s\nCheck entry in OPS_FILE_HEADER table' % name, PF_EXIT_FAILURE)
#M 
#M         updateDict[name] += "/%s/%s" % (file_header_info[name]['description'], 
#M                                         file_header_info[name]['fits_data_type'])
#M 
#M     return updateDict
#M         
#M          
#M 
#M #####################################################################################################
#M def create_one_sect_metadata_info(derived_from, filetype_metadata, wclsect = None, file_header_info=None):
#M     """ Create a dictionary containing instructions for a single section (req, opt) to be used by other code that retrieves metadata for a file """
#M 
#M     metainfo = OrderedDict()
#M     updatemeta = None
#M 
#M     #print "create_one_sect_metadata_info:"
#M     #wclutils.write_wcl(filetype_metadata)
#M     #wclutils.write_wcl(file_header_info)
#M 
#M     if META_HEADERS in filetype_metadata:
#M         metainfo[IW_META_HEADERS] = ','.join(filetype_metadata[META_HEADERS].keys())
#M 
#M     if META_COMPUTE in filetype_metadata:
#M         if file_header_info is not None:   # if supposed to update headers and update DB
#M             updatemeta = create_update_items(derived_from, filetype_metadata[META_COMPUTE].keys(), file_header_info)
#M             if IW_META_HEADERS not in metainfo:
#M                 metainfo[IW_META_HEADERS] = ""
#M             else:
#M                 metainfo[IW_META_HEADERS] += ','
#M 
#M             metainfo[IW_META_HEADERS] += ','.join(filetype_metadata[META_COMPUTE].keys())
#M         else:  # just compute values for DB
#M             metainfo[IW_META_COMPUTE] = ','.join(filetype_metadata[META_COMPUTE].keys())
#M 
#M     if META_COPY in filetype_metadata:
#M         if file_header_info is not None:   # if supposed to update headers and update DB
#M             updatemeta = create_update_items(derived_from, filetype_metadata[META_COMPUTE].keys(), file_header_info) #MMG
#M             if IW_META_HEADERS not in metainfo:
#M                 metainfo[IW_META_HEADERS] = ""
#M             else:
#M                 metainfo[IW_META_HEADERS] += ','
#M 
#M             metainfo[IW_META_HEADERS] += ','.join(filetype_metadata[META_COMPUTE].keys())
#M         else:  # just compute values for DB
#M             metainfo[IW_META_COMPUTE] = ','.join(filetype_metadata[META_COMPUTE].keys())
#M 
#M     if META_WCL in filetype_metadata:
#M         wclkeys = []
#M         for k in filetype_metadata[META_WCL].keys():
#M              if wclsect is not None:
#M                  wclkey = '%s.%s' % (wclsect, k)
#M              else:
#M                  wclkey = k
#M              wclkeys.append(wclkey)
#M         metainfo[IW_META_WCL] = ','.join(wclkeys)
#M 
#M     #print "create_one_sect_metadata_info:"
#M     #print "\tmetainfo = ", metainfo
#M     #print "\tupdatemeta = ", updatemeta
#M     return (metainfo, updatemeta)
#M 


#M ##################################################################################################
#M def create_file_metadata_dict(filetype, filetype_metadata, wclsect = None, file_header_info=None):
#M     """ Create a dictionary containing instructions to be used by other code that retrieves metadata for a file """
#M     reqmeta = None
#M     optmeta = None
#M     updatemeta = None
#M 
#M     if filetype in filetype_metadata:
#M         # required
#M         if META_REQUIRED in filetype_metadata[filetype]:
#M             (reqmeta, updatemeta) = create_one_sect_metadata_info(META_REQUIRED, 
#M                                                                   filetype_metadata[filetype][META_REQUIRED],
#M                                                                   wclsect, file_header_info)
#M 
#M         # optional
#M         if META_OPTIONAL in filetype_metadata[filetype]:
#M             (optmeta, tmp_updatemeta) = create_one_sect_metadata_info(META_OPTIONAL, 
#M                                                                   filetype_metadata[filetype][META_OPTIONAL],
#M                                                                   wclsect, file_header_info)
#M             if tmp_updatemeta is not None:
#M                 if updatemeta is None:
#M                     updatemeta = tmp_updatemeta
#M                 else:
#M                     updatemeta.update(tmp_updatemeta)
#M 
#M     return (reqmeta, optmeta, updatemeta)
#M 

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


#######################################################################
def index_job_info(jobinfo):    
    """ create dictionary of jobs indexed on blknum """
    # index jobinfo by blknum
    job_byblk = {}
    for j in jobinfo.keys():
        blknum = jobinfo[j]['blknum']
        #print "job = ",j,"blknum =", blknum
        if blknum not in job_byblk:
            job_byblk[blknum] = {}
        job_byblk[blknum][j] = jobinfo[j]

    return job_byblk


#######################################################################
def index_wrapper_info(wrapinfo):
    """ create dictionaries of wrappers indexed on jobnum and modname """
    wrap_byjob = {}
    wrap_bymod = {}
    for wrap in wrapinfo.values():
        if wrap['jobnum'] not in wrap_byjob:
            wrap_byjob[wrap['jobnum']] = {}
        wrap_byjob[wrap['jobnum']][wrap['wrapnum']] = wrap
        if wrap['modname'] not in wrap_bymod:
            wrap_bymod[wrap['modname']] = {}
        wrap_bymod[wrap['modname']][wrap['wrapnum']] = wrap

    return wrap_byjob, wrap_bymod
