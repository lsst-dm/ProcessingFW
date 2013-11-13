#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import re
import subprocess
import argparse
import sys
import os
import time
import tarfile 
import copy
import traceback
import resource

from coreutils.miscutils import *
from processingfw.pfwdefs import *
from filemgmt.filemgmt_defs import *

import filemgmt.utils as fmutils
import processingfw.pfwutils as pfwutils
import processingfw.pfwdb as pfwdb
import intgutils.wclutils as wclutils


VERSION = '$Rev$'


######################################################################
def transfer_job_to_archives(pfw_dbh, wcl, putinfo, tasktype, tasklabel):
    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")
    fwdebug(3, "PFWRUNJOB_DEBUG", "len(putinfo) = %d" % len(putinfo))

    if len(putinfo) > 0: 
        if checkTrue(USE_TARGET_ARCHIVE_OUTPUT, wcl):
            transfer_job_to_single_archive(pfw_dbh, wcl, putinfo, 'target', tasktype, tasklabel)
    
        if USE_HOME_ARCHIVE_OUTPUT in wcl:
            if wcl[USE_HOME_ARCHIVE_OUTPUT].lower() == 'wrapper':
                transfer_job_to_single_archive(pfw_dbh, wcl, putinfo, 'home', tasktype, tasklabel)
            elif wcl[USE_HOME_ARCHIVE_OUTPUT].lower() == 'job':
                wcl['output_putinfo'].update(putinfo)

    fwdebug(3, "PFWRUNJOB_DEBUG", "END")
                   

######################################################################
def ingest_file_metadata(pfw_dbh, wcl, archive_info, file_metadata, tasktype, tasklabel):
    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG (%s, %s)" % (tasktype, tasklabel))

    starttime = time.time()
    if pfw_dbh is not None:
        pfw_dbh.insert_task(wcl, tasktype, tasklabel)

    filemgmt = None
    try:
        filemgmt_class = dynamically_load_class(archive_info['filemgmt'])
        valDict = fmutils.get_config_vals(archive_info, wcl, filemgmt_class.requested_config_vals())
        filemgmt = filemgmt_class(config=valDict)
    except:
        if pfw_dbh is not None:
            pfw_dbh.update_task_end(wcl, tasktype, PF_EXIT_FAILURE)
        (type, value, traceback) = sys.exc_info()
        print "\nError: creating filemgmt object\n%s" % value
        raise

    try:
        filemgmt.ingest_file_metadata(file_metadata)
        filemgmt.commit()
        if pfw_dbh is not None:
            pfw_dbh.update_task_end(wcl, tasktype, PF_EXIT_SUCCESS)
        else:
            print "DESDMTIME: %s %0.3f" % (tasklabel, time.time()-starttime)
    except:
        if pfw_dbh is None:
            print "DESDMTIME: %s %0.3f" % (tasklabel, time.time()-starttime)
        (type, value, traceback) = sys.exc_info()
        print "\nError: Problem ingesting file metadata\n%s" % value
        if pfw_dbh is not None:
            pfw_dbh.update_task_end(wcl, tasktype, PF_EXIT_FAILURE)
            pfw_dbh.insert_message(wcl, '%s_task' % tasktype, pfwdb.PFW_MSG_ERROR, str(value))
        print "file metadata to ingest:"
        wclutils.write_wcl(file_metadata)
        raise
    fwdebug(3, "PFWRUNJOB_DEBUG", "END")



def transfer_single_archive_to_job(pfw_dbh, wcl, files2get, jobfiles, dest):
    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")
    
    archive_info = wcl['%s_archive_info' % dest.lower()]

    results = None
    transinfo = get_file_archive_info(pfw_dbh, wcl, files2get, jobfiles, archive_info)

    if len(transinfo) > 0:
        fwdebug(3, "PFWRUNJOB_DEBUG", "\tCalling target2job on %s files" % len(transinfo))
        starttime = time.time()
        if pfw_dbh is not None:
            pfw_dbh.insert_job_wrapper_task(wcl, 'target2job')

        jobfilemvmt = None
        try:
            jobfilemvmt_class = dynamically_load_class(wcl['job_file_mvmt']['mvmtclass'])
            valDict = fmutils.get_config_vals(wcl['job_file_mvmt'], wcl, jobfilemvmt_class.requested_config_vals())
            jobfilemvmt = jobfilemvmt_class(wcl['home_archive_info'], wcl['target_archive_info'], 
                                            wcl['job_file_mvmt'], valDict)
        except Exception as err:
            print "ERROR\nError: creating job_file_mvmt object\n%s" % err
            if pfw_dbh is not None:
                pfw_dbh.update_job_wrapper_task_end(wcl, PF_EXIT_FAILURE)
                pfw_dbh.insert_message(wcl, tasktype, pfwdb.PFW_MSG_ERROR, str(value))
            raise

        if dest.lower() == 'target':
            results = jobfilemvmt.target2job(transinfo)
        else:
            results = jobfilemvmt.home2job(transinfo)

        if pfw_dbh is not None:
            pfw_dbh.update_job_wrapper_task_end(wcl, PF_EXIT_SUCCESS)
        else:
            print "DESDMTIME: %s2job %0.3f" % (dest.lower(), time.time()-starttime)

    fwdebug(3, "PFWRUNJOB_DEBUG", "END")
    return results
        


def transfer_archives_to_job(pfw_dbh, wcl, neededfiles):
    # transfer files from target/home archives to job scratch dir

    files2get = neededfiles.keys()
    if len(files2get) > 0 and checkTrue(USE_TARGET_ARCHIVE_INPUT, wcl, True):
        results = transfer_single_archive_to_job(pfw_dbh, wcl, files2get, neededfiles, 'target')

        if results is not None and len(results) > 0:
            problemfiles = {}
            for f, finfo in results.items():
                if 'err' in finfo:
                    problemfiles[f] = finfo

            files2get = list(set(files2get) - set(results.keys()))
            if len(problemfiles) != 0:
                print "Warning: had problems getting input files from target archive"
                print "\t", problemfiles
                files2get += problemfiles.keys()
        else:
            print "Warning: had problems getting input files from target archive."
            print "\ttransfer function returned no results"


    # home archive
    #use_home_archive_inputs - stage, job, never
    #use_home_archive_outputs - module, job, block, run, never.
    if len(files2get) > 0 and USE_HOME_ARCHIVE_INPUT in wcl and \
        wcl[USE_HOME_ARCHIVE_INPUT].lower() == 'job':
        results = transfer_single_archive_to_job(pfw_dbh, wcl, files2get, neededfiles, 'home')

        if results is not None and len(results) > 0:
            problemfiles = {}
            for f, finfo in results.items():
                 if 'err' in finfo:
                     problemfiles[f] = finfo

            files2get = list(set(files2get) - set(transinfo.keys()))
            if len(problemfiles) != 0:
                print "Warning: had problems getting input files from home archive"
                print "\t", problemfiles
                files2get += problemfiles.keys()
        else:
            print "Warning: had problems getting input files from home archive."
            print "\ttransfer function returned no results"
    
    return files2get




def get_file_archive_info(pfw_dbh, wcl, files2get, jobfiles, archive_info):
    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    # dynamically load class for archive file mgmt to find location of files in archive
    filemgmt = None
    try:
        filemgmt_class = dynamically_load_class(archive_info['filemgmt'])
        valDict = fmutils.get_config_vals(archive_info, wcl, filemgmt_class.requested_config_vals())
        filemgmt = filemgmt_class(config=valDict)
    except:
        (type, value, traceback) = sys.exc_info()
        print "ERROR\nError: creating filemgmt object\n%s" % value
        raise

    fileinfo_archive = filemgmt.get_file_archive_info(files2get, archive_info['name'], FM_PREFER_UNCOMPRESSED)

    archroot = archive_info['root']
    transinfo = {}
    for name, info in fileinfo_archive.items():
        transinfo[name] = copy.deepcopy(info)
        transinfo[name]['src'] = '%s/%s' % (archroot, info['rel_filename'])
        transinfo[name]['dst'] = jobfiles[name]

    fwdebug(3, "PFWRUNJOB_DEBUG", "END")
    return transinfo



def setup_wrapper(wcl, iwfilename, logfilename):
    """ Create output directories, get files from archive, and other setup work """

    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    wcl['infullnames'].append(iwfilename)

    # make directory for log file
    logdir = os.path.dirname(logfilename)
    coremakedirs(logdir)

    # make directory for outputwcl
    outputwclfile = wcl[IW_WRAPSECT]['outputwcl']
    outputwcldir = os.path.dirname(outputwclfile)
    coremakedirs(outputwcldir)

    pfw_dbh = None
    if wcl['use_db']:
        pfw_dbh = pfwdb.PFWDB()
        wcl['dbids'] = {}
        wcl['wrapperid'] = pfw_dbh.insert_wrapper(wcl, iwfilename)
    else:
        wcl['wrapperid'] = -1
        wcl['dbids'] = {}


    # register any list files for this wrapper
    cnt = 1
    if IW_LISTSECT in wcl:
        pfw_file_metadata = {}
        for llabel, ldict in wcl[IW_LISTSECT].items():
            cnt += 1
            pfw_file_metadata['file_%d' % (cnt)] = {'filename': parse_fullname(ldict['fullname'], CU_PARSE_FILENAME),
                                                    'filetype': 'list'}

        ingest_file_metadata(pfw_dbh, wcl, wcl['target_archive_info'], pfw_file_metadata, 
                             'job_wrapper', 'ingest-metadata_lists')

    
    # make directories for output files, get input files from targetnode
    fwdebug(3, "PFWRUNJOB_DEBUG", "section loop beg")
    execnamesarr = [wcl['wrapper']['wrappername']]
    outfiles = {}
    execs = pfwutils.get_exec_sections(wcl, IW_EXECPREFIX)
    for sect in sorted(execs):
        fwdebug(3, "PFWRUNJOB_DEBUG", "section %s" % sect)
        if 'execname' not in wcl[sect]:
            print "Error: Missing execname in input wcl.  sect =", sect
            print "wcl[sect] = ", wclutils.write_wcl(wcl[sect])
            fwdie("Error: Missing execname in input wcl", PF_EXIT_FAILURE)
                
        execname = wcl[sect]['execname']
        execnamesarr.append(execname)

        if 'execnum' not in wcl[sect]:
            result = re.match('%s(\d+)' % IW_EXECPREFIX, sect)
            if not result:
                fwdie("Error:  Cannot determine execnum for input wcl sect %s" % sect, PF_EXIT_FAILURE)
            wcl[sect]['execnum'] = result.group(1)

        if pfw_dbh is not None:
            wcl['dbids'][sect] = pfw_dbh.insert_exec(wcl, sect) 

        if IW_EXEC_DEF in wcl:
            if pfw_dbh is not None:
                pfw_dbh.insert_job_exec_task(wcl, wcl[sect]['execnum'], 'get_version')
            wcl[sect]['version'] = pfwutils.get_version(execname, wcl[IW_EXEC_DEF])
            if pfw_dbh is not None:
                pfw_dbh.update_exec_version(wcl['dbids'][sect], wcl[sect]['version']) 
                pfw_dbh.update_job_exec_task_end(wcl, wcl[sect]['execnum'], PF_EXIT_SUCCESS)


        starttime = time.time()
        if pfw_dbh is not None:
            pfw_dbh.insert_job_exec_task(wcl, wcl[sect]['execnum'], 'make_output_dirs')
        if IW_OUTPUTS in wcl[sect]:
            for outfile in fwsplit(wcl[sect][IW_OUTPUTS]):
                outfiles[outfile] = True
                fullnames = pfwutils.get_wcl_value(outfile+'.fullname', wcl)
                #print "fullnames = ", fullnames
                if '$RNMLST{' in fullnames:
                    m = re.search("\$RNMLST{\${(.+)},(.+)}", fullnames)
                    if m:
                        pattern = pfwutils.get_wcl_value(m.group(1), wcl)
                    else:
                        raise Exception("Could not parse $RNMLST")
                        
                else:
                    outfile_names = fwsplit(fullnames)
                    for outfile in outfile_names:
                        outfile_dir = os.path.dirname(outfile)
                        coremakedirs(outfile_dir)
        else:
            print "Info: 0 output files (%s) in exec section %s" % (IW_OUTPUTS, sect)

        if pfw_dbh is not None:
            pfw_dbh.update_job_exec_task_end(wcl, wcl[sect]['execnum'], PF_EXIT_SUCCESS)
        else:
            print "DESDMTIME: make_output_dirs %0.3f" % (time.time()-starttime)



    if 'wrapinputs' in wcl and wcl[PF_WRAPNUM] in wcl['wrapinputs'] and len(wcl['wrapinputs'][wcl[PF_WRAPNUM]].values()) > 0:
        # check which input files are already in job scratch directory (i.e., outputs from a previous execution)
        neededinputs = {}
        for infile in wcl['wrapinputs'][wcl[PF_WRAPNUM]].values():
            wcl['infullnames'].append(infile)
            if not os.path.exists(infile) and not infile in outfiles:
                neededinputs[parse_fullname(infile, CU_PARSE_FILENAME)] = infile

        if len(neededinputs) > 0: 
            files2get = transfer_archives_to_job(pfw_dbh, wcl, neededinputs)

            # check if still missing input files
            if len(files2get) > 0:
                print "******************************"
                print "Error: input files needed that were not retrieved from target or home archives\n(%s)" % files2get
                for f in files2get:
                    print "\t%s" % f
                raise Exception("Error:  Cannot find all input files in an archive")
        else:
            print "\tInfo: all %s input file(s) already in job directory." % \
                    len(wcl['wrapinputs'][wcl[PF_WRAPNUM]].values())
    else:
        print "Info: 0 wrapinputs"

    wcl['execnames'] = ','.join(execnamesarr)

    fwdebug(3, "PFWRUNJOB_DEBUG", "END")


#def run_wrapper(wrappercmd, logfilename, wrapperid, execnames, wcl, 
#                bufsize=5000, useDB=False, useQCF=False):
#    """ Execute the wrapper piping stdout/stderr to log and QCF """
#    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")
#    print "\nrun_wrapper:"
#    print "\twrappercmd = ", wrappercmd
#    print "\tlogfilename = ", logfilename
#    print "\tuseQCF = ", useQCF
#
#    logfh = open(logfilename, 'w', 0)
#
#    processWrap = subprocess.Popen(wrappercmd.split(),
#                                   shell=False,
#                                   stdout=subprocess.PIPE,
#                                   stderr=subprocess.STDOUT)
#    if useQCF:
#        cmdQCF = "qcf_controller.pl -wrapperInstanceId %s -execnames %s" % (wrapperid, execnames)
#        processQCF = subprocess.Popen(cmdQCF.split(),
#                                      shell=False,
#                                      stdin=subprocess.PIPE,
#                                      stderr=subprocess.STDOUT)
#
#    try:
#        buf = os.read(processWrap.stdout.fileno(), bufsize)
#        while processWrap.poll() == None or len(buf) != 0:
#            filtered_string = buf.replace("[1A", "")     # remove special characters present in AstrOmatic outputs
#            filtered_string = filtered_string.replace(chr(27), "")  
#            filtered_string = filtered_string.replace("[1M", "")
#            filtered_string = filtered_string.replace("[7m", "")
#
#            logfh.write(filtered_string)   # write to log file
#            if useQCF:
#                processQCF.stdin.write(filtered_string) # pass to QCF
#            buf = os.read(processWrap.stdout.fileno(), bufsize)
#
#        logfh.close()
#        if useQCF:
#            processQCF.stdin.close()
#            while processQCF.poll() == None:
#                time.sleep(1)
#            if processQCF.returncode != 0:
#                if pfw_dbh is not None:
#                    pfw_dbh.insert_message(wcl, 'job_wrapper_task', pfwdb.PFW_MSG_WARNING,
#                                           "QCF returned non-zero exit code")
#                print "\tQCF returned non-zero exit code"
#    except IOError as e:
#        print "\tI/O error({0}): {1}".format(e.errno, e.strerror)
#        if useQCF:
#            qcfpoll = processQCF.poll()
#            if qcfpoll != None and qcfpoll != 0:
#                if processWrap.poll() == None:
#                    buf = os.read(processWrap.stdout.fileno(), bufsize)
#                    while processWrap.poll() == None or len(buf) != 0:
#                        logfh.write(buf)
#                        buf = os.read(processWrap.stdout.fileno(), bufsize)
#
#                    logfh.close()
#            else:
#                (type, value, traceback) = sys.exc_info()
#                print "\tError: Unexpected error: %s" % value
#                if pfw_dbh is not None:
#                    pfw_dbh.update_job_wrapper_task_end(wcl, PF_EXIT_FAILURE)
#                    pfw_dbh.insert_message(wcl, 'job_wrapper_task', pfwdb.PFW_MSG_ERROR, str(value))
#                    pfw_dbh.close()
#                raise
#                
#    except:
#        (type, value, traceback) = sys.exc_info()
#        print "\tError: Unexpected error: %s" % value
#        if pfw_dbh is not None:
#            pfw_dbh.update_job_wrapper_task_end(wcl, PF_EXIT_FAILURE)
#            pfw_dbh.insert_message(wcl, 'job_wrapper_task', pfwdb.PFW_MSG_ERROR, str(value))
#            pfw_dbh.close()
#        raise
#
#    if processWrap.returncode != 0:
#        print "\tError: wrapper returned non-zero exit code (%s)" % processWrap.returncode
#    else:
#        print "\tInfo: wrapper exited with exit code = 0"
#
#    print resource.getrusage(resource.RUSAGE_CHILDREN)
#
#
#    fwdebug(3, "PFWRUNJOB_DEBUG", "END")
#    return processWrap.returncode



######################################################################
def compose_path(dirpat, wcl, infdict, fdict):
    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    maxtries = 1000    # avoid infinite loop
    count = 0
    m = re.search("(?i)\$\{([^}]+)\}", dirpat)
    while m and count < maxtries:
        count += 1
        var = m.group(1)
        parts = var.split(':')
        newvar = parts[0]
        fwdebug(6, 'PFWRUNJOB_DEBUG', "\twhy req: newvar: %s " % (newvar))

        # search for replacement value
        if newvar in wcl:
            newval = wcl[newvar]
        elif newvar in infdict:
            newval = wcl[newvar]
        else:
            raise Exception("Error: Could not find value for %s" % newvar)

        fwdebug(6, 'PFWRUNJOB_DEBUG',
              "\twhy req: newvar, newval, type(newval): %s %s %s" % (newvar, newval, type(newval)))
        newval = str(newval)
        if len(parts) > 1:
            prpat = "%%0%dd" % int(parts[1])
            try:
                newval = prpat % int(newval)
            except ValueError as err:
                print "Error: Problem padding value (%s, %s, %s): %s" % (var, newval, prpat, err)
                raise
        dirpat = re.sub("(?i)\${%s}" % var, newval, dirpat)
        m = re.search("(?i)\$\{([^}]+)\}", dirpat)

    if count >= maxtries:
        raise Exception("Error: Aborting from infinite loop\n. Current string: '%s'" % dirpat)
    fwdebug(3, "PFWRUNJOB_DEBUG", "END")
    return dirpat





######################################################################
def register_files_in_archive(pfw_dbh, wcl, archive_info, fileinfo, tasktype, tasklabel):
    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")
    if pfw_dbh is not None:
        pfw_dbh.insert_task(wcl, tasktype, tasklabel)

    # load file management class
    filemgmt = None
    try:
        filemgmt_class = dynamically_load_class(archive_info['filemgmt'])
        valDict = fmutils.get_config_vals(archive_info, wcl, filemgmt_class.requested_config_vals())
        filemgmt = filemgmt_class(config=valDict)
    except:
        (type, value, traceback) = sys.exc_info()
        print "ERROR\nError: creating filemgmt object\n%s" % value
        if pfw_dbh is not None:
            pfw_dbh.update_job_wrapper_task_end(wcl, PF_EXIT_FAILURE)
            pfw_dbh.insert_message(wcl, tasktype, pfwdb.PFW_MSG_ERROR, str(value))
        raise


    # call function to do the register
    try:
        filemgmt.register_file_in_archive(fileinfo, {'archive': archive_info['name']})
        filemgmt.commit()
    except:
        (type, value, traceback) = sys.exc_info()
        print "ERROR\nError: creating filemgmt object\n%s" % value
        if pfw_dbh is not None:
            pfw_dbh.update_job_wrapper_task_end(wcl, PF_EXIT_FAILURE)
            pfw_dbh.insert_message(wcl, tasktype, pfwdb.PFW_MSG_ERROR, str(value))
        raise
    fwdebug(3, "PFWRUNJOB_DEBUG", "END")



######################################################################
def transfer_job_to_single_archive(pfw_dbh, wcl, putinfo, dest, tasktype, tasklabel):
    # dynamically load class for filemgmt
    fwdebug(3, "PFWRUNJOB_DEBUG", "TRANSFER JOB TO ARCHIVE SECTION")
    if pfw_dbh is not None:
        pfw_dbh.insert_task(wcl, tasktype, tasklabel)

    archive_info = wcl['%s_archive_info' % dest.lower()]
       
    # make archive full paths for transfer
    arroot = archive_info['root']
    for fdict in putinfo.values():
        fdict['dst'] = "%s/%s/%s" % (arroot, fdict['path'], os.path.basename(fdict['src']))

    # dynamically load class for job_file_mvmt
    if 'job_file_mvmt' not in wcl:
        if pfw_dbh is not None:
            pfw_dbh.update_job_wrapper_task_end(wcl, PF_EXIT_FAILURE)
            pfw_dbh.insert_message(wcl, tasktype, pfwdb.PFW_MSG_ERROR, str(value))
        raise KeyError("Error:  Missing job_file_mvmt in job wcl")

    jobfilemvmt = None
    try:
        jobfilemvmt_class = dynamically_load_class(wcl['job_file_mvmt']['mvmtclass'])
        valDict = fmutils.get_config_vals(wcl['job_file_mvmt'], wcl, jobfilemvmt_class.requested_config_vals())
        jobfilemvmt = jobfilemvmt_class(wcl['home_archive_info'], wcl['target_archive_info'], 
                                        wcl['job_file_mvmt'], valDict)
    except Exception as err:
        print "ERROR\nError: creating job_file_mvmt object\n%s" % err
        if pfw_dbh is not None:
            pfw_dbh.update_job_wrapper_task_end(wcl, PF_EXIT_FAILURE)
            pfw_dbh.insert_message(wcl, tasktype, pfwdb.PFW_MSG_ERROR, str(value))
        raise

    # tranfer files to archive
    #pretty_print_dict(putinfo)
    starttime = time.time()
    if dest.lower() == 'target':
        results = jobfilemvmt.job2target(putinfo)
    else:
        results = jobfilemvmt.job2home(putinfo)
    
    if pfw_dbh is None:
        print "DESDMTIME: %s-filemvmt %0.3f" % (tasklabel, time.time()-starttime)

    # register files that we just copied into archive
    files2register = {}
    problemfiles = {}
    for f, finfo in results.items():
        if 'err' in finfo:
            problemfiles[f] = finfo 
        else:
            files2register[f] = finfo
    fwdebug(3, "PFWRUNJOB_DEBUG", "Registering %s file(s) in archive..." % len(files2register))

    starttime = time.time()
    regprobs = register_files_in_archive(pfw_dbh, wcl, archive_info, files2register, tasktype, tasklabel)
    if pfw_dbh is None:
        print "DESDMTIME: %s-register_files %0.3f" % (tasklabel, time.time()-starttime)

    if regprobs is not None and len(regprobs) > 0:
        problemfiles.update(regprobs)

    if len(problemfiles) > 0:
        print "ERROR\n\n\nError: putting %d files into archive %s" % (len(problemfiles), archive_info['name'])
        for file in problemfiles:
            print file, problemfiles[file]
        raise Exception("Error: problems putting %d files into archive %s" % 
                        (len(problemfiles), archive_info['name']))



######################################################################
def save_log_file(pfw_dbh, wcl, logfile):
    """ register log file and copy to archive """

    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    if logfile is not None and os.path.isfile(logfile):
        fwdebug(3, "PFWRUNJOB_DEBUG", "log exists (%s)" % logfile)

        # Register log file
        pfw_file_metadata = {}
        pfw_file_metadata['file_1'] = {'filename' : parse_fullname(logfile, CU_PARSE_FILENAME), 'filetype' : 'log'}
        ingest_file_metadata(pfw_dbh, wcl, wcl['target_archive_info'], pfw_file_metadata, 
                             'job_wrapper', 'ingest-metadata_logfile')

        # since able to register log file, save as not junk file
        wcl['outfullnames'].append(logfile) 

        # copy log to archive(s)
        putinfo = {}
        putinfo[os.path.basename(logfile)] = {'src': logfile, 
                                                   'filename': parse_fullname(logfile, CU_PARSE_FILENAME),
                                                   'compression': None,
                                                   'path': wcl['log_archive_path'],
                                                   'filesize': os.path.getsize(logfile)}
        transfer_job_to_archives(pfw_dbh, wcl, putinfo, 'job_wrapper', 'copy2archive_logfile')
    else:
        fwdebug(3, "PFWRUNJOB_DEBUG", "Warning: log doesn't exist (%s)" % logfile)





######################################################################
def copy_output_to_archive(pfw_dbh, wcl, fileinfo):
    """ If requested, copy output file(s) to archive """
    # fileinfo[filename] = {filename, fullname, sectname}

    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    putinfo = {}

    fwdebug(3, "PFWRUNJOB_DEBUG", "Checking for save_file_archive")
    for (filename, fdict) in fileinfo.items():
        fwdebug(3, "PFWRUNJOB_DEBUG", "filename %s, fullname=%s" % (filename, fdict['fullname']))
        infdict = wcl[IW_FILESECT][fdict['sectname']]
        (filename, compression) = parse_fullname(fdict['fullname'], CU_PARSE_FILENAME|CU_PARSE_EXTENSION) 
        if checkTrue(SAVE_FILE_ARCHIVE, infdict):
            putinfo[filename] = {'src': fdict['fullname'],
                                 'path': infdict['archivepath'],
                                 'compression': compression,
                                 'filesize': os.path.getsize(fdict['fullname']),
                                 'filename': filename }
    transfer_job_to_archives(pfw_dbh, wcl, putinfo, 'job_wrapper', 'copy2archive_output')
    fwdebug(3, "PFWRUNJOB_DEBUG", "END")

                   

######################################################################
def postwrapper(wcl, logfile, exitcode):
    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    # don't save logfile name if none was actually written
    if not os.path.isfile(logfile):
        logfile = None

    outputwclfile = wcl[IW_WRAPSECT]['outputwcl']
    if not os.path.exists(outputwclfile):
        outputwclfile = None

    pfw_dbh = None
    if wcl['use_db']:
        pfw_dbh = pfwdb.PFWDB()
        pfw_dbh.update_wrapper_end(wcl, outputwclfile, logfile, exitcode)


    # always try to save log file
    save_log_file(pfw_dbh, wcl, logfile)

    outputwcl = None
    if outputwclfile and os.path.exists(outputwclfile):
        with open(outputwclfile, 'r') as outwclfh:
            outputwcl = wclutils.read_wcl(outwclfh, filename=outputwclfile)

        
        # add to list of non-junk output files
        wcl['outfullnames'].append(outputwclfile) 

        # append output wcl file to tarball for returning to submit machine
        try:
            with tarfile.open(wcl['condor_job_init_dir'] + '/' + wcl['output_wcl_tar'], 'a') as tar:
                tar.add(outputwclfile)
        except Exception as err:
            warnmsg = "Warning:  Could not append output wcl file to tarball: %s" % err
            if pfw_dbh is not None:
                pfw_dbh.insert_message(wcl, 'wrapper', pfwdb.PFW_MSG_WARNING, warnmsg)
            print warnmsg
            print "\tContinuing job"


        # handle copying output files to archive
        if outputwcl is not None and len(outputwcl) > 0:
            execs = pfwutils.get_exec_sections(outputwcl, OW_EXECPREFIX)
            for sect in execs:
                if pfw_dbh is not None:
                    pfw_dbh.update_exec_end(outputwcl[sect], wcl['dbids'][sect], exitcode)
                else:
                    print "DESDMTIME: app_exec %s %0.3f" % (sect, float(outputwcl[sect]['walltime']))

            finfo = {}
            if exitcode == 0:
                if OW_METASECT in outputwcl and len(outputwcl[OW_METASECT]) > 0:
                    wrapoutfullnames = [] 
                    # separate metadata needed for PFW from DB metadata tables
                    for fdict in outputwcl[OW_METASECT].values():
                        finfo[fdict['filename']] = { 'sectname': fdict['sectname'],
                                                     'fullname': fdict['fullname'],
                                                     'filename': fdict['filename'] }
                        del fdict['sectname']  # deleting because not needed by later ingest_file_metadata
                        wrapoutfullnames.append(fdict['fullname']) 

                        del fdict['fullname']  # deleting because not needed by later ingest_file_metadata
                    #wclutils.write_wcl(finfo)

                    ingest_file_metadata(pfw_dbh, wcl, wcl['target_archive_info'], 
                                         outputwcl[OW_METASECT], 'job_wrapper', 
                                         'ingest-metadata_wrapper-outputs')

                    copy_output_to_archive(pfw_dbh, wcl, finfo)
                    wcl['outfullnames'].extend(wrapoutfullnames)

                if OW_PROVSECT in outputwcl and len(outputwcl[OW_PROVSECT].keys()) > 0 and pfw_dbh is not None:
                    try:
                        starttime = time.time()
                        if pfw_dbh is not None:
                            pfw_dbh.insert_job_wrapper_task(wcl, 'ingest_provenance')
                        provdbh = pfwdb.PFWDB()
                        provdbh.ingest_provenance(outputwcl[OW_PROVSECT], wcl['dbids'])
                        provdbh.commit()
                        if pfw_dbh is not None:
                            pfw_dbh.update_job_wrapper_task_end(wcl, PF_EXIT_SUCCESS)
                        else:
                            print "DESDMTIME: ingest_provenance %0.3f" % (time.time()-starttime)
                    except:
                        (type, value, traceback) = sys.exc_info()
                        print type, value
                        print "outputwcl"
                        wclutils.write_wcl(outputwcl[OW_PROVSECT])
                        if pfw_dbh is not None:
                            pfw_dbh.update_job_wrapper_task_end(wcl, PF_EXIT_FAILURE)
                        raise

    fwdebug(3, "PFWRUNJOB_DEBUG", "END")
    



def run_tasks(taskfile, jobwcl={}):
    # run each wrapper execution sequentially
    linecnt = 0
    with open(taskfile, 'r') as tasksfh:
        # for each task
        line = tasksfh.readline()
        linecnt += 1
        while line:
            lineparts = fwsplit(line.strip())
            if len(lineparts) == 5:
                (wrapnum, wrapname, wclfile, wrapdebug, logfile) = lineparts
            elif len(lineparts) == 4:
                (wrapnum, wrapname, wclfile, logfile) = lineparts
                wrapdebug = 0
            else:
                print "Error: incorrect number of items in line #%s" % linecnt
                print "\tline: %s" % line
                return(1)

            wrappercmd = "%s --input=%s --debug=%s" % (wrapname, wclfile, wrapdebug)
            print "%04d:" % (int(wrapnum))

            if not os.path.exists(wclfile):
                print "Error: input wcl file does not exist (%s)" % wclfile
                return(1)

            with open(wclfile, 'r') as wclfh:
                wcl = wclutils.read_wcl(wclfh, filename=wclfile)
            wcl.update(jobwcl)

            setup_wrapper(wcl, wclfile, logfile)

            starttime = time.time()
            if wcl['use_db']:
                pfw_dbh = pfwdb.PFWDB() 
                pfw_dbh.insert_job_wrapper_task(wcl, 'run_wrapper')
                pfw_dbh.close()

            try:
                exitcode = pfwutils.run_cmd_qcf(wrappercmd, logfile, wcl['wrapperid'], wcl['execnames'], 
                                                5000, wcl['use_qcf'])
            except:
                (type, value, trback) = sys.exc_info()
                print "******************************"
                if wcl['use_db'] and pfw_dbh is None:   
                    pfw_dbh = pfwdb.PFWDB()
                    pfw_dbh.insert_message(wcl, 'job_wrapper', pfwdb.PFW_MSG_ERROR, str(value))
                traceback.print_exception(type, value, trback, file=sys.stdout)
                exitcode = PF_EXIT_FAILURE
                

            if exitcode != 0:
                print "Error: wrapper exited with non-zero exit code %s.   Check log." % exitcode

            if wcl['use_db']:
                pfw_dbh = pfwdb.PFWDB() 
                pfw_dbh.update_job_wrapper_task_end(wcl, exitcode)
                pfw_dbh.close()
            else:
                print "DESDMTIME: run_wrapper %0.3f" % (time.time()-starttime)

            postwrapper(wcl, logfile, exitcode) 

            sys.stdout.flush()
            sys.stderr.flush()
            if exitcode:
                print "Aborting due to non-zero exit code"
                return(exitcode)
            line = tasksfh.readline()
    return(0)



def run_job(args): 
    """Run tasks inside single job"""

    wcl = {}

    jobstart = time.time()
    if args.config:
        with open(args.config, 'r') as wclfh:
            wcl = wclutils.read_wcl(wclfh, filename=args.config) 
            wcl['use_db'] = checkTrue('usedb', wcl, True)
            wcl['use_qcf'] = checkTrue('useqcf', wcl, False)
    else:
        raise Exception("Error:  Must specify job config file")

    pfw_dbh = None
    if wcl['use_db']:   
        # export serviceAccess info to environment
        if 'des_services' in wcl:
            os.environ['DES_SERVICES'] = wcl['des_services']
        if 'des_db_section' in wcl:
            os.environ['DES_DB_SECTION'] = wcl['des_db_section']

        # insert job 
        pfw_dbh = pfwdb.PFWDB()
        pfw_dbh.insert_job(wcl)
        pfw_dbh.close()    # in case job is long running, will reopen connection at end of job
        pfw_dbh = None


    # Save pointers to archive information for quick lookup
    if wcl[USE_HOME_ARCHIVE_INPUT] != 'never' or wcl[USE_HOME_ARCHIVE_OUTPUT] != 'never':
        wcl['home_archive_info'] = wcl['archive'][wcl[HOME_ARCHIVE]]
    else:
        wcl['home_archive_info'] = None

    if checkTrue(USE_TARGET_ARCHIVE_INPUT, wcl, True) or checkTrue(USE_TARGET_ARCHIVE_OUTPUT, wcl, True):
        wcl['target_archive_info'] = wcl['archive'][wcl[TARGET_ARCHIVE]]
    else:
        wcl['target_archive_info'] = None


    # used to keep track of all input files and registered output files
    wcl['outfullnames'] = []
    wcl['infullnames'] = [args.config, args.taskfile]
    if USE_HOME_ARCHIVE_OUTPUT in wcl and wcl[USE_HOME_ARCHIVE_OUTPUT].lower() == 'job':
        wcl['output_putinfo'] = {}
    

    # run the tasks (i.e., each wrapper execution)
    pfw_dbh = None
    try:
        exitcode = run_tasks(args.taskfile, wcl)
    except Exception as err:
        (type, value, trback) = sys.exc_info()
        print "******************************"
        if wcl['use_db'] and pfw_dbh is None:   
            pfw_dbh = pfwdb.PFWDB()
            pfw_dbh.insert_message(wcl, 'job', pfwdb.PFW_MSG_ERROR, str(value))
        traceback.print_exception(type, value, trback, file=sys.stdout)
        exitcode = PF_EXIT_FAILURE
        print "Aborting rest of wrapper executions.  Continuing to end-of-job tasks\n\n"

    if wcl['use_db'] and pfw_dbh is None:   
        pfw_dbh = pfwdb.PFWDB()

    # if should transfer home at end of job
    if USE_HOME_ARCHIVE_OUTPUT in wcl and wcl[USE_HOME_ARCHIVE_OUTPUT].lower() == 'job':
        transfer_job_to_single_archive(pfw_dbh, wcl, wcl['output_putinfo'], 'home', 'job', 'transfer_job2home')

    if CREATE_JUNK_TARBALL in wcl and convertBool(wcl[CREATE_JUNK_TARBALL]):
        create_junk_tarball(pfw_dbh, wcl)

    if pfw_dbh is not None:
        pfw_dbh.update_job_end(wcl, exitcode)
        pfw_dbh.close()
    else:
        print "DESDMTIME: pfwrun_job %0.3f" % (time.time()-jobstart)

    return exitcode



def create_junk_tarball(pfw_dbh, wcl):
    # input files are what files where staged by framework (i.e., input wcl)
    # output files are only those listed as outputs in outout wcl
    fwdebug(3, "PFWRUNJOB_DEBUG", "\n\nBEG")

    fwdebug(3, "PFWRUNJOB_DEBUG", "infullnames = %s" % wcl['infullnames'])
    fwdebug(3, "PFWRUNJOB_DEBUG", "outfullnames = %s" % wcl['outfullnames'])


    junklist = []

    # remove paths
    notjunk = {}
    for f in wcl['infullnames']:
        notjunk[os.path.basename(f)] = True
    
    # remove paths
    for f in wcl['outfullnames']:
        notjunk[os.path.basename(f)] = True

    fwdebug(3, "PFWRUNJOB_DEBUG", "notjunk = %s" % notjunk.keys())

    # walk job directory to get all files
    fullnames = {}

    cwd = '.'
#    if 'PWD' not in os.environ:
#        cwd = os.getcwd() 
#    else:
#        cwd = os.getenv('PWD')
    for (dirpath, dirnames, filenames) in os.walk(cwd):
        for walkname in filenames:
            fwdebug(4, "PFWRUNJOB_DEBUG", "walkname = %s" % walkname)
            if walkname not in notjunk:
                fwdebug(4, "PFWRUNJOB_DEBUG", "Appending walkname to list = %s" % walkname)
                junklist.append("%s/%s" % (dirpath, walkname))
                

    fwdebug(3, "PFWRUNJOB_DEBUG", "junklist = %s" % junklist)

    if len(junklist) > 0:
        if pfw_dbh is not None:
            pfw_dbh.insert_job_task(wcl, 'create_junktar')
        pfwutils.tar_list(wcl['junktar'], junklist)
        if pfw_dbh is not None:
            pfw_dbh.update_job_junktar(wcl, wcl['junktar'])
            pfw_dbh.update_job_task_end(wcl, PF_EXIT_SUCCESS)

        # register junktar with file manager
        if pfw_dbh is not None:
            pfw_dbh.insert_job_task(wcl, 'register_junktar')

        pfw_file_metadata = {}
        pfw_file_metadata['file_1'] = {'filename' : wcl['junktar'],
                                       'filetype' : 'junk_tar'}
        ingest_file_metadata(pfw_dbh, wcl, wcl['target_archive_info'], 
                             pfw_file_metadata, 'job', 'ingest-metadata_junktar')
    

        # gather "disk" metadata about tarball
        putinfo= {wcl['junktar']: {'src': wcl['junktar'],
                                   'filename': wcl['junktar'],
                                   'compression': None,
                                   'path': wcl['junktar_archive_path'],
                                   'filesize': os.path.getsize(wcl['junktar'])}}
        
        transfer_job_to_archives(pfw_dbh, wcl, putinfo, 'job', 'copy2archive_junktar')

    fwdebug(3, "PFWRUNJOB_DEBUG", "END\n\n")
     


def parse_args(argv):
    parser = argparse.ArgumentParser(description='pfwrun_job.py')
    parser.add_argument('--version', action='store_true', default=False)
    parser.add_argument('--config', action='store')
    parser.add_argument('taskfile', action='store')

    args = parser.parse_args()

    if args.version:
        print VERSION
        sys.exit(0)

    return args

if __name__ == '__main__':
    print "Cmdline given: %s" % ' '.join(sys.argv)
    sys.exit(run_job(parse_args(sys.argv)))
