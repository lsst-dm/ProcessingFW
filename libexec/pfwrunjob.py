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
import socket

import coreutils.miscutils as coremisc
import processingfw.pfwdefs as pfwdefs
import filemgmt.filemgmt_defs as fmdefs

import filemgmt.utils as fmutils
import processingfw.pfwutils as pfwutils
import processingfw.pfwdb as pfwdb
import intgutils.wclutils as wclutils
import coreutils.dbsemaphore as dbsem


VERSION = '$Rev$'

######################################################################
def determine_exec_task_id(pfw_dbh, wcl):
    exec_ids=[]
    execs = pfwutils.get_exec_sections(wcl, pfwdefs.IW_EXECPREFIX)
    execlist = sorted(execs)
    for sect in execlist:
        if '(' not in wcl[sect]['execname']:  # if not a wrapper function
            exec_ids.append(wcl['task_id']['exec'][sect])

    if len(exec_ids) > 1:
        msg = "Warning: wrapper has more than 1 non-function exec.  Defaulting to first exec."
        print msg
        if pfw_dbh is not None:
            pfw_dbh.insert_message(wcl['task_id']['wrapper'], pfwdb.PFW_MSG_WARN, str(msg))

    if len(exec_ids) == 0: # if no non-function exec, pick first function exec
        exec_id = wcl['task_id']['exec'][execlist[0]] 
    else:
        exec_id = exec_ids[0]

    return exec_id


######################################################################
def transfer_job_to_archives(pfw_dbh, wcl, putinfo, level, parent_tid, task_label, exitcode):
    """ Call the appropriate transfers based upon which archives job is using """ 
    """ level: current calling point: wrapper or job """
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "BEG %s %s %s" % (level, parent_tid, task_label))
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "len(putinfo) = %d" % len(putinfo))
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "USE_TARGET_ARCHIVE_OUTPUT = %s" % wcl[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT].lower())
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "USE_HOME_ARCHIVE_OUTPUT = %s" % wcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower())

    level = level.lower()

    if len(putinfo) > 0: 
        if pfwdefs.USE_TARGET_ARCHIVE_OUTPUT in wcl and level == wcl[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT].lower():
            transfer_job_to_single_archive(pfw_dbh, wcl, putinfo, 'target', parent_tid, task_label, exitcode)

        if pfwdefs.USE_HOME_ARCHIVE_OUTPUT in wcl and level == wcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower():
            transfer_job_to_single_archive(pfw_dbh, wcl, putinfo, 'home', parent_tid, task_label, exitcode)


        # if not end of job and transferring at end of job, save file info for later
        if (level != 'job' and 
           (pfwdefs.USE_TARGET_ARCHIVE_OUTPUT in wcl and wcl[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT].lower() == 'job' or
            pfwdefs.USE_HOME_ARCHIVE_OUTPUT in wcl and wcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower() == 'job')):
            coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "Adding %s files to save later" % len(putinfo))
            wcl['output_putinfo'].update(putinfo)  

    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "END\n\n")
                   

######################################################################
def create_wgb_prov(filelist, task_id):
    prov = {'was_generated_by': {'exec_1': ','.join(filelist)}}
    tasks = {'exec_1': task_id}
    return prov, tasks



######################################################################
def ingest_file_prov(pfw_dbh, wcl, file_prov, prov_task_ids, task_label, parent_tid):
    """ Call ingest file provenance routine after setting up appropriate filemgmt object """ 
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "BEG (%s, %s)" % (task_label, parent_tid))

    starttime = time.time()

    archive_info = None
    if pfwdefs.USE_HOME_ARCHIVE_OUTPUT in wcl and wcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower() != 'never':
        archive_info = wcl['home_archive_info']
    elif pfwdefs.USE_TARGET_ARCHIVE_OUTPUT in wcl and wcl[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT].lower() != 'never':
        archive_info = wcl['target_archive_info']
    else:
        raise Exception('Error: Could not determine archive for output files');

        
    task_id = -1
    if pfw_dbh is not None:
        task_id = pfw_dbh.create_task(name = 'dynclass', 
                                      info_table = None,
                                      parent_task_id = parent_tid,
                                      root_task_id = wcl['task_id']['attempt'],
                                      label = 'fm_meta',
                                      do_begin = True,
                                      do_commit = True)
    filemgmt = None
    try:
        filemgmt_class = coremisc.dynamically_load_class(archive_info['filemgmt'])
        valDict = fmutils.get_config_vals(archive_info, wcl, filemgmt_class.requested_config_vals())
        filemgmt = filemgmt_class(config=valDict)
    except:
        (type, value, trback) = sys.exc_info()
        msg = "Error: creating filemgmt object %s" % value
        print "\n%s" % msg
        if pfw_dbh is not None:
            pfw_dbh.insert_message(task_id, pfwdb.PFW_MSG_ERROR, msg)
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
        raise

    if pfw_dbh is not None:
        pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)


    if pfw_dbh is not None:
        task_id = pfw_dbh.create_task(name = 'ingest_provenance', 
                                      info_table = None,
                                      parent_task_id = parent_tid,
                                      root_task_id = wcl['task_id']['attempt'],
                                      label = task_label,
                                      do_begin = True,
                                      do_commit = True)

    try:
        filemgmt.ingest_provenance(file_prov, prov_task_ids)
        filemgmt.commit()

        if pfw_dbh is not None:
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)
        else:
            print "DESDMTIME: ingest_provenance %0.3f" % (time.time()-starttime)
    except:
        (type, value, trback) = sys.exc_info()
        traceback.print_exception(type, value, trback, file=sys.stdout)
        if pfw_dbh is not None:
            pfw_dbh.insert_message(task_id, pfwdb.PFW_MSG_ERROR, value)
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
        else:
            print "DESDMTIME: ingest_provenance %0.3f" % (time.time()-starttime)
        raise

    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "END\n\n")



######################################################################
def ingest_file_metadata(pfw_dbh, wcl, file_metadata, task_label, parent_tid):
    """ Call ingest file metaddata routine after setting up appropriate filemgmt object """ 
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "BEG (%s, %s)" % (task_label, parent_tid))

    starttime = time.time()

    archive_info = None
    if pfwdefs.USE_HOME_ARCHIVE_OUTPUT in wcl and wcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower() != 'never':
        archive_info = wcl['home_archive_info']
    elif pfwdefs.USE_TARGET_ARCHIVE_OUTPUT in wcl and wcl[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT].lower() != 'never':
        archive_info = wcl['target_archive_info']
    else:
        raise Exception('Error: Could not determine archive for output files');

        
    task_id = -1
    if pfw_dbh is not None:
        task_id = pfw_dbh.create_task(name = 'dynclass', 
                                      info_table = None,
                                      parent_task_id = parent_tid,
                                      root_task_id = wcl['task_id']['attempt'],
                                      label = 'fm_meta',
                                      do_begin = True,
                                      do_commit = True)
    filemgmt = None
    try:
        filemgmt_class = coremisc.dynamically_load_class(archive_info['filemgmt'])
        valDict = fmutils.get_config_vals(archive_info, wcl, filemgmt_class.requested_config_vals())
        filemgmt = filemgmt_class(config=valDict)
    except:
        (type, value, trback) = sys.exc_info()
        msg = "Error: creating filemgmt object %s" % value
        print "\n%s" % msg
        if pfw_dbh is not None:
            pfw_dbh.insert_message(task_id, pfwdb.PFW_MSG_ERROR, msg)
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
        raise

    if pfw_dbh is not None:
        pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)



    if pfw_dbh is not None:
        task_id = pfw_dbh.create_task(name = 'ingest_metadata',
                                      info_table = None,
                                      parent_task_id = parent_tid,
                                      root_task_id = wcl['task_id']['attempt'],
                                      label = task_label,
                                      do_begin = True,
                                      do_commit = True)
    try:
        filemgmt.ingest_file_metadata(file_metadata)
        filemgmt.commit()
        if pfw_dbh is not None:
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)
        else:
            print "DESDMTIME: %s %0.3f" % (task_label, time.time()-starttime)
    except:
        if pfw_dbh is None:
            print "DESDMTIME: %s %0.3f" % (task_label, time.time()-starttime)
        (type, value, trback) = sys.exc_info()
        msg = "Error: Problem ingesting file metadata %s" % value
        print "\n%s" % msg
        if pfw_dbh is not None:
            pfw_dbh.insert_message(task_id, pfwdb.PFW_MSG_ERROR, msg)
            pfw_dbh.end_task(task_id,  pfwdefs.PF_EXIT_FAILURE, True)
        print "file metadata to ingest:"
        wclutils.write_wcl(file_metadata)
        raise
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "END\n\n")



def transfer_single_archive_to_job(pfw_dbh, wcl, files2get, jobfiles, dest, parent_tid):
    """ Handle the transfer of files from a single archive to the job directory """
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")
    
    if pfw_dbh is not None:
        trans_task_id = pfw_dbh.create_task(name = 'transfer', 
                                            info_table = None,
                                            parent_task_id = parent_tid,
                                            root_task_id = wcl['task_id']['attempt'],
                                            label = None,
                                            do_begin = True,
                                            do_commit = True)

    archive_info = wcl['%s_archive_info' % dest.lower()]

    results = None
    transinfo = get_file_archive_info(pfw_dbh, wcl, files2get, jobfiles, archive_info, trans_task_id)
    
    if len(transinfo) > 0:
        coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "\tCalling target2job on %s files" % len(transinfo))
        starttime = time.time()
        tasktype = '%s2job' % dest
        task_id = -1

        tstats = None
        if 'transfer_stats' in wcl:
            if pfw_dbh is not None:
                task_id = pfw_dbh.create_task(name = 'dynclass', 
                                              info_table = None,
                                              parent_task_id = trans_task_id,
                                              root_task_id = wcl['task_id']['attempt'],
                                              label = 'stats_' + tasktype,
                                              do_begin = True,
                                              do_commit = True)
            try:
                tstats_class = coremisc.dynamically_load_class(wcl['transfer_stats'])
                valDict = fmutils.get_config_vals(None, wcl, tstats_class.requested_config_vals())
                tstats = tstats_class(trans_task_id, wcl['task_id']['attempt'], valDict)
            except Exception as err:
                print "ERROR\nError: creating transfer_stats object\n%s" % err
                if pfw_dbh is not None:
                    pfw_dbh.insert_message(task_id, pfwdb.PFW_MSG_ERROR, str(err))
                    pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
                    pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_FAILURE, True)
                raise
            if pfw_dbh is not None:
                pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)
            
        if pfw_dbh is not None:
            task_id = pfw_dbh.create_task(name = 'dynclass',
                                          info_table = None,
                                          parent_task_id = trans_task_id,
                                          root_task_id = wcl['task_id']['attempt'],
                                          label = 'jobfmv_' + tasktype,
                                          do_begin = True,
                                          do_commit = True)
        jobfilemvmt = None
        try:
            jobfilemvmt_class = coremisc.dynamically_load_class(wcl['job_file_mvmt']['mvmtclass'])
            valDict = fmutils.get_config_vals(wcl['job_file_mvmt'], wcl, jobfilemvmt_class.requested_config_vals())
            jobfilemvmt = jobfilemvmt_class(wcl['home_archive_info'], wcl['target_archive_info'], 
                                            wcl['job_file_mvmt'], tstats, valDict)
        except Exception as err:
            print "ERROR\nError: creating job_file_mvmt object\n%s" % err
            if pfw_dbh is not None:
                pfw_dbh.insert_message(task_id, pfwdb.PFW_MSG_ERROR, str(err))
                pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
                pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_FAILURE, True)
            raise
        if pfw_dbh is not None:
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)

        sem = None
        if wcl['use_db'] and 'transfer_semname' in wcl:
            sem = dbsem.DBSemaphore(wcl['transfer_semname'], trans_task_id)
            coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "Semaphore info: %s" % str(sem))

        if dest.lower() == 'target':
            results = jobfilemvmt.target2job(transinfo)
        else:
            results = jobfilemvmt.home2job(transinfo)
        if sem is not None:
            coremisc.fwdebug(0, "PFWRUNJOB_DEBUG", "Releasing lock")
            del sem

    if pfw_dbh is not None:
        pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_SUCCESS, True)
    else:
        print "DESDMTIME: %s2job %0.3f" % (dest.lower(), time.time()-starttime)

    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "END\n\n")
    return results
        


def transfer_archives_to_job(pfw_dbh, wcl, neededfiles, parent_tid):
    """ Call the appropriate transfers based upon which archives job is using """ 
    # transfer files from target/home archives to job scratch dir

    files2get = neededfiles.keys()
    if len(files2get) > 0 and wcl[pfwdefs.USE_TARGET_ARCHIVE_INPUT].lower() != 'never':
        results = transfer_single_archive_to_job(pfw_dbh, wcl, files2get, neededfiles, 'target', parent_tid)

        if results is not None and len(results) > 0:
            problemfiles = {}
            for f, finfo in results.items():
                if 'err' in finfo:
                    problemfiles[f] = finfo
                    msg = "Warning: Error trying to get file %s from target archive: %s" % (f, finfo['err'])
                    print msg
                    if pfw_dbh:
                        pfw_dbh.insert_message(wcl['task_id']['wrapper'], pfwdb.PFW_MSG_WARN, msg)

            files2get = list(set(files2get) - set(results.keys()))
            if len(problemfiles) != 0:
                print "Warning: had problems getting input files from target archive"
                #print "\t", problemfiles
                print "\t", problemfiles.keys()
                files2get += problemfiles.keys()
        else:
            print "Warning: had problems getting input files from target archive."
            print "\ttransfer function returned no results"


    # home archive
    if len(files2get) > 0 and pfwdefs.USE_HOME_ARCHIVE_INPUT in wcl and \
        wcl[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() == 'wrapper':
        results = transfer_single_archive_to_job(pfw_dbh, wcl, files2get, neededfiles, 'home', parent_tid)

        if results is not None and len(results) > 0:
            problemfiles = {}
            for f, finfo in results.items():
                 if 'err' in finfo:
                     problemfiles[f] = finfo
                     msg = "Warning: Error trying to get file %s from home archive: %s" % (f, finfo['err'])
                     print msg
                     if pfw_dbh:
                        pfw_dbh.insert_message(wcl['task_id']['wrapper'], pfwdb.PFW_MSG_WARN, msg)

            files2get = list(set(files2get) - set(results.keys()))
            if len(problemfiles) != 0:
                print "Warning: had problems getting input files from home archive"
                print "\t", problemfiles.keys()
                #print "\t", problemfiles
                files2get += problemfiles.keys()
        else:
            print "Warning: had problems getting input files from home archive."
            print "\ttransfer function returned no results"
    
    return files2get




def get_file_archive_info(pfw_dbh, wcl, files2get, jobfiles, archive_info, parent_tid):
    """ Get information about files in the archive after creating appropriate filemgmt object """
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "archive_info = %s" % archive_info)

    
    if pfw_dbh is not None:
        task_id = pfw_dbh.create_task(name = 'dynclass', 
                                      info_table = None,
                                      parent_task_id = parent_tid,
                                      root_task_id = wcl['task_id']['attempt'],
                                      label = 'fm_query',
                                      do_begin = True,
                                      do_commit = True)

    # dynamically load class for archive file mgmt to find location of files in archive
    filemgmt = None
    try:
        filemgmt_class = coremisc.dynamically_load_class(archive_info['filemgmt'])
        valDict = fmutils.get_config_vals(archive_info, wcl, filemgmt_class.requested_config_vals())
        filemgmt = filemgmt_class(config=valDict)
    except:
        (type, value, trback) = sys.exc_info()
        print "ERROR\nError: creating filemgmt object\n%s" % value
        if pfw_dbh is not None:
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
        raise
    if pfw_dbh is not None:
        pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)
        task_id = pfw_dbh.create_task(name = 'query_fileArchInfo', 
                                      info_table = None,
                                      parent_task_id = parent_tid,
                                      root_task_id = wcl['task_id']['attempt'],
                                      label = None,
                                      do_begin = True,
                                      do_commit = True)


    fileinfo_archive = filemgmt.get_file_archive_info(files2get, archive_info['name'], fmdefs.FM_PREFER_UNCOMPRESSED)
    if pfw_dbh is not None:
        pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)

    if len(files2get) != 0 and len(fileinfo_archive) == 0:
        print "Info: 0 files found on %s" % archive_info['name']
        print "\tfilemgmt = %s" % archive_info['filemgmt']

    #archroot = archive_info['root']
    transinfo = {}
    for name, info in fileinfo_archive.items():
        transinfo[name] = copy.deepcopy(info)
        #transinfo[name]['src'] = '%s/%s' % (archroot, info['rel_filename'])
        transinfo[name]['src'] = info['rel_filename']
        transinfo[name]['dst'] = jobfiles[name]

    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "END\n\n")
    return transinfo



def setup_wrapper(pfw_dbh, wcl, iwfilename, logfilename):
    """ Create output directories, get files from archive, and other setup work """

    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    # make directory for log file
    logdir = os.path.dirname(logfilename)
    coremisc.coremakedirs(logdir)

    # make directory for outputwcl
    outputwclfile = wcl[pfwdefs.IW_WRAPSECT]['outputwcl']
    outputwcldir = os.path.dirname(outputwclfile)
    coremisc.coremakedirs(outputwcldir)

    wcl['task_id']['exec'] = {}

    # register any list files for this wrapper
    list_filenames = []
    cnt = 1
    if pfwdefs.IW_LISTSECT in wcl:
        pfw_file_metadata = {}
        for llabel, ldict in wcl[pfwdefs.IW_LISTSECT].items():
            cnt += 1
            listname = coremisc.parse_fullname(ldict['fullname'], coremisc.CU_PARSE_FILENAME)
            pfw_file_metadata['file_%d' % (cnt)] = {'filename': listname,
                                                    'filetype': 'list'}
            # add to list of input files so don't go into junk tarball
            wcl['infullnames'].append(ldict['fullname'])   
            list_filenames.append(listname)    # lists are not individually compressed, so do not need compression value

        ingest_file_metadata(pfw_dbh, wcl, pfw_file_metadata, 'lists', wcl['task_id']['jobwrapper'])
        (prov, tids) = create_wgb_prov(list_filenames, wcl['task_id']['jobwrapper'])
        ingest_file_prov(pfw_dbh, wcl, prov, tids, 'lists', wcl['task_id']['jobwrapper'])

    
    # make directories for output files, get input files from targetnode
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "section loop beg")
    execnamesarr = [wcl['wrapper']['wrappername']]
    outfiles = {}
    execs = pfwutils.get_exec_sections(wcl, pfwdefs.IW_EXECPREFIX)
    for sect in sorted(execs):
        coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "section %s" % sect)
        if 'execname' not in wcl[sect]:
            print "Error: Missing execname in input wcl.  sect =", sect
            print "wcl[sect] = ", wclutils.write_wcl(wcl[sect])
            coremisc.fwdie("Error: Missing execname in input wcl", pfwdefs.PF_EXIT_FAILURE)
                
        execname = wcl[sect]['execname']
        execnamesarr.append(execname)

        if 'execnum' not in wcl[sect]:
            result = re.match('%s(\d+)' % pfwdefs.IW_EXECPREFIX, sect)
            if not result:
                coremisc.fwdie("Error:  Cannot determine execnum for input wcl sect %s" % sect, pfwdefs.PF_EXIT_FAILURE)
            wcl[sect]['execnum'] = result.group(1)

        if pfw_dbh is not None:
            wcl['task_id']['exec'][sect] = pfw_dbh.insert_exec(wcl, sect) 

        if pfwdefs.IW_EXEC_DEF in wcl:
            task_id = -1
            if pfw_dbh is not None:
                task_id = pfw_dbh.create_task(name = 'get_version', 
                                              info_table = None,
                                              parent_task_id = wcl['task_id']['jobwrapper'],
                                              root_task_id = wcl['task_id']['attempt'],
                                              label = sect,
                                              do_begin = True,
                                              do_commit = True)
            wcl[sect]['version'] = pfwutils.get_version(execname, wcl[pfwdefs.IW_EXEC_DEF])
            if pfw_dbh is not None:
                pfw_dbh.update_exec_version(wcl['task_id']['exec'][sect], wcl[sect]['version']) 
                pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)


        starttime = time.time()
        task_id = -1
        if pfw_dbh is not None:
            task_id = pfw_dbh.create_task(name = 'make_output_dirs',
                                          info_table = None,
                                          parent_task_id = wcl['task_id']['jobwrapper'],
                                          root_task_id = wcl['task_id']['attempt'],
                                          label = sect,
                                          do_begin = True,
                                          do_commit = True)
        if pfwdefs.IW_OUTPUTS in wcl[sect]:
            for outfile in coremisc.fwsplit(wcl[sect][pfwdefs.IW_OUTPUTS]):
                outfiles[outfile] = True
                fullnames = pfwutils.get_wcl_value(outfile+'.fullname', wcl)
                #print "fullnames = ", fullnames
                if '$RNMLST{' in fullnames:
                    m = re.search("\$RNMLST{\${(.+)},(.+)}", fullnames)
                    if m:
                        pattern = pfwutils.get_wcl_value(m.group(1), wcl)
                    else:
                        if pfw_dbh is not None:
                            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
                        raise Exception("Could not parse $RNMLST")
                else:
                    outfile_names = coremisc.fwsplit(fullnames)
                    for outfile in outfile_names:
                        outfile_dir = os.path.dirname(outfile)
                        coremisc.coremakedirs(outfile_dir)
        else:
            print "Info: 0 output files (%s) in exec section %s" % (pfwdefs.IW_OUTPUTS, sect)

        if pfw_dbh is not None:
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)
        else:
            print "DESDMTIME: make_output_dirs %0.3f" % (time.time()-starttime)



    if 'wrapinputs' in wcl and wcl[pfwdefs.PF_WRAPNUM] in wcl['wrapinputs'] and len(wcl['wrapinputs'][wcl[pfwdefs.PF_WRAPNUM]].values()) > 0:
        # check which input files are already in job scratch directory (i.e., outputs from a previous execution)
        neededinputs = {}
        for infile in wcl['wrapinputs'][wcl[pfwdefs.PF_WRAPNUM]].values():
            wcl['infullnames'].append(infile)
            if not os.path.exists(infile) and not infile in outfiles:
                neededinputs[coremisc.parse_fullname(infile, coremisc.CU_PARSE_FILENAME)] = infile

        if len(neededinputs) > 0: 
            files2get = transfer_archives_to_job(pfw_dbh, wcl, neededinputs, wcl['task_id']['jobwrapper'])

            # check if still missing input files
            if len(files2get) > 0:
                print "******************************"
                for f in files2get:
                    msg="Error: input file needed that was not retrieved from target or home archives\n(%s)" % f
                    print msg
                    if pfw_dbh is not None:
                        pfw_dbh.insert_message(wcl['task_id']['jobwrapper'], pfwdb.PFW_MSG_ERROR, msg)
                raise Exception("Error:  Cannot find all input files in an archive")

            # double-check: check that files are now on filesystem
            errcnt = 0
            for infile in wcl['infullnames']:
                if not os.path.exists(infile) and not infile in outfiles and \
                   not coremisc.parse_fullname(infile, coremisc.CU_PARSE_FILENAME) in files2get:
                    msg= "Error: input file doesn't exist despite transfer success (%s)" % infile
                    print msg
                    if pfw_dbh is not None:
                        pfw_dbh.insert_message(wcl['task_id']['jobwrapper'], pfwdb.PFW_MSG_ERROR, msg)
                    errcnt += 1
            if errcnt > 0:
                raise Exception("Error:  Cannot find all input files after transfer.")
        else:
            print "\tInfo: all %s input file(s) already in job directory." % \
                    len(wcl['wrapinputs'][wcl[pfwdefs.PF_WRAPNUM]].values())
    else:
        print "Info: 0 wrapinputs"

    wcl['execnames'] = ','.join(execnamesarr)

    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "END\n\n")



######################################################################
def compose_path(dirpat, wcl, infdict, fdict):
    """ Create path by replacing variables in given directory pattern """
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    maxtries = 1000    # avoid infinite loop
    count = 0
    m = re.search("(?i)\$\{([^}]+)\}", dirpat)
    while m and count < maxtries:
        count += 1
        var = m.group(1)
        parts = var.split(':')
        newvar = parts[0]
        coremisc.fwdebug(6, 'PFWRUNJOB_DEBUG', "\twhy req: newvar: %s " % (newvar))

        # search for replacement value
        if newvar in wcl:
            newval = wcl[newvar]
        elif newvar in infdict:
            newval = wcl[newvar]
        else:
            raise Exception("Error: Could not find value for %s" % newvar)

        coremisc.fwdebug(6, 'PFWRUNJOB_DEBUG',
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
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "END\n\n")
    return dirpat





######################################################################
def register_files_in_archive(pfw_dbh, wcl, archive_info, fileinfo, task_label, parent_tid):
    """ Call the method to register files in the archive after creating the appropriate filemgmt object """
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    task_id = -1
    if pfw_dbh is not None:
        task_id = pfw_dbh.create_task(name = 'dynclass', 
                                      info_table = None,
                                      parent_task_id = parent_tid,
                                      root_task_id = wcl['task_id']['attempt'],
                                      label = 'fm_register',
                                      do_begin = True,
                                      do_commit = True)

    # load file management class
    filemgmt = None
    try:
        filemgmt_class = coremisc.dynamically_load_class(archive_info['filemgmt'])
        valDict = fmutils.get_config_vals(archive_info, wcl, filemgmt_class.requested_config_vals())
        filemgmt = filemgmt_class(config=valDict)
    except:
        (type, value, trback) = sys.exc_info()
        msg = "Error: creating filemgmt object %s" % value
        print "ERROR\n%s" % msg
        if pfw_dbh is not None:
            pfw_dbh.insert_message(task_id, pfwdb.PFW_MSG_ERROR, msg)
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
        raise
    if pfw_dbh is not None:
        pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)
        task_id = pfw_dbh.create_task(name = 'register',
                                      info_table = None,
                                      parent_task_id = parent_tid,
                                      root_task_id = wcl['task_id']['attempt'],
                                      label = task_label,
                                      do_begin = True,
                                      do_commit = True)

    # call function to do the register
    try:
        filemgmt.register_file_in_archive(fileinfo, {'archive': archive_info['name']})
        filemgmt.commit()
    except:
        (type, value, trback) = sys.exc_info()
        msg = "Error: creating filemgmt object %s" % value
        print "ERROR\n%s" % msg
        if pfw_dbh is not None:
            pfw_dbh.insert_message(task_id, pfwdb.PFW_MSG_ERROR, msg)
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
        raise
    pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "END\n\n")



######################################################################
def transfer_job_to_single_archive(pfw_dbh, wcl, putinfo, dest, parent_tid, task_label, exitcode):
    """ Handle the transfer of files from the job directory to a single archive """

    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "TRANSFER JOB TO ARCHIVE SECTION")
    tasknum = -1
    if pfw_dbh is not None:
        trans_task_id = pfw_dbh.create_task(name = 'job2archive',
                                            info_table = None,
                                            parent_task_id = parent_tid,
                                            root_task_id = wcl['task_id']['attempt'],
                                            label = task_label,
                                            do_begin = True,
                                            do_commit = True)


    archive_info = wcl['%s_archive_info' % dest.lower()]
    mastersave = wcl[pfwdefs.MASTER_SAVE_FILE].lower()
       
    # make archive rel paths for transfer
    saveinfo = {}
    for key,fdict in putinfo.items():
        if pfwutils.should_save_file(mastersave, fdict['filesave'], exitcode):
            if 'path' not in fdict:
                if pfw_dbh is not None:
                    pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_FAILURE, True)
                coremisc.fwdebug(0, "PFWRUNJOB_DEBUG", "Error: Missing path (archivepath) in file definition")
                print key,fdict
                sys.exit(1)
            fdict['dst'] = "%s/%s" % (fdict['path'], os.path.basename(fdict['src']))
            saveinfo[key] = fdict


    tstats = None
    if 'transfer_stats' in wcl:
        if pfw_dbh is not None:
            task_id = pfw_dbh.create_task(name = 'dynclass', 
                                          info_table = None,
                                          parent_task_id = trans_task_id,
                                          root_task_id = wcl['task_id']['attempt'],
                                          label = 'stats_' + task_label,
                                          do_begin = True,
                                          do_commit = True)
        try:
            tstats_class = coremisc.dynamically_load_class(wcl['transfer_stats'])
            valDict = fmutils.get_config_vals(None, wcl, tstats_class.requested_config_vals())
            tstats = tstats_class(trans_task_id, wcl['task_id']['attempt'], valDict)
        except Exception as err:
            msg = "Error: creating transfer_stats object\n%s" % err
            print "ERROR\n%s" % msg
            if pfw_dbh is not None:
                pfw_dbh.insert_message(task_id, pfwdb.PFW_MSG_ERROR, msg)
                pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
                pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_FAILURE, True)
            raise
        if pfw_dbh is not None:
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)
            

    if pfw_dbh is not None:
        task_id = pfw_dbh.create_task(name = 'dynclass', 
                                      info_table = None,
                                      parent_task_id = trans_task_id,
                                      root_task_id = wcl['task_id']['attempt'],
                                      label = 'jobfmv_' + task_label,
                                      do_begin = True,
                                      do_commit = True)

    # dynamically load class for job_file_mvmt
    if 'job_file_mvmt' not in wcl:
        msg = "Error:  Missing job_file_mvmt in job wcl"
        if pfw_dbh is not None:
            pfw_dbh.insert_message(task_id, pfwdb.PFW_MSG_ERROR, msg)
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
            pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_FAILURE, True)
        raise KeyError(msg)


    jobfilemvmt = None
    try:
        jobfilemvmt_class = coremisc.dynamically_load_class(wcl['job_file_mvmt']['mvmtclass'])
        valDict = fmutils.get_config_vals(wcl['job_file_mvmt'], wcl, jobfilemvmt_class.requested_config_vals())
        jobfilemvmt = jobfilemvmt_class(wcl['home_archive_info'], wcl['target_archive_info'], 
                                        wcl['job_file_mvmt'], tstats, valDict)
    except Exception as err:
        msg = "Error: creating job_file_mvmt object\n%s" % err
        print "ERROR\n%s" % msg
        if pfw_dbh is not None:
            pfw_dbh.insert_message(task_id, pfwdb.PFW_MSG_ERROR, msg)
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
            pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_FAILURE, True)
        raise
    if pfw_dbh is not None:
        pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)

    # tranfer files to archive
    #pretty_print_dict(putinfo)
    starttime = time.time()
    sem = None
    if wcl['use_db'] and 'transfer_semname' in wcl:
        sem = dbsem.DBSemaphore(wcl['transfer_semname'], trans_task_id)
        coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "Semaphore info: %s" % str(sem))
    if dest.lower() == 'target':
        results = jobfilemvmt.job2target(saveinfo)
    else:
        results = jobfilemvmt.job2home(saveinfo)
    if sem is not None:
        coremisc.fwdebug(0, "PFWRUNJOB_DEBUG", "Releasing lock")
        del sem
    
    if pfw_dbh is None:
        print "DESDMTIME: %s-filemvmt %0.3f" % (task_label, time.time()-starttime)

    # register files that we just copied into archive
    files2register = {}
    problemfiles = {}
    for f, finfo in results.items():
        if 'err' in finfo:
            problemfiles[f] = finfo
            msg = "Warning: Error trying to copy file %s to %s archive: %s" % (f, dest, finfo['err'])
            print msg
            if pfw_dbh:
                pfw_dbh.insert_message(trans_task_id, pfwdb.PFW_MSG_WARN, msg)
        else:
            files2register[f] = finfo

    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "Registering %s file(s) in archive..." % len(files2register))
    starttime = time.time()
    regprobs = register_files_in_archive(pfw_dbh, wcl, archive_info, files2register, task_label, trans_task_id)
    if pfw_dbh is None:
        print "DESDMTIME: %s-register_files %0.3f" % (task_label, time.time()-starttime)

    if regprobs is not None and len(regprobs) > 0:
        problemfiles.update(regprobs)

    if len(problemfiles) > 0:
        print "ERROR\n\n\nError: putting %d files into archive %s" % (len(problemfiles), archive_info['name'])
        print "\t", problemfiles.keys()
        #for file in problemfiles:
        #    print file, problemfiles[file]
        if pfw_dbh is not None: 
            pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_FAILURE, True)
        raise Exception("Error: problems putting %d files into archive %s" % 
                        (len(problemfiles), archive_info['name']))

    if pfw_dbh is not None: 
        pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_SUCCESS, True)



######################################################################
def save_log_file(pfw_dbh, wcl, logfile):
    """ Register log file and prepare for copy to archive """

    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    putinfo = {}
    if logfile is not None and os.path.isfile(logfile):
        coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "log exists (%s)" % logfile)

        # Register log file
        pfw_file_metadata = {}
        filename = coremisc.parse_fullname(logfile, coremisc.CU_PARSE_FILENAME)
        pfw_file_metadata['file_1'] = {'filename' : filename,
                                       'filetype' : 'log'}
        ingest_file_metadata(pfw_dbh, wcl, pfw_file_metadata, 'logfile', wcl['task_id']['jobwrapper'])
        (prov, tids) = create_wgb_prov([filename], wcl['task_id']['jobwrapper'])
        ingest_file_prov(pfw_dbh, wcl, prov, tids, 'logfile', wcl['task_id']['jobwrapper'])

        # since able to register log file, save as not junk file
        wcl['outfullnames'].append(logfile) 

        # prep for copy log to archive(s)
        filename = coremisc.parse_fullname(logfile, coremisc.CU_PARSE_FILENAME)
        putinfo[filename] = {'src': logfile, 
                             'filename': filename,
                             'compression': None,
                             'path': wcl['log_archive_path'],
                             'filesize': os.path.getsize(logfile),
                             'filesave': True}
    else:
        coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "Warning: log doesn't exist (%s)" % logfile)

    return putinfo




######################################################################
def copy_output_to_archive(pfw_dbh, wcl, fileinfo, loginfo, exitcode):
    """ If requested, copy output file(s) to archive """
    # fileinfo[filename] = {filename, fullname, sectname}

    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "loginfo = %s" % loginfo)
    mastersave = wcl[pfwdefs.MASTER_SAVE_FILE].lower()
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "mastersave = %s" % mastersave)

    putinfo = {}

    # unless mastersave is never, always save log file
    if mastersave != 'never' and loginfo is not None and len(loginfo) > 0:
        putinfo.update(loginfo)

    # check each output file definition to see if should save file
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "Checking for save_file_archive")
    for (filename, fdict) in fileinfo.items():
        coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "filename %s, fullname=%s" % (filename, fdict['fullname']))
        infdict = wcl[pfwdefs.IW_FILESECT][fdict['sectname']]
        (filename, compression) = coremisc.parse_fullname(fdict['fullname'], coremisc.CU_PARSE_FILENAME|coremisc.CU_PARSE_EXTENSION) 

        filesave = coremisc.checkTrue(pfwdefs.SAVE_FILE_ARCHIVE, infdict, True)
        putinfo[filename] = {'src': fdict['fullname'],
                             'compression': compression,
                             'filesize': os.path.getsize(fdict['fullname']),
                             'filename': filename,
                             'filesave': filesave}

        if 'archivepath' in infdict:
            putinfo[filename]['path'] = infdict['archivepath']

    # transfer_job_to_archives(pfw_dbh, wcl, putinfo, level, parent_tid, task_label, exitcode):
    transfer_job_to_archives(pfw_dbh, wcl, putinfo, 'wrapper', wcl['task_id']['jobwrapper'], 'wrapper_output', exitcode)
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "END\n\n")

                   

######################################################################
def postwrapper(pfw_dbh, wcl, logfile, exitcode):
    """ Execute tasks after a wrapper is done """
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    # don't save logfile name if none was actually written
    if not os.path.isfile(logfile):
        logfile = None

    outputwclfile = wcl[pfwdefs.IW_WRAPSECT]['outputwcl']
    if not os.path.exists(outputwclfile):
        outputwclfile = None

    if pfw_dbh is not None:
        pfw_dbh.update_wrapper_end(wcl, outputwclfile, logfile, exitcode)


    # always try to save log file
    logfinfo=save_log_file(pfw_dbh, wcl, logfile)
    finfo = {}

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
                pfw_dbh.insert_message(wcl['task_id']['jobwrapper'], pfwdb.PFW_MSG_WARN, warnmsg)
            print warnmsg
            print "\tContinuing job"


        # handle copying output files to archive
        if outputwcl is not None and len(outputwcl) > 0:
            execs = pfwutils.get_exec_sections(outputwcl, pfwdefs.OW_EXECPREFIX)
            for sect in execs:
                if pfw_dbh is not None:
                    pfw_dbh.update_exec_end(outputwcl[sect], wcl['task_id']['exec'][sect], exitcode)
                else:
                    print "DESDMTIME: app_exec %s %0.3f" % (sect, float(outputwcl[sect]['walltime']))

            if exitcode == 0:
                if pfwdefs.OW_METASECT in outputwcl and len(outputwcl[pfwdefs.OW_METASECT]) > 0:
                    wrapoutfullnames = [] 

                    # separate metadata needed for PFW from DB metadata tables
                    for fdict in outputwcl[pfwdefs.OW_METASECT].values():
                        finfo[fdict['filename']] = { 'sectname': fdict['sectname'],
                                                     'fullname': fdict['fullname'],
                                                     'filename': fdict['filename'] }
                        del fdict['sectname']  # deleting because not needed by later ingest_file_metadata
                        wrapoutfullnames.append(fdict['fullname']) 
                        (filename, compression) = coremisc.parse_fullname(fdict['fullname'], coremisc.CU_PARSE_FILENAME | coremisc.CU_PARSE_EXTENSION)
                        if compression is not None:
                            filename += compression

                        del fdict['fullname']  # deleting because not needed by later ingest_file_metadata
                    #wclutils.write_wcl(finfo)

                    ingest_file_metadata(pfw_dbh, wcl, outputwcl[pfwdefs.OW_METASECT], 'wrapper-outputs', 
                                         wcl['task_id']['jobwrapper']) 

                    wcl['outfullnames'].extend(wrapoutfullnames)

                if pfwdefs.OW_PROVSECT in outputwcl and len(outputwcl[pfwdefs.OW_PROVSECT].keys()) > 0:
                    ingest_file_prov(pfw_dbh, wcl, outputwcl[pfwdefs.OW_PROVSECT], wcl['task_id']['exec'], None, wcl['task_id']['jobwrapper'])

    copy_output_to_archive(pfw_dbh, wcl, finfo, logfinfo, exitcode)

    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "END\n\n")
    


def parse_wrapper_line(line, linecnt):
    """ Parse a line from the job's wrapper list """
    wrapinfo = {}
    lineparts = coremisc.fwsplit(line.strip())
    if len(lineparts) == 5:
        (wrapinfo['wrapnum'], wrapinfo['wrapname'], wrapinfo['wclfile'], wrapinfo['wrapdebug'], wrapinfo['logfile']) = lineparts
    elif len(lineparts) == 4:
        (wrapinfo['wrapnum'], wrapinfo['wrapname'], wrapinfo['wclfile'], wrapinfo['logfile']) = lineparts
        wrapinfo['wrapdebug'] = 0  # default wrapdebug 
    else:
        print "Error: incorrect number of items in line #%s" % linecnt
        print "       Check that modnamepat matches wrapperloop"
        print "\tline: %s" % line
        raise SyntaxError("Error: incorrect number of items in line #%s" % linecnt)
    return wrapinfo
    

def gather_inwcl_fullnames(workflow, wcl):
    """ save input wcl fullnames in input list so won't appear in junk tarball """
    linecnt = 0
    with open(workflow, 'r') as workflowfh:
        # for each task
        line = workflowfh.readline()
        linecnt += 1
        while line:
            task = None
            try:
                task = parse_wrapper_line(line, linecnt)
                wcl['infullnames'].append(task['wclfile'])
            except Exception as e:
                print "Error: parsing task file line %s (%s)" % (linecnt, str(e)) 
                return(1)
            line = workflowfh.readline()

    #print wcl['infullnames']



def job_workflow(workflow, jobwcl={}):
    """ Run each wrapper execution sequentially """

    linecnt = 0
    with open(workflow, 'r') as workflowfh:
        # for each wrapper execution
        line = workflowfh.readline()
        linecnt += 1
        while line:
            task = parse_wrapper_line(line, linecnt)

            wrappercmd = "%s --input=%s --debug=%s" % (task['wrapname'], task['wclfile'], task['wrapdebug'])
            print "\n\n%04d: %s" % (int(task['wrapnum']), wrappercmd)

            if not os.path.exists(task['wclfile']):
                print "Error: input wcl file does not exist (%s)" % task['wclfile']
                return(1)

            with open(task['wclfile'], 'r') as wclfh:
                wcl = wclutils.read_wcl(wclfh, filename=task['wclfile'])
            wcl.update(jobwcl)

            job_task_id = wcl['task_id']['job'][wcl[pfwdefs.PF_JOBNUM]] 

            pfw_dbh = None
            if wcl['use_db']:
                pfw_dbh = pfwdb.PFWDB() 
                wcl['task_id']['jobwrapper'] = pfw_dbh.create_task(
                                                            name ='jobwrapper', 
                                                            info_table = None,
                                                            parent_task_id = job_task_id,
                                                            root_task_id = wcl['task_id']['attempt'],
                                                            label = None,
                                                            do_begin = True,
                                                            do_commit = True)
                wcl['task_id']['wrapper'] = pfw_dbh.insert_wrapper(wcl, task['wclfile'], wcl['task_id']['jobwrapper'])
            else:
                wcl['task_id']['jobwrapper'] = -1

            setup_wrapper(pfw_dbh, wcl, task['wclfile'], task['logfile'])
            exectid = determine_exec_task_id(pfw_dbh, wcl)

            if pfw_dbh is not None:
                pfw_dbh.begin_task(wcl['task_id']['wrapper'], True)
                pfw_dbh.close()
                pfw_dbh = None

            starttime = time.time()
            try:
                os.putenv("DESDMFW_TASKID", str(exectid))
                exitcode = pfwutils.run_cmd_qcf(wrappercmd, task['logfile'], wcl['task_id']['wrapper'], 
                                                wcl['execnames'], 5000, wcl['use_qcf'])
            except:
                (type, value, trback) = sys.exc_info()
                print "******************************"
                if wcl['use_db']:
                    pfw_dbh = pfwdb.PFWDB()
                    pfw_dbh.insert_message(wcl['task_id']['wrapper'], pfwdb.PFW_MSG_ERROR, str(value))
                else:
                    print "DESDMTIME: run_cmd_qcf %0.3f" % (time.time()-starttime)
                traceback.print_exception(type, value, trback, file=sys.stdout)
                exitcode = pfwdefs.PF_EXIT_FAILURE
                

            if exitcode != 0:
                print "Error: wrapper %s exited with non-zero exit code %s.   Check log:" % \
                    (wcl[pfwdefs.PF_WRAPNUM], exitcode),
                logfilename = coremisc.parse_fullname(wcl['log'], coremisc.CU_PARSE_FILENAME)
                print " %s/%s" % (wcl['log_archive_path'], logfilename)

            if wcl['use_db']:
                if pfw_dbh is None:
                    pfw_dbh = pfwdb.PFWDB() 
            else:
                print "DESDMTIME: run_wrapper %0.3f" % (time.time()-starttime)

            postwrapper(pfw_dbh, wcl, task['logfile'], exitcode) 
            pfw_dbh.end_task(wcl['task_id']['jobwrapper'], exitcode, True)

            sys.stdout.flush()
            sys.stderr.flush()
            if exitcode:
                print "Aborting due to non-zero exit code"
                return(exitcode)
            line = workflowfh.readline()

    return(0)



def run_job(args): 
    """Run tasks inside single job"""

    wcl = {}

    jobstart = time.time()
    if args.config:
        with open(args.config, 'r') as wclfh:
            wcl = wclutils.read_wcl(wclfh, filename=args.config) 
            wcl['use_db'] = coremisc.checkTrue('usedb', wcl, True)
            wcl['use_qcf'] = coremisc.checkTrue('useqcf', wcl, False)
    else:
        raise Exception("Error:  Must specify job config file")


    condor_id = None
    if 'SUBMIT_CONDORID' in os.environ:
        condor_id = os.environ['SUBMIT_CONDORID']

    batch_id = None
    if "PBS_JOBID" in os.environ:
        batch_id = os.environ['PBS_JOBID'].split('.')[0]
    elif 'LSB_JOBID' in os.environ:
        batch_id = os.environ['LSB_JOBID']
    elif 'LOADL_STEP_ID' in os.environ:
        batch_id = os.environ['LOADL_STEP_ID'].split('.').pop()
    elif 'SUBMIT_CONDORID' in os.environ:
        batch_id = os.environ['SUBMIT_CONDORID']

    pfw_dbh = None
    if wcl['use_db']:   
        # export serviceAccess info to environment
        if 'des_services' in wcl:
            os.environ['DES_SERVICES'] = wcl['des_services']
        if 'des_db_section' in wcl:
            os.environ['DES_DB_SECTION'] = wcl['des_db_section']

        # update job batch/condor ids
        pfw_dbh = pfwdb.PFWDB()
        pfw_dbh.update_job_target_info(wcl, condor_id, batch_id, socket.gethostname())
        pfw_dbh.close()    # in case job is long running, will reopen connection elsewhere in job
        pfw_dbh = None

    # Save pointers to archive information for quick lookup
    if wcl[pfwdefs.USE_HOME_ARCHIVE_INPUT] != 'never' or wcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT] != 'never':
        wcl['home_archive_info'] = wcl['archive'][wcl[pfwdefs.HOME_ARCHIVE]]
    else:
        wcl['home_archive_info'] = None

    if wcl[pfwdefs.USE_TARGET_ARCHIVE_INPUT] != 'never' or wcl[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT] != 'never':
        wcl['target_archive_info'] = wcl['archive'][wcl[pfwdefs.TARGET_ARCHIVE]]
    else:
        wcl['target_archive_info'] = None


    # used to keep track of all input files and registered output files
    wcl['outfullnames'] = []
    wcl['infullnames'] = [args.config, args.workflow]


    wcl['output_putinfo'] = {}  # to be used if transferring at end of job
    

    job_task_id = wcl['task_id']['job'][wcl[pfwdefs.PF_JOBNUM]] 

    # run the tasks (i.e., each wrapper execution)
    pfw_dbh = None
    try:
        exitcode = gather_inwcl_fullnames(args.workflow, wcl)
        exitcode = job_workflow(args.workflow, wcl)
    except Exception as err:
        (type, value, trback) = sys.exc_info()
        print "******************************"
        if wcl['use_db'] and pfw_dbh is None:   
            pfw_dbh = pfwdb.PFWDB()
            pfw_dbh.insert_message(job_task_id, pfwdb.PFW_MSG_ERROR, str(value))
        traceback.print_exception(type, value, trback, file=sys.stdout)
        exitcode = pfwdefs.PF_EXIT_FAILURE
        print "Aborting rest of wrapper executions.  Continuing to end-of-job tasks\n\n"

    if wcl['use_db'] and pfw_dbh is None:   
        pfw_dbh = pfwdb.PFWDB()

    junkinfo = {}
    if pfwdefs.CREATE_JUNK_TARBALL in wcl and coremisc.convertBool(wcl[pfwdefs.CREATE_JUNK_TARBALL]):
        junkinfo = create_junk_tarball(pfw_dbh, wcl, exitcode)
        if len(junkinfo) > 0:
            wcl['output_putinfo'].update(junkinfo)
        
    # if should transfer at end of job
    if len(wcl['output_putinfo']) > 0:
        print "\n\nCalling file transfer for end of job"
        transfer_job_to_archives(pfw_dbh, wcl, wcl['output_putinfo'], 'job', 
                                 job_task_id, 'job_output', exitcode)

    if pfw_dbh is not None:
        pfw_dbh.close()
    else:
        print "\nDESDMTIME: pfwrun_job %0.3f" % (time.time()-jobstart)

    return exitcode



def create_junk_tarball(pfw_dbh, wcl, exitcode):
    """ Create the junk tarball """

    # input files are what files where staged by framework (i.e., input wcl)
    # output files are only those listed as outputs in outout wcl
    coremisc.fwdebug(1, "PFWRUNJOB_DEBUG", "BEG")
    coremisc.fwdebug(1, "PFWRUNJOB_DEBUG", "# infullnames = %s" % len(wcl['infullnames']))
    coremisc.fwdebug(1, "PFWRUNJOB_DEBUG", "# outfullnames = %s" % len(wcl['outfullnames']))
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "infullnames = %s" % wcl['infullnames'])
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "outfullnames = %s" % wcl['outfullnames'])

    job_task_id = wcl['task_id']['job'][wcl[pfwdefs.PF_JOBNUM]] 

    junklist = []

    # remove paths
    notjunk = {}
    for f in wcl['infullnames']:
        notjunk[os.path.basename(f)] = True
    
    # remove paths
    for f in wcl['outfullnames']:
        notjunk[os.path.basename(f)] = True

    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "notjunk = %s" % notjunk.keys())

    # walk job directory to get all files
    fullnames = {}

    cwd = '.'
#    if 'PWD' not in os.environ:
#        cwd = os.getcwd() 
#    else:
#        cwd = os.getenv('PWD')
    for (dirpath, dirnames, filenames) in os.walk(cwd):
        for walkname in filenames:
            coremisc.fwdebug(4, "PFWRUNJOB_DEBUG", "walkname = %s" % walkname)
            if walkname not in notjunk:
                coremisc.fwdebug(4, "PFWRUNJOB_DEBUG", "Appending walkname to list = %s" % walkname)
                junklist.append("%s/%s" % (dirpath, walkname))
                

    coremisc.fwdebug(1, "PFWRUNJOB_DEBUG", "# in junklist = %s" % len(junklist))
    coremisc.fwdebug(3, "PFWRUNJOB_DEBUG", "junklist = %s" % junklist)

    putinfo = {}
    if len(junklist) > 0:
        task_id = -1
        if pfw_dbh is not None:
            task_id = pfw_dbh.create_task(name = 'create_junktar', 
                                          info_table = None,
                                          parent_task_id = job_task_id,
                                          root_task_id = wcl['task_id']['attempt'],
                                          label = None,
                                          do_begin = True,
                                          do_commit = True)

        pfwutils.tar_list(wcl['junktar'], junklist)

        if pfw_dbh is not None:
            pfw_dbh.update_job_junktar(wcl, wcl['junktar'])
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)

        # register junktar with file manager
        pfw_file_metadata = {}
        pfw_file_metadata['file_1'] = {'filename' : wcl['junktar'],
                                       'filetype' : 'junk_tar'}
        ingest_file_metadata(pfw_dbh, wcl, pfw_file_metadata, 'junktar', job_task_id) 
        (prov, tids) = create_wgb_prov([wcl['junktar']], job_task_id)
        ingest_file_prov(pfw_dbh, wcl, prov, tids, 'junktar', job_task_id)
    

        # gather "disk" metadata about tarball
        putinfo= {wcl['junktar']: {'src': wcl['junktar'],
                                   'filename': wcl['junktar'],
                                   'compression': None,
                                   'path': wcl['junktar_archive_path'],
                                   'filesize': os.path.getsize(wcl['junktar']),
                                   'filesave': True}}
         
        # if save setting is wrapper, save here, otherwise save at end of job
        transfer_job_to_archives(pfw_dbh, wcl, putinfo, 'wrapper', job_task_id, 
                                 'junktar', exitcode)

    coremisc.fwdebug(1, "PFWRUNJOB_DEBUG", "END\n\n")
    return putinfo
     


def parse_args(argv):
    """ Parse the command line arguments """
    parser = argparse.ArgumentParser(description='pfwrun_job.py')
    parser.add_argument('--version', action='store_true', default=False)
    parser.add_argument('--config', action='store')
    parser.add_argument('workflow', action='store')

    args = parser.parse_args()

    if args.version:
        print VERSION
        sys.exit(0)

    return args


if __name__ == '__main__':
    os.putenv('PYTHONUNBUFFERED', 'true')
    print "Cmdline given: %s" % ' '.join(sys.argv)
    sys.exit(run_job(parse_args(sys.argv)))
