#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

# pylint: disable=print-statement

""" Executes a series of wrappers within a single job """

import re
import subprocess
import argparse
import sys
import os
import time
import tarfile
import copy
import traceback
import socket
from collections import OrderedDict

import despydmdb.dbsemaphore as dbsem
import despymisc.miscutils as miscutils
import despymisc.provdefs as provdefs
import filemgmt.filemgmt_defs as fmdefs
import filemgmt.disk_utils_local as diskutils
from intgutils.wcl import WCL
import intgutils.intgdefs as intgdefs
import intgutils.intgmisc as intgmisc
import intgutils.replace_funcs as replfuncs
import processingfw.pfwdefs as pfwdefs
import processingfw.pfwutils as pfwutils
import processingfw.pfwdb as pfwdb
import processingfw.pfwcompression as pfwcompress


__version__ = '$Rev$'


######################################################################
def get_batch_id_from_job_ad(jobad_file):
    """ Parse condor job ad to get condor job id """

    batch_id = None
    try:
        info = {}
        with open(jobad_file, 'r') as jobadfh:
            for line in jobadfh:
                lmatch = re.match(r"^\s*(\S+)\s+=\s+(.+)\s*$", line)
                info[lmatch.group(1).lower()] = lmatch.group(2)

        # GlobalJobId currently too long to store as target job id
        # Print it here so have it in stdout just in case
        print "PFW: GlobalJobId:", info['globaljobid']

        batch_id = "%s.%s" % (info['clusterid'], info['procid'])
        print "PFW: batchid:", batch_id
    except Exception as ex:
        miscutils.fwdebug_print("Problem getting condor job id from job ad: %s" % (str(ex)))
        miscutils.fwdebug_print("Continuing without condor job id")


    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("condor_job_id = %s" % batch_id)
    return batch_id


######################################################################
def determine_exec_task_id(pfw_dbh, wcl):
    """ Get task_id for exec """
    exec_ids = []
    execs = intgmisc.get_exec_sections(wcl, pfwdefs.IW_EXECPREFIX)
    execlist = sorted(execs)
    for sect in execlist:
        if '(' not in wcl[sect]['execname']:  # if not a wrapper function
            exec_ids.append(wcl['task_id']['exec'][sect])

    if len(exec_ids) > 1:
        msg = "Warning: wrapper has more than 1 non-function exec.  Defaulting to first exec."
        print msg
        if pfw_dbh is not None:
            pfw_dbh.insert_message(wcl['pfw_attempt_id'],
                                   wcl['task_id']['wrapper'],
                                   pfwdefs.PFWDB_MSG_WARN, str(msg))

    if len(exec_ids) == 0: # if no non-function exec, pick first function exec
        exec_id = wcl['task_id']['exec'][execlist[0]]
    else:
        exec_id = exec_ids[0]

    return exec_id


######################################################################
def save_trans_end_of_job(pfw_dbh, wcl, jobfiles, putinfo):
    """ If transfering at end of job, save file info for later """

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")
        miscutils.fwdebug_print("len(putinfo) = %d" % len(putinfo))

    job2target = 'never'
    if pfwdefs.USE_TARGET_ARCHIVE_OUTPUT in wcl:
       job2target = wcl[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT].lower()
    job2home = 'never'
    if pfwdefs.USE_HOME_ARCHIVE_OUTPUT in wcl:
       job2home = wcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower()

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("job2target = %s" % job2target)
        miscutils.fwdebug_print("job2home = %s" % job2home)

    if len(putinfo) > 0:
        # if not end of job and transferring at end of job, save file info for later
        if job2target == 'job' or job2home == 'job':
            if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
                miscutils.fwdebug_print("Adding %s files to save later" % len(putinfo))
            jobfiles['output_putinfo'].update(putinfo)

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")


######################################################################
def transfer_job_to_archives(pfw_dbh, wcl, jobfiles, putinfo, level,
                             parent_tid, task_label, exitcode):
    """ Call the appropriate transfers based upon which archives job is using """
    #  level: current calling point: wrapper or job

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG %s %s %s" % (level, parent_tid, task_label))
        miscutils.fwdebug_print("len(putinfo) = %d" % len(putinfo))
        miscutils.fwdebug_print("putinfo = %s" % putinfo)

    level = level.lower()
    job2target = 'never'
    if pfwdefs.USE_TARGET_ARCHIVE_OUTPUT in wcl:
       job2target = wcl[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT].lower()
    job2home = 'never'
    if pfwdefs.USE_HOME_ARCHIVE_OUTPUT in wcl:
       job2home = wcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower()

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("job2target = %s" % job2target)
        miscutils.fwdebug_print("job2home = %s" % job2home)

    if len(putinfo) > 0:
        saveinfo = None
        if (level == job2target or level == job2home):
            saveinfo = output_transfer_prep(pfw_dbh, wcl, jobfiles, putinfo, parent_tid, task_label, exitcode)
        
        if level == job2target:
            transfer_job_to_single_archive(pfw_dbh, wcl, saveinfo, 'target',
                                           parent_tid, task_label, exitcode)

        if level == job2home:
            transfer_job_to_single_archive(pfw_dbh, wcl, saveinfo, 'home',
                                           parent_tid, task_label, exitcode)

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")


######################################################################
def dynam_load_filemgmt(wcl, pfw_dbh, archive_info, parent_tid):
    """ Dynamically load filemgmt class """

    if archive_info is None:
        if ((pfwdefs.USE_HOME_ARCHIVE_OUTPUT in wcl and
             wcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower() != 'never') or
                (pfwdefs.USE_HOME_ARCHIVE_INPUT in wcl and
                 wcl[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() != 'never')):
            archive_info = wcl['home_archive_info']
        elif ((pfwdefs.USE_TARGET_ARCHIVE_OUTPUT in wcl and
               wcl[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT].lower() != 'never') or
              (pfwdefs.USE_TARGET_ARCHIVE_INPUT in wcl and
               wcl[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() != 'never')):
            archive_info = wcl['target_archive_info']
        else:
            raise Exception('Error: Could not determine archive for output files. Check USE_*_ARCHIVE_* WCL vars.')

    filemgmt = pfwutils.pfw_dynam_load_class(pfw_dbh, wcl, parent_tid, wcl['task_id']['attempt'],
                                             'filemgmt', archive_info['filemgmt'], None)
    return filemgmt


######################################################################
def dynam_load_jobfilemvmt(wcl, pfw_dbh, tstats, parent_tid):
    """ Dynamically load job file mvmt class """

    #task_id = -1
    #if pfw_dbh is not None:
    #    task_id = pfw_dbh.create_task(name='dynclass',
    #                                  info_table=None,
    #                                  parent_task_id=parent_tid,
    #                                  root_task_id=wcl['task_id']['attempt'],
    #                                  label='jobfmv',
    #                                  do_begin=True,
    #                                  do_commit=True)
    jobfilemvmt = None
    try:
        jobfilemvmt_class = miscutils.dynamically_load_class(wcl['job_file_mvmt']['mvmtclass'])
        valdict = miscutils.get_config_vals(wcl['job_file_mvmt'], wcl,
                                            jobfilemvmt_class.requested_config_vals())
        jobfilemvmt = jobfilemvmt_class(wcl['home_archive_info'], wcl['target_archive_info'],
                                        wcl['job_file_mvmt'], tstats, valdict)
    except Exception as err:
        msg = "Error: creating job_file_mvmt object\n%s" % err
        print "ERROR\n%s" % msg
        if pfw_dbh is not None:
            pfw_dbh.insert_message(wcl['pfw_attempt_id'], parent_tid, pfwdefs.PFWDB_MSG_ERROR, msg)
            #pfw_dbh.insert_message(wcl['pfw_attempt_id'], task_id, pfwdefs.PFWDB_MSG_ERROR, msg)
            #pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
        raise
    #if pfw_dbh is not None:
    #    pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)

    return jobfilemvmt


######################################################################
def pfw_save_file_info(pfw_dbh, filemgmt, ftype, fullnames,
                       pfw_attempt_id, attempt_tid, parent_tid, wgb_tid,
                       do_update, update_info):
    """ Call and time filemgmt.register_file_data routine for pfw created files """
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG (%s, %s)" % (ftype, parent_tid))
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("fullnames=%s" % (fullnames))
        miscutils.fwdebug_print("do_update=%s, update_info=%s" % (do_update, update_info))

    starttime = time.time()
    task_id = -1
    if pfw_dbh is not None:
        task_id = pfw_dbh.create_task(name='save_file_info',
                                      info_table=None,
                                      parent_task_id=parent_tid,
                                      root_task_id=attempt_tid,
                                      label=ftype,
                                      do_begin=True,
                                      do_commit=True)

    try:
        filemgmt.register_file_data(ftype, fullnames, wgb_tid, do_update, update_info)
        filemgmt.commit()

        if pfw_dbh is not None:
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)
        else:
            print "DESDMTIME: pfw_save_file_info %0.3f" % (time.time()-starttime)
    except:
        (extype, exvalue, trback) = sys.exc_info()
        traceback.print_exception(extype, exvalue, trback, file=sys.stdout)
        if pfw_dbh is not None:
            pfw_dbh.insert_message(pfw_attempt_id, task_id, pfwdefs.PFWDB_MSG_ERROR,
                                   "%s: %s" % (extype, str(exvalue)))
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
        else:
            print "DESDMTIME: pfw_save_file_info %0.3f" % (time.time()-starttime)
        raise

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")


######################################################################
def transfer_single_archive_to_job(pfw_dbh, wcl, files2get, jobfiles, dest, parent_tid):
    """ Handle the transfer of files from a single archive to the job directory """
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")

    trans_task_id = 0
    if pfw_dbh is not None:
        trans_task_id = pfw_dbh.create_task(name='trans_input_%s' % dest,
                                            info_table=None,
                                            parent_task_id=parent_tid,
                                            root_task_id=wcl['task_id']['attempt'],
                                            label=None,
                                            do_begin=True,
                                            do_commit=True)

    archive_info = wcl['%s_archive_info' % dest.lower()]

    results = None
    transinfo = get_file_archive_info(pfw_dbh, wcl, files2get, jobfiles,
                                      archive_info, trans_task_id)

    if len(transinfo) > 0:
        if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
            miscutils.fwdebug_print("\tCalling target2job on %s files" % len(transinfo))
        starttime = time.time()
        tasktype = '%s2job' % dest
        tstats = None
        if 'transfer_stats' in wcl:
            tstats = pfwutils.pfw_dynam_load_class(pfw_dbh, wcl, trans_task_id,
                                                   wcl['task_id']['attempt'],
                                                   'stats_'+tasktype, wcl['transfer_stats'],
                                                   {'parent_task_id': trans_task_id,
                                                    'root_task_id': wcl['task_id']['attempt']})

        jobfilemvmt = None
        try:
            jobfilemvmt = dynam_load_jobfilemvmt(wcl, pfw_dbh, tstats, trans_task_id)
        except:
            pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_FAILURE, True)
            raise

        sem = get_semaphore(wcl, 'input', dest, trans_task_id)

        if dest.lower() == 'target':
            results = jobfilemvmt.target2job(transinfo)
        else:
            results = jobfilemvmt.home2job(transinfo)

        if sem is not None:
            if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
                miscutils.fwdebug_print("Releasing lock")
            del sem

    if pfw_dbh is not None:
        pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_SUCCESS, True)
    else:
        print "DESDMTIME: %s2job %0.3f" % (dest.lower(), time.time()-starttime)

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    return results



######################################################################
def transfer_archives_to_job(pfw_dbh, wcl, neededfiles, parent_tid):
    """ Call the appropriate transfers based upon which archives job is using """
    # transfer files from target/home archives to job scratch dir

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")
    if miscutils.fwdebug_check(6, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("neededfiles = %s" % neededfiles)

    files2get = neededfiles.keys()
    if len(files2get) > 0 and wcl[pfwdefs.USE_TARGET_ARCHIVE_INPUT].lower() != 'never':
        results = transfer_single_archive_to_job(pfw_dbh, wcl, files2get, neededfiles,
                                                 'target', parent_tid)

        if results is not None and len(results) > 0:
            problemfiles = {}
            for fkey, finfo in results.items():
                if 'err' in finfo:
                    problemfiles[fkey] = finfo
                    msg = "Warning: Error trying to get file %s from target archive: %s" % \
                          (fkey, finfo['err'])
                    print msg
                    if pfw_dbh:
                        pfw_dbh.insert_message(wcl['pfw_attempt_id'], wcl['task_id']['wrapper'],
                                               pfwdefs.PFWDB_MSG_WARN, msg)

            files2get = list(set(files2get) - set(results.keys()))
            if len(problemfiles) != 0:
                print "Warning: had problems getting input files from target archive"
                print "\t", problemfiles.keys()
                files2get += problemfiles.keys()
        else:
            print "Warning: had problems getting input files from target archive."
            print "\ttransfer function returned no results"


    # home archive
    if len(files2get) > 0 and pfwdefs.USE_HOME_ARCHIVE_INPUT in wcl and \
        wcl[pfwdefs.USE_HOME_ARCHIVE_INPUT].lower() == 'wrapper':
        results = transfer_single_archive_to_job(pfw_dbh, wcl, files2get, neededfiles,
                                                 'home', parent_tid)

        if results is not None and len(results) > 0:
            problemfiles = {}
            for fkey, finfo in results.items():
                if 'err' in finfo:
                    problemfiles[fkey] = finfo
                    msg = "Warning: Error trying to get file %s from home archive: %s" % \
                          (fkey, finfo['err'])
                    print msg
                    if pfw_dbh:
                        pfw_dbh.insert_message(wcl['pfw_attempt_id'], wcl['task_id']['wrapper'],
                                               pfwdefs.PFWDB_MSG_WARN, msg)

            files2get = list(set(files2get) - set(results.keys()))
            if len(problemfiles) != 0:
                print "Warning: had problems getting input files from home archive"
                print "\t", problemfiles.keys()
                files2get += problemfiles.keys()
        else:
            print "Warning: had problems getting input files from home archive."
            print "\ttransfer function returned no results"

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    return files2get




######################################################################
def get_file_archive_info(pfw_dbh, wcl, files2get, jobfiles, archive_info, parent_tid):
    """ Get information about files in the archive after creating appropriate filemgmt object """
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")
        miscutils.fwdebug_print("archive_info = %s" % archive_info)


    # dynamically load class for archive file mgmt to find location of files in archive
    filemgmt = dynam_load_filemgmt(wcl, pfw_dbh, archive_info, parent_tid)

    if pfw_dbh is not None:
        task_id = pfw_dbh.create_task(name='query_fileArchInfo',
                                      info_table=None,
                                      parent_task_id=parent_tid,
                                      root_task_id=wcl['task_id']['attempt'],
                                      label=None,
                                      do_begin=True,
                                      do_commit=True)

    fileinfo_archive = filemgmt.get_file_archive_info(files2get, archive_info['name'],
                                                      fmdefs.FM_PREFER_UNCOMPRESSED)
    if pfw_dbh is not None:
        pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)

    if len(files2get) != 0 and len(fileinfo_archive) == 0:
        print "\tInfo: 0 files found on %s" % archive_info['name']
        print "\t\tfilemgmt = %s" % archive_info['filemgmt']

    transinfo = {}
    for name, info in fileinfo_archive.items():
        transinfo[name] = copy.deepcopy(info)
        transinfo[name]['src'] = info['rel_filename']
        transinfo[name]['dst'] = jobfiles[name]

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    return transinfo


######################################################################
def get_wrapper_inputs(pfw_dbh, wcl, jobfiles, outfiles):
    """ Transfer any inputs needed for this wrapper """

    if 'wrapinputs' in wcl and \
       wcl[pfwdefs.PF_WRAPNUM] in wcl['wrapinputs'] and \
       len(wcl['wrapinputs'][wcl[pfwdefs.PF_WRAPNUM]].values()) > 0:

        # check which input files are already in job scratch directory
        #    (i.e., outputs from a previous execution)
        neededinputs = {}
        for infile in wcl['wrapinputs'][wcl[pfwdefs.PF_WRAPNUM]].values():
            jobfiles['infullnames'].append(infile)
            if not os.path.exists(infile) and not infile in outfiles:
                neededinputs[miscutils.parse_fullname(infile, miscutils.CU_PARSE_FILENAME)] = infile

        if len(neededinputs) > 0:
            files2get = transfer_archives_to_job(pfw_dbh, wcl, neededinputs,
                                                 wcl['task_id']['jobwrapper'])

            # check if still missing input files
            if len(files2get) > 0:
                print '!' * 60
                for fname in files2get:
                    msg = "Error: input file needed that was not retrieved from target or home archives\n(%s)" % fname
                    print msg
                    if pfw_dbh is not None:
                        pfw_dbh.insert_message(wcl['pfw_attempt_id'], wcl['task_id']['jobwrapper'],
                                               pfwdefs.PFWDB_MSG_ERROR, msg)
                raise Exception("Error:  Cannot find all input files in an archive")

            # double-check: check that files are now on filesystem
            errcnt = 0
            for infile in wcl['wrapinputs'][wcl[pfwdefs.PF_WRAPNUM]].values():
                if not os.path.exists(infile) and not infile in outfiles and \
                   not miscutils.parse_fullname(infile, miscutils.CU_PARSE_FILENAME) in files2get:
                    msg = "Error: input file doesn't exist despite transfer success (%s)" % infile
                    print msg
                    if pfw_dbh is not None:
                        pfw_dbh.insert_message(wcl['pfw_attempt_id'], wcl['task_id']['jobwrapper'],
                                               pfwdefs.PFWDB_MSG_ERROR, msg)
                    errcnt += 1
            if errcnt > 0:
                raise Exception("Error:  Cannot find all input files after transfer.")
        else:
            print "\tInfo: all %s input file(s) already in job directory." % \
                    len(wcl['wrapinputs'][wcl[pfwdefs.PF_WRAPNUM]].values())
    else:
        print "\tInfo: 0 wrapinputs"

######################################################################
def get_exec_names(wcl):
    """ Return string containing comma separated list of executable names """

    execnamesarr = []
    exec_sectnames = intgmisc.get_exec_sections(wcl, pfwdefs.IW_EXECPREFIX)
    for sect in sorted(exec_sectnames):
        if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
            miscutils.fwdebug_print("section %s" % sect)
        if 'execname' not in wcl[sect]:
            print "Error: Missing execname in input wcl.  sect =", sect
            print "wcl[sect] = ", miscutils.pretty_print_dict(wcl[sect])
            miscutils.fwdie("Error: Missing execname in input wcl", pfwdefs.PF_EXIT_FAILURE)

        execnamesarr.append(wcl[sect]['execname'])

    return ','.join(execnamesarr)


######################################################################
def create_exec_tasks(pfw_dbh, wcl):
    """ Create exec tasks saving task_ids in wcl """

    wcl['task_id']['exec'] = OrderedDict()

    exec_sectnames = intgmisc.get_exec_sections(wcl, pfwdefs.IW_EXECPREFIX)
    for sect in sorted(exec_sectnames):
        # make sure execnum in the exec section in wcl for the insert_exec function
        if 'execnum' not in wcl[sect]:
            result = re.match(r'%s(\d+)' % pfwdefs.IW_EXECPREFIX, sect)
            if not result:
                miscutils.fwdie("Error:  Cannot determine execnum for input wcl sect %s" % \
                                sect, pfwdefs.PF_EXIT_FAILURE)
            wcl[sect]['execnum'] = result.group(1)

        if pfw_dbh is not None:
            wcl['task_id']['exec'][sect] = pfw_dbh.insert_exec(wcl, sect)

######################################################################
def get_wrapper_outputs(wcl, jobfiles):
    """ get output filenames for this wrapper """
    # placeholder - needed for multiple exec sections
    return {}

######################################################################
def setup_wrapper(pfw_dbh, wcl, jobfiles, logfilename):
    """ Create output directories, get files from archive, and other setup work """

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")

    wcl['pre_disk_usage'] = pfwutils.diskusage(wcl['jobroot'])


    # make directory for log file
    logdir = os.path.dirname(logfilename)
    miscutils.coremakedirs(logdir)

    # get execnames to put on command line for QC Framework
    wcl['execnames'] = wcl['wrapper']['wrappername'] + ',' + get_exec_names(wcl)

    # get output filenames
    outfiles = get_wrapper_outputs(pfw_dbh, wcl)

    # get input files from targetnode
    get_wrapper_inputs(pfw_dbh, wcl, jobfiles, outfiles)

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")



######################################################################
def compose_path(dirpat, wcl, infdict, fdict):
    """ Create path by replacing variables in given directory pattern """
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")

    dirpat2 = replfuncs.replace_vars(dirpat, wcl, {'searchobj': infdict,
                                                   'required': True,
                                                   intgdefs.REPLACE_VARS: True})
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
    return dirpat2





######################################################################
def register_files_in_archive(pfw_dbh, wcl, archive_info, fileinfo, task_label, parent_tid):
    """ Call the method to register files in the archive after
            creating the appropriate filemgmt object """

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")

    # load file management class
    filemgmt = dynam_load_filemgmt(wcl, pfw_dbh, archive_info, parent_tid)

    task_id = -1
    if pfw_dbh is not None:
        task_id = pfw_dbh.create_task(name='register',
                                      info_table=None,
                                      parent_task_id=parent_tid,
                                      root_task_id=wcl['task_id']['attempt'],
                                      label=task_label,
                                      do_begin=True,
                                      do_commit=True)

    # call function to do the register
    try:
        filemgmt.register_file_in_archive(fileinfo, archive_info['name'])
        filemgmt.commit()
    except Exception as exc:
        (extype, exvalue, _) = sys.exc_info()
        msg = "Error registering files in archive %s - %s" % (exc.__class__.__name__, exvalue)
        print "ERROR\n%s" % msg
        if pfw_dbh is not None:
            pfw_dbh.insert_message(wcl['pfw_attempt_id'], task_id, pfwdefs.PFWDB_MSG_ERROR, msg)
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
        raise

    if pfw_dbh is not None:
        pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")


######################################################################
def output_transfer_prep(pfw_dbh, wcl, jobfiles, putinfo, parent_tid, task_label, exitcode):
    """ Compress files if necessary and make archive rel paths """

    mastersave = wcl.get(pfwdefs.MASTER_SAVE_FILE).lower()
    mastercompress = wcl.get(pfwdefs.MASTER_COMPRESSION)
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("mastersave = %s" % mastersave)
        miscutils.fwdebug_print("mastercompress = %s" % mastercompress)

    # make archive rel paths for transfer
    saveinfo = {}
    for key, fdict in putinfo.items():
        if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
            miscutils.fwdebug_print("putinfo[%s] = %s" % (key, fdict))
        should_save = pfwutils.should_save_file(mastersave, fdict['filesave'], exitcode)
        if should_save:
            if 'path' not in fdict:
                if pfw_dbh is not None:
                    pfw_dbh.end_task(parent_tid, pfwdefs.PF_EXIT_FAILURE, True)
                miscutils.fwdebug_print("Error: Missing path (archivepath) in file definition")
                print key, fdict
                sys.exit(1)
            should_compress = pfwutils.should_compress_file(mastercompress, fdict['filecompress'], exitcode)
            fdict['filecompress'] = should_compress
            fdict['dst'] = "%s/%s" % (fdict['path'], os.path.basename(fdict['src']))
            saveinfo[key] = fdict

    call_compress_files(pfw_dbh, wcl, jobfiles, saveinfo, exitcode)
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("After compress saveinfo = %s" % (saveinfo))

    return saveinfo


######################################################################
def transfer_job_to_single_archive(pfw_dbh, wcl, saveinfo, dest,
                                   parent_tid, task_label, exitcode):
    """ Handle the transfer of files from the job directory to a single archive """

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("TRANSFER JOB TO ARCHIVE SECTION")
    trans_task_id = -1
    task_id = -1
    if pfw_dbh is not None:
        trans_task_id = pfw_dbh.create_task(name='trans_output_%s' % dest,
                                            info_table=None,
                                            parent_task_id=parent_tid,
                                            root_task_id=wcl['task_id']['attempt'],
                                            label=task_label,
                                            do_begin=True,
                                            do_commit=True)

    archive_info = wcl['%s_archive_info' % dest.lower()]
    tstats = None
    if 'transfer_stats' in wcl:
        tstats = pfwutils.pfw_dynam_load_class(pfw_dbh, wcl, trans_task_id,
                                               wcl['task_id']['attempt'],
                                               'stats_'+task_label, wcl['transfer_stats'],
                                               {'parent_task_id': trans_task_id,
                                                'root_task_id': wcl['task_id']['attempt']})


    # dynamically load class for job_file_mvmt
    if 'job_file_mvmt' not in wcl:
        msg = "Error:  Missing job_file_mvmt in job wcl"
        if pfw_dbh is not None:
            pfw_dbh.insert_message(wcl['pfw_attempt_id'], task_id, pfwdefs.PFWDB_MSG_ERROR, msg)
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_FAILURE, True)
            pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_FAILURE, True)
        raise KeyError(msg)


    jobfilemvmt = None
    try:
        jobfilemvmt = dynam_load_jobfilemvmt(wcl, pfw_dbh, tstats, trans_task_id)
    except:
        pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_FAILURE, True)
        raise

    # tranfer files to archive
    starttime = time.time()
    sem = get_semaphore(wcl, 'output', dest, trans_task_id)
    if dest.lower() == 'target':
        results = jobfilemvmt.job2target(saveinfo)
    else:
        results = jobfilemvmt.job2home(saveinfo)

    if sem is not None:
        if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
            miscutils.fwdebug_print("Releasing lock")
        del sem

    if pfw_dbh is None:
        print "DESDMTIME: %s-filemvmt %0.3f" % (task_label, time.time()-starttime)

    # register files that we just copied into archive
    files2register = []
    problemfiles = {}
    for fkey, finfo in results.items():
        if 'err' in finfo:
            problemfiles[fkey] = finfo
            msg = "Warning: Error trying to copy file %s to %s archive: %s" % \
                   (fkey, dest, finfo['err'])
            print msg
            if pfw_dbh:
                pfw_dbh.insert_message(wcl['pfw_attempt_id'], trans_task_id, pfwdefs.PFWDB_MSG_WARN, msg)
        else:
            files2register.append(finfo)

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("Registering %s file(s) in archive..." % len(files2register))
    starttime = time.time()
    register_files_in_archive(pfw_dbh, wcl, archive_info, files2register, task_label, trans_task_id)
    if pfw_dbh is None:
        print "DESDMTIME: %s-register_files %0.3f" % (task_label, time.time()-starttime)

    if len(problemfiles) > 0:
        print "ERROR\n\n\nError: putting %d files into archive %s" % \
              (len(problemfiles), archive_info['name'])
        print "\t", problemfiles.keys()
        if pfw_dbh is not None:
            pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_FAILURE, True)
        raise Exception("Error: problems putting %d files into archive %s" %
                        (len(problemfiles), archive_info['name']))

    if pfw_dbh is not None:
        pfw_dbh.end_task(trans_task_id, pfwdefs.PF_EXIT_SUCCESS, True)



######################################################################
def save_log_file(pfw_dbh, filemgmt, wcl, jobfiles, logfile):
    """ Register log file and prepare for copy to archive """

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")

    putinfo = {}
    if logfile is not None and os.path.isfile(logfile):
        if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
            miscutils.fwdebug_print("log exists (%s)" % logfile)

        # Register log file
        pfw_save_file_info(pfw_dbh, filemgmt, 'log', [logfile], wcl['pfw_attempt_id'],
                           wcl['task_id']['attempt'], wcl['task_id']['jobwrapper'],
                           wcl['task_id']['jobwrapper'],
                           False, None)

        # since able to register log file, save as not junk file
        jobfiles['outfullnames'].append(logfile)

        # prep for copy log to archive(s)
        filename = miscutils.parse_fullname(logfile, miscutils.CU_PARSE_FILENAME)
        putinfo[filename] = {'src': logfile,
                             'filename': filename,
                             'fullname': logfile,
                             'compression': None,
                             'path': wcl['log_archive_path'],
                             'filetype': 'log',
                             'filesave': True,
                             'filecompress': False}
    else:
        miscutils.fwdebug_print("Warning: log doesn't exist (%s)" % logfile)

    return putinfo




######################################################################
def copy_output_to_archive(pfw_dbh, wcl, jobfiles, fileinfo, level, parent_task_id, task_label, exitcode):
    """ If requested, copy output file(s) to archive """
    # fileinfo[filename] = {filename, fullname, sectname}

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")
    putinfo = {}


    # check each output file definition to see if should save file
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("Checking for save_file_archive")

    for (filename, fdict) in fileinfo.items():
        if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
            miscutils.fwdebug_print("filename %s, fdict=%s" % (filename, fdict))
        (filename, compression) = miscutils.parse_fullname(fdict['fullname'],
                                       miscutils.CU_PARSE_FILENAME|miscutils.CU_PARSE_COMPRESSION)

        putinfo[filename] = {'src': fdict['fullname'],
                             'compression': compression,
                             'filename': filename,
                             'filetype': fdict['filetype'],
                             'filesave': fdict['filesave'],
                             'filecompress': fdict['filecompress'],
                             'path': fdict['path']}

    # transfer_job_to_archives(pfw_dbh, wcl, putinfo, level, parent_tid, task_label, exitcode):
    transfer_job_to_archives(pfw_dbh, wcl, jobfiles, putinfo, level,
                             parent_task_id, task_label, exitcode)

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")


######################################################################
def get_pfw_hdrupd(wcl):
    """ Create the dictionary with PFW values to be written to fits file header """
    hdrupd = {}
    hdrupd['pipeline'] = "%s/DESDM pipeline name/str" %  wcl.get('wrapper.pipeline')
    hdrupd['reqnum'] = "%s/DESDM processing request number/int" % wcl.get('reqnum')
    hdrupd['unitname'] = "%s/DESDM processing unit name/str" % wcl.get('unitname')
    hdrupd['attnum'] = "%s/DESDM processing attempt number/int" % wcl.get('attnum')
    hdrupd['eupsprod'] = "%s/eups pipeline meta-package name/str" % wcl.get('wrapper.pipeprod')
    hdrupd['eupsver'] = "%s/eups pipeline meta-package version/str" % wcl.get('wrapper.pipever')
    return hdrupd


######################################################################
def post_wrapper(pfw_dbh, wcl, jobfiles, logfile, exitcode):
    """ Execute tasks after a wrapper is done """
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")

    # Save disk usage for wrapper execution
    disku = pfwutils.diskusage(wcl['jobroot'])
    wcl['wrap_usage'] = disku - wcl['pre_disk_usage']

    # don't save logfile name if none was actually written
    if not os.path.isfile(logfile):
        logfile = None

    outputwclfile = wcl[pfwdefs.IW_WRAPSECT]['outputwcl']
    if not os.path.exists(outputwclfile):
        outputwclfile = None

    if pfw_dbh is not None:
        pfw_dbh.end_task(wcl['task_id']['wrapper'], exitcode, True)

    filemgmt = dynam_load_filemgmt(wcl, pfw_dbh, None, wcl['task_id']['jobwrapper'])

    finfo = {}

    # always try to save log file
    logfinfo = save_log_file(pfw_dbh, filemgmt, wcl, jobfiles, logfile)
    if logfinfo is not None and len(logfinfo) > 0:
        finfo.update(logfinfo)

    outputwcl = WCL()
    if outputwclfile and os.path.exists(outputwclfile):
        with open(outputwclfile, 'r') as outwclfh:
            outputwcl.read(outwclfh, filename=outputwclfile)

        if pfw_dbh is not None:
            pfw_dbh.update_wrapper_end(wcl, outputwclfile, logfile, exitcode, wcl['wrap_usage'])

        # add wcl file to list of non-junk output files
        jobfiles['outfullnames'].append(outputwclfile)

        # append output wcl file to tarball for returning to submit machine
        try:
            with tarfile.open(wcl['condor_job_init_dir'] + '/' + wcl['output_wcl_tar'], 'a') as tar:
                tar.add(outputwclfile)
        except Exception as err:
            warnmsg = "Warning:  Could not append output wcl file to tarball: %s" % err
            if pfw_dbh is not None:
                pfw_dbh.insert_message(wcl['pfw_attempt_id'], wcl['task_id']['jobwrapper'],
                                       pfwdefs.PFWDB_MSG_WARN, warnmsg)
            print warnmsg
            print "\tContinuing job"


        # handle output files - file metadata, prov, copying to archive
        if outputwcl is not None and len(outputwcl) > 0:
            pfw_hdrupd = get_pfw_hdrupd(wcl)
            execs = intgmisc.get_exec_sections(outputwcl, pfwdefs.OW_EXECPREFIX)
            for sect in execs:
                if pfw_dbh is not None:
                    pfw_dbh.update_exec_end(outputwcl[sect], wcl['task_id']['exec'][sect])
                else:
                    print "DESDMTIME: app_exec %s %0.3f" % (sect,
                                                            float(outputwcl[sect]['walltime']))

            if exitcode == 0:   # problems with data in output wcl if non-zero exit code
                if pfwdefs.OW_OUTPUTS_BY_SECT in outputwcl and \
                   len(outputwcl[pfwdefs.OW_OUTPUTS_BY_SECT]) > 0:
                    wrap_output_files = []
                    for sectname, byexec in outputwcl[pfwdefs.OW_OUTPUTS_BY_SECT].items():
                        sectdict = wcl[pfwdefs.IW_FILESECT][sectname]
                        filesave = miscutils.checkTrue(pfwdefs.SAVE_FILE_ARCHIVE, sectdict, True)
                        filecompress = miscutils.checkTrue(pfwdefs.COMPRESS_FILES, sectdict, False)

                        updatedef = {}
                        # get any hdrupd secton from inputwcl
                        for key, val in sectdict.items():
                            if key.startswith('hdrupd'):
                                updatedef[key] = val

                        # add pfw hdrupd values
                        updatedef['hdrupd_pfw'] = pfw_hdrupd
                        if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
                            miscutils.fwdebug_print("sectname %s, updatedef=%s" % (sectname, updatedef))

                        for ekey, elist in byexec.items():
                            fullnames = miscutils.fwsplit(elist, ',')
                            task_id = wcl['task_id']['exec'][ekey]
                            wrap_output_files.extend(fullnames)
                            pfw_save_file_info(pfw_dbh, filemgmt, sectdict['filetype'], fullnames, wcl['pfw_attempt_id'],
                                               wcl['task_id']['attempt'],
                                               wcl['task_id']['jobwrapper'],
                                               task_id, True, updatedef)

                            for fname in fullnames:
                                finfo[fname] = {'sectname': sectname,
                                                'filetype': sectdict['filetype'],
                                                'filesave': filesave,
                                                'filecompress': filecompress,
                                                'fullname': fname}
                                if 'archivepath' in sectdict:
                                    finfo[fname]['path'] = sectdict['archivepath']

                    jobfiles['outfullnames'].extend(wrap_output_files)

                prov = None
                execids = None
                if pfwdefs.OW_PROVSECT in outputwcl and \
                   len(outputwcl[pfwdefs.OW_PROVSECT].keys()) > 0:
                    prov = outputwcl[pfwdefs.OW_PROVSECT]
                    execids = wcl['task_id']['exec']
                    filemgmt.ingest_provenance(prov, execids)
            filemgmt.commit()

    if len(finfo) > 0:
        save_trans_end_of_job(pfw_dbh, wcl, jobfiles, finfo)
        copy_output_to_archive(pfw_dbh, wcl, jobfiles, finfo, 'wrapper', wcl['task_id']['jobwrapper'], 'wrapper_output', exitcode)

    # clean up any input files no longer needed - TODO

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")
# end postwrapper


######################################################################
def parse_wrapper_line(line, linecnt):
    """ Parse a line from the job's wrapper list """
    wrapinfo = {}
    lineparts = miscutils.fwsplit(line.strip())
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


######################################################################
def gather_initial_fullnames():
    """ save fullnames for files initially in job scratch directory 
        so won't appear in junk tarball """

    infullnames = []
    for (dirpath, dirnames, filenames) in os.walk('.'):
        dpath = dirpath[2:]
        if len(dpath) > 0:
            dpath += '/'
        for fname in filenames:
            infullnames.append('%s%s' % (dpath, fname))

    if miscutils.fwdebug_check(6, 'PFWRUNJOB_DEBUG'):
        miscutils.fwdebug_print("initial infullnames=%s" % infullnames)
    return infullnames

######################################################################
def exechost_status(wrapnum):
    """ Print various information about exec host """

    exechost = socket.gethostname()

    # free
    try:
        subp = subprocess.Popen(["free", "-m"], stdout=subprocess.PIPE)
        output = subp.communicate()[0]
        print "%04d: EXECSTAT %s FREE\n%s" % (int(wrapnum), exechost, output)
    except:
        print "Problem running free command"
        (extype, exvalue, trback) = sys.exc_info()
        traceback.print_exception(extype, exvalue, trback, limit=1, file=sys.stdout)
        print "Ignoring error and continuing...\n"

    # df
    try:
        cwd = os.getcwd()
        subp = subprocess.Popen(["df", "-h", cwd], stdout=subprocess.PIPE)
        output = subp.communicate()[0]
        print "%04d: EXECSTAT %s DF\n%s" % (int(wrapnum), exechost, output)
    except:
        print "Problem running df command"
        (extype, exvalue, trback) = sys.exc_info()
        traceback.print_exception(extype, exvalue, trback, limit=1, file=sys.stdout)
        print "Ignoring error and continuing...\n"



def job_workflow(workflow, jobfiles, jobwcl=WCL()):
    """ Run each wrapper execution sequentially """

    linecnt = 0
    with open(workflow, 'r') as workflowfh:
        # for each wrapper execution
        line = workflowfh.readline()
        linecnt += 1
        while line:
            task = parse_wrapper_line(line, linecnt)

            print "\n\n%04d: %s" % (int(task['wrapnum']), '*' * 60)

            # print machine status information
            exechost_status(task['wrapnum'])

            wrappercmd = "%s %s" % (task['wrapname'], task['wclfile'])

            if not os.path.exists(task['wclfile']):
                print "Error: input wcl file does not exist (%s)" % task['wclfile']
                return 1

            wcl = WCL()
            with open(task['wclfile'], 'r') as wclfh:
                wcl.read(wclfh, filename=task['wclfile'])
            wcl.update(jobwcl)

            print "%04d: module %s " % (int(task['wrapnum']), wcl['modname'])

            job_task_id = wcl['task_id']['job']

            pfw_dbh = None
            if wcl['use_db']:
                pfw_dbh = pfwdb.PFWDB()
                wcl['task_id']['jobwrapper'] = pfw_dbh.create_task(name='jobwrapper',
                                                                   info_table=None,
                                                                   parent_task_id=job_task_id,
                                                                   root_task_id=wcl['task_id']['attempt'],
                                                                   label=None,
                                                                   do_begin=True,
                                                                   do_commit=True)
            else:
                wcl['task_id']['jobwrapper'] = -1

            print "%04d: Setup" % (int(task['wrapnum']))
            setup_wrapper(pfw_dbh, wcl, jobfiles, task['logfile'])

            if pfw_dbh is not None:
                wcl['task_id']['wrapper'] = pfw_dbh.insert_wrapper(wcl, task['wclfile'],
                                                                   wcl['task_id']['jobwrapper'])
                create_exec_tasks(pfw_dbh, wcl)
                exectid = determine_exec_task_id(pfw_dbh, wcl)
                pfw_dbh.begin_task(wcl['task_id']['wrapper'], True)
                pfw_dbh.close()
                pfw_dbh = None
            else:
                wcl['task_id']['wrapper'] = -1
                exectid = -1

            print "%04d: Running wrapper: %s" % (int(task['wrapnum']), wrappercmd)
            starttime = time.time()
            try:
                os.putenv("DESDMFW_TASKID", str(exectid))
                exitcode = pfwutils.run_cmd_qcf(wrappercmd, task['logfile'],
                                                wcl['task_id']['wrapper'],
                                                wcl['execnames'], 5000, wcl['use_qcf'])
            except:
                (extype, exvalue, trback) = sys.exc_info()
                print '!' * 60
                if wcl['use_db']:
                    pfw_dbh = pfwdb.PFWDB()
                    pfw_dbh.insert_message(wcl['pfw_attempt_id'], wcl['task_id']['wrapper'], pfwdefs.PFWDB_MSG_ERROR,
                                           "%s: %s" % (extype, str(exvalue)))
                else:
                    print "DESDMTIME: run_cmd_qcf %0.3f" % (time.time()-starttime)
                traceback.print_exception(extype, exvalue, trback, file=sys.stdout)
                exitcode = pfwdefs.PF_EXIT_FAILURE


            if exitcode != 0:
                print "Error: wrapper %s exited with non-zero exit code %s.   Check log:" % \
                    (wcl[pfwdefs.PF_WRAPNUM], exitcode),
                logfilename = miscutils.parse_fullname(wcl['log'], miscutils.CU_PARSE_FILENAME)
                print " %s/%s" % (wcl['log_archive_path'], logfilename)

            if wcl['use_db']:
                if pfw_dbh is None:
                    pfw_dbh = pfwdb.PFWDB()
            else:
                print "DESDMTIME: run_wrapper %0.3f" % (time.time()-starttime)

            print "%04d: Post-steps (exit: %s)" % (int(task['wrapnum']), exitcode)
            post_wrapper(pfw_dbh, wcl, jobfiles, task['logfile'], exitcode)
            if wcl['wrap_usage'] > jobwcl['job_max_usage']:
                jobwcl['job_max_usage'] = wcl['wrap_usage']

            if pfw_dbh is not None:
                pfw_dbh.end_task(wcl['task_id']['jobwrapper'], exitcode, True)

            sys.stdout.flush()
            sys.stderr.flush()
            if exitcode:
                print "Aborting due to non-zero exit code"
                return exitcode
            line = workflowfh.readline()

    return 0



def run_job(args):
    """Run tasks inside single job"""

    jobwcl = WCL()
    jobfiles = {'infullnames': [args.config, args.workflow],
                'outfullnames': [],
                'output_putinfo': {}}

    jobstart = time.time()
    with open(args.config, 'r') as wclfh:
        jobwcl.read(wclfh, filename=args.config)
    jobwcl['use_db'] = miscutils.checkTrue('usedb', jobwcl, True)
    jobwcl['use_qcf'] = miscutils.checkTrue('useqcf', jobwcl, False)

    jobwcl['jobroot'] = os.getcwd()
    jobwcl['job_max_usage'] = 0
    #jobwcl['pre_job_disk_usage'] = pfwutils.diskusage(jobwcl['jobroot'])
    jobwcl['pre_job_disk_usage'] = 0

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
    elif '_CONDOR_JOB_AD' in os.environ:
        batch_id = get_batch_id_from_job_ad(os.environ['_CONDOR_JOB_AD'])

    pfw_dbh = None
    if jobwcl['use_db']:
        # export serviceAccess info to environment
        if 'des_services' in jobwcl:
            os.environ['DES_SERVICES'] = jobwcl['des_services']
        if 'des_db_section' in jobwcl:
            os.environ['DES_DB_SECTION'] = jobwcl['des_db_section']

        # update job batch/condor ids
        pfw_dbh = pfwdb.PFWDB()
        pfw_dbh.update_job_target_info(jobwcl, condor_id, batch_id, socket.gethostname())
        pfw_dbh.close()    # in case job is long running, will reopen connection elsewhere in job
        pfw_dbh = None

    # Save pointers to archive information for quick lookup
    if jobwcl[pfwdefs.USE_HOME_ARCHIVE_INPUT] != 'never' or \
       jobwcl[pfwdefs.USE_HOME_ARCHIVE_OUTPUT] != 'never':
        jobwcl['home_archive_info'] = jobwcl[pfwdefs.SW_ARCHIVESECT][jobwcl[pfwdefs.HOME_ARCHIVE]]
    else:
        jobwcl['home_archive_info'] = None

    if jobwcl[pfwdefs.USE_TARGET_ARCHIVE_INPUT] != 'never' or \
            jobwcl[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT] != 'never':
        jobwcl['target_archive_info'] = jobwcl[pfwdefs.SW_ARCHIVESECT][jobwcl[pfwdefs.TARGET_ARCHIVE]]
    else:
        jobwcl['target_archive_info'] = None


    job_task_id = jobwcl['task_id']['job']

    # run the tasks (i.e., each wrapper execution)
    pfw_dbh = None
    try:
        jobfiles['infullnames'] = gather_initial_fullnames()
        exitcode = job_workflow(args.workflow, jobfiles, jobwcl)
    except Exception:
        (extype, exvalue, trback) = sys.exc_info()
        print '!' * 60
        if jobwcl['use_db'] and pfw_dbh is None:
            pfw_dbh = pfwdb.PFWDB()
            pfw_dbh.insert_message(jobwcl['pfw_attempt_id'], job_task_id, pfwdefs.PFWDB_MSG_ERROR,
                                   "%s: %s" % (extype, str(exvalue)))
        traceback.print_exception(extype, exvalue, trback, file=sys.stdout)
        exitcode = pfwdefs.PF_EXIT_FAILURE
        print "Aborting rest of wrapper executions.  Continuing to end-of-job tasks\n\n"

    if jobwcl['use_db'] and pfw_dbh is None:
        pfw_dbh = pfwdb.PFWDB()

    # create junk tarball with any unknown files
    create_junk_tarball(pfw_dbh, jobwcl, jobfiles, exitcode)

    # if should transfer at end of job
    if len(jobfiles['output_putinfo']) > 0:
        print "\n\nCalling file transfer for end of job (%s files)" % \
              (len(jobfiles['output_putinfo']))

        copy_output_to_archive(pfw_dbh, jobwcl, jobfiles, jobfiles['output_putinfo'], 'job', job_task_id, 'job_output', exitcode)
    else:
        print "\n\n0 files to transfer for end of job"
        if miscutils.fwdebug_check(1, "PFWRUNJOB_DEBUG"):
            miscutils.fwdebug_print("len(jobfiles['outfullnames'])=%s" % \
                                    (len(jobfiles['outfullnames'])))

    if pfw_dbh is not None:
        disku = pfwutils.diskusage(jobwcl['jobroot'])
        curr_usage = disku - jobwcl['pre_job_disk_usage']
        if curr_usage > jobwcl['job_max_usage']:
            jobwcl['job_max_usage'] = curr_usage
        pfw_dbh.update_tjob_info(jobwcl, jobwcl['task_id']['job'], {'diskusage': jobwcl['job_max_usage']})
        pfw_dbh.commit()
        pfw_dbh.close()
    else:
        print "\nDESDMTIME: pfwrun_job %0.3f" % (time.time()-jobstart)

    return exitcode

###############################################################################
def create_compression_wdf(used_fnames, wgb_fnames):
    """ Create the was derived from provenance for the compression """
    wdf = {}
    cnt = 1
    for child in wgb_fnames:
        parent = os.path.splitext(child)[0]
        wdf['derived_%s' % cnt] = {provdefs.PROV_PARENTS: parent, provdefs.PROV_CHILDREN: child}
        cnt += 1

    return wdf


###############################################################################
def call_compress_files(pfw_dbh, jobwcl, jobfiles, putinfo, exitcode):
    """ Compress output files as specified """

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")

    task_id = None
    compress_ver = pfwutils.get_version(jobwcl[pfwdefs.COMPRESSION_EXEC],
                                        jobwcl[pfwdefs.IW_EXEC_DEF])
    if pfw_dbh is not None:
        task_id = pfw_dbh.create_task(name='compress_files',
                                      info_table='compress_task',
                                      parent_task_id=jobwcl['task_id']['job'],
                                      root_task_id=jobwcl['task_id']['attempt'],
                                      label=None,
                                      do_begin=True,
                                      do_commit=True)
        # add to compress_task table
        pfw_dbh.insert_compress_task(task_id, jobwcl[pfwdefs.COMPRESSION_EXEC],
                                     compress_ver, jobwcl[pfwdefs.COMPRESSION_ARGS],
                                     putinfo)


    # determine which files need to be compressed
    to_compress = []
    for fname, fdict in putinfo.items():
        if fdict['filecompress']:
            to_compress.append(fdict['src'])
            

    if miscutils.fwdebug_check(6, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("to_compress = %s" % to_compress)

    errcnt = 0
    tot_bytes_after = 0
    if len(to_compress) > 0:
        (results, tot_bytes_before, tot_bytes_after) = pfwcompress.compress_files(to_compress,
                                                                                  jobwcl[pfwdefs.COMPRESSION_SUFFIX],
                                                                                  jobwcl[pfwdefs.COMPRESSION_EXEC],
                                                                                  jobwcl[pfwdefs.COMPRESSION_ARGS],
                                                                                  3, jobwcl[pfwdefs.COMPRESSION_CLEANUP])
        if pfw_dbh is not None:
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)

        filelist = []
        for fname, fdict in results.items():
            if miscutils.fwdebug_check(3, 'PFWRUNJOB_DEBUG'):
                miscutils.fwdebug_print("%s = %s" % (fname, fdict))

            if fdict['err'] is None:
                # add new filename to jobfiles['outfullnames'] so not junk
                jobfiles['outfullnames'].append(fdict['outname'])

                # update jobfiles['output_putinfo'] for transfer
                (filename, compression) = miscutils.parse_fullname(fdict['outname'],
                                                                   miscutils.CU_PARSE_FILENAME | miscutils.CU_PARSE_EXTENSION)
                if filename in putinfo:
                    # info for desfile entry
                    dinfo = diskutils.get_single_file_disk_info(fdict['outname'],
                                                                save_md5sum=True,
                                                                archive_root=None)
                    # compressed file should be one saved to archive
                    putinfo[filename]['src'] = fdict['outname']
                    putinfo[filename]['compression'] = compression
                    putinfo[filename]['dst'] += compression

                    del dinfo['path']
                    dinfo['fullname'] = fdict['outname']
                    dinfo['pfw_attempt_id'] = int(jobwcl['pfw_attempt_id'])
                    dinfo['filetype'] = putinfo[filename]['filetype']
                    dinfo['wgb_task_id'] = task_id
                    filelist.append(dinfo)

                else:
                    miscutils.fwdie("Error: compression mismatch %s" % filename, pfwdefs.PF_EXIT_FAILURE)
            else:  # errstr
                miscutils.fwdebug_print("WARN: problem compressing file - %s" % fdict['err'])
                errcnt += 1

        # register compressed file with file manager, save used provenance info
        filemgmt = dynam_load_filemgmt(jobwcl, pfw_dbh, None, task_id)
        filemgmt.create_artifacts(filelist)
        used_fnames = [os.path.basename(x) for x in to_compress]
        wgb_fnames = [os.path.basename(x['fullname']) for x in filelist]

        prov = {provdefs.PROV_USED: {'exec_1': provdefs.PROV_DELIM.join(used_fnames)},
                provdefs.PROV_WGB: {'exec_1': provdefs.PROV_DELIM.join(wgb_fnames)},
                provdefs.PROV_WDF: create_compression_wdf(used_fnames, wgb_fnames)}
        filemgmt.ingest_provenance(prov, {'exec_1': task_id})
        force_update_desfile_filetype(filemgmt, filelist)
        filemgmt.commit()

    if pfw_dbh is not None:
        pfw_dbh.update_compress_task(task_id, errcnt, tot_bytes_after)

    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END")

################################################################################
def force_update_desfile_filetype(dbh, filelist):
    """ Force update filetype in desfile table for compressed files """

    sql = "update desfile set filetype=%s where filename=%s and compression = %s" % \
        (dbh.get_named_bind_string('filetype'),
         dbh.get_named_bind_string('filename'),
         dbh.get_named_bind_string('compression'))
    curs = dbh.cursor()
    curs.prepare(sql)
    for dinfo in filelist:
        params = {'filename': dinfo['filename'],
                  'compression': dinfo['compression'],
                  'filetype': dinfo['filetype']}
        curs.execute(None, params)
    dbh.commit()

################################################################################
def create_junk_tarball(pfw_dbh, wcl, jobfiles, exitcode):
    """ Create the junk tarball """

    if not pfwdefs.CREATE_JUNK_TARBALL in wcl or \
       not miscutils.convertBool(wcl[pfwdefs.CREATE_JUNK_TARBALL]):
        return

    # input files are what files where staged by framework (i.e., input wcl)
    # output files are only those listed as outputs in outout wcl
    if miscutils.fwdebug_check(1, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("BEG")
        miscutils.fwdebug_print("# infullnames = %s" % len(jobfiles['infullnames']))
        miscutils.fwdebug_print("# outfullnames = %s" % len(jobfiles['outfullnames']))
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("infullnames = %s" % jobfiles['infullnames'])
        miscutils.fwdebug_print("outfullnames = %s" % jobfiles['outfullnames'])

    job_task_id = wcl['task_id']['job']

    junklist = []

    # remove paths
    notjunk = {}
    for fname in jobfiles['infullnames']:
        notjunk[os.path.basename(fname)] = True
    for fname in jobfiles['outfullnames']:
        notjunk[os.path.basename(fname)] = True

    if miscutils.fwdebug_check(6, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("notjunk = %s" % notjunk.keys())

    # walk job directory to get all files
    cwd = '.'
    for (dirpath, _, filenames) in os.walk(cwd):
        for walkname in filenames:
            if miscutils.fwdebug_check(6, "PFWRUNJOB_DEBUG"):
                miscutils.fwdebug_print("walkname = %s" % walkname)
            if walkname not in notjunk:
                if miscutils.fwdebug_check(6, "PFWRUNJOB_DEBUG"):
                    miscutils.fwdebug_print("Appending walkname to list = %s" % walkname)

                if dirpath.startswith('./'):
                    dirpath = dirpath[2:]
                elif dirpath == '.':
                    dirpath = ''
                if len(dirpath) > 0:
                    fname = "%s/%s" % (dirpath, walkname)
                else:
                    fname = walkname

                junklist.append(fname)



    if miscutils.fwdebug_check(1, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("# in junklist = %s" % len(junklist))
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("junklist = %s" % junklist)

    putinfo = {}
    if len(junklist) > 0:
        task_id = -1
        if pfw_dbh is not None:
            task_id = pfw_dbh.create_task(name='create_junktar',
                                          info_table=None,
                                          parent_task_id=job_task_id,
                                          root_task_id=wcl['task_id']['attempt'],
                                          label=None,
                                          do_begin=True,
                                          do_commit=True)

        pfwutils.tar_list(wcl['junktar'], junklist)

        if pfw_dbh is not None:
            pfw_dbh.update_job_junktar(wcl, wcl['junktar'])
            pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)

        # register junktar with file manager
        filemgmt = dynam_load_filemgmt(wcl, pfw_dbh, None, job_task_id)
        pfw_save_file_info(pfw_dbh, filemgmt, 'junk_tar', [wcl['junktar']], wcl['pfw_attempt_id'],
                           wcl['task_id']['attempt'], job_task_id, job_task_id,
                           False, None)

        parsemask = miscutils.CU_PARSE_FILENAME|miscutils.CU_PARSE_COMPRESSION
        (filename, compression) = miscutils.parse_fullname(wcl['junktar'], parsemask)

        # gather "disk" metadata about tarball
        putinfo = {wcl['junktar']: {'src': wcl['junktar'],
                                    'filename': filename,
                                    'fullname': wcl['junktar'],
                                    'compression': compression,
                                    'path': wcl['junktar_archive_path'],
                                    'filetype': 'junk_tar',
                                    'filesave': True,
                                    'filecompress': False}}

        # if save setting is wrapper, save junktar here, otherwise save at end of job
        save_trans_end_of_job(pfw_dbh, wcl, jobfiles, putinfo)
        transfer_job_to_archives(pfw_dbh, wcl, jobfiles, putinfo, 'wrapper',
                                 job_task_id, 'junktar', exitcode)

    if miscutils.fwdebug_check(1, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("END\n\n")

    if len(putinfo) > 0:
        jobfiles['output_putinfo'].update(putinfo)


######################################################################
def parse_args(argv):
    """ Parse the command line arguments """
    parser = argparse.ArgumentParser(description='pfwrun_job.py')
    parser.add_argument('--version', action='store_true', default=False)
    parser.add_argument('--config', action='store', required=True)
    parser.add_argument('workflow', action='store')

    args = parser.parse_args(argv)

    if args.version:
        print __version__
        sys.exit(0)

    return args


######################################################################
def get_semaphore(wcl, stype, dest, trans_task_id):
    """ create semaphore if being used """
    if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
        miscutils.fwdebug_print("get_semaphore: stype=%s dest=%s tid=%s" % (stype, dest, trans_task_id))

    sem = None
    if wcl['use_db']:
        semname = None
        if dest.lower() == 'target' and '%s_transfer_semname_target' % stype.lower() in wcl:
            semname = wcl['%s_transfer_semname_target' % stype.lower()]
        elif dest.lower() != 'target' and '%s_transfer_semname_home' % stype.lower() in wcl:
            semname = wcl['%s_transfer_semname_home' % stype.lower()]
        elif '%s_transfer_semname' % stype.lower() in wcl:
            semname = wcl['%s_transfer_semname' % stype.lower()]
        elif 'transfer_semname' in wcl:
            semname = wcl['transfer_semname']

        if semname is not None and semname != '__NONE__':
            sem = dbsem.DBSemaphore(semname, trans_task_id)
            if miscutils.fwdebug_check(3, "PFWRUNJOB_DEBUG"):
                miscutils.fwdebug_print("Semaphore info: %s" % str(sem))
    return sem

if __name__ == '__main__':
    os.putenv('PYTHONUNBUFFERED', 'true')
    print "Cmdline given: %s" % ' '.join(sys.argv)
    sys.exit(run_job(parse_args(sys.argv[1:])))
