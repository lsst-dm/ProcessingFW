#!usr/bin/env python

# pylint: disable=print-statement

""" Miscellaneous support functions for processing framework """

import re
import os
import sys
import tarfile
import errno
import time
import subprocess
import shlex

import despymisc.miscutils as miscutils
import processingfw.pfwdefs as pfwdefs
import qcframework.Messaging as Messaging


def pad_jobnum(jobnum):
    """ Pad the job number """
    return "%04d" % int(jobnum)


def get_hdrup_sections(wcl, prefix):
    """ Returns header update sections appearing in given wcl """
    hdrups = {}
    for key, val in list(wcl.items()):
        if miscutils.fwdebug_check(3, "PFWUTILS_DEBUG"):
            miscutils.fwdebug_print("\tsearching for hdrup prefix in %s" % key)

        if re.search(r"^%s\S+$" % prefix, key):
            if miscutils.fwdebug_check(3, "PFWUTILS_DEBUG"):
                miscutils.fwdebug_print("\tFound hdrup prefex %s" % key)
            hdrups[key] = val
    return hdrups


def search_wcl_for_variables(wcl):
    """ Find variables in given wcl """
    if miscutils.fwdebug_check(9, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("BEG")
    usedvars = {}
    for key, val in list(wcl.items()):
        if isinstance(val, dict):
            uvars = search_wcl_for_variables(val)
            if uvars is not None:
                usedvars.update(uvars)
        elif isinstance(val, str):
            viter = [m.group(1) for m in re.finditer(r'(?i)\$\{([^}]+)\}', val)]
            for vstr in viter:
                if ':' in vstr:
                    vstr = vstr.split(':')[0]
                usedvars[vstr] = True
        else:
            if miscutils.fwdebug_check(9, "PFWUTILS_DEBUG"):
                miscutils.fwdebug_print("Note: wcl is not string.")
                miscutils.fwdebug_print("key = %s, type(val) = %s, val = '%s'" %
                                        (key, type(val), val))

    if miscutils.fwdebug_check(9, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("END")
    return usedvars


def get_wcl_value(key, wcl):
    """ Return value of key from wcl, follows section notation """
    if miscutils.fwdebug_check(9, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("BEG")
    val = wcl
    for k in key.split('.'):
        #print "get_wcl_value: k=", k
        val = val[k]
    if miscutils.fwdebug_check(9, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("END")
    return val


def set_wcl_value(key, val, wcl):
    """ Sets value of key in wcl, follows section notation """
    if miscutils.fwdebug_check(9, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("BEG")
    wclkeys = key.split('.')
    valkey = wclkeys.pop()
    wcldict = wcl
    for k in wclkeys:
        wcldict = wcldict[k]

    wcldict[valkey] = val
    if miscutils.fwdebug_check(9, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("END")


def tar_dir(filename, indir):
    """ Tars a directory """
    if filename.endswith('.gz'):
        mode = 'w:gz'
    else:
        mode = 'w'
    with tarfile.open(filename, mode) as tar:
        tar.add(indir)


def tar_list(tarfilename, filelist):
    """ Tars a directory """

    if tarfilename.endswith('.gz'):
        mode = 'w:gz'
    else:
        mode = 'w'

    with tarfile.open(tarfilename, mode) as tar:
        for filen in filelist:
            tar.add(filen)


def untar_dir(filename, outputdir):
    """ Untars a directory """
    if filename.endswith('.gz'):
        mode = 'r:gz'
    else:
        mode = 'r'

    maxcnt = 4
    cnt = 1
    done = False
    while not done and cnt <= maxcnt:
        with tarfile.open(filename, mode) as tar:
            try:
                tar.extractall(outputdir)
                done = True
            except OSError as exc:
                if exc.errno == errno.EEXIST:
                    print("Problems untaring %s: %s" % (filename, exc))
                    if cnt < maxcnt:
                        print("Trying again.")
                else:
                    print("Error: %s" % exc)
                    raise
        cnt += 1

    if not done:
        print("Could not untar %s.  Aborting" % filename)


# assumes exit code for version is 0
def get_version(execname, execdefs):
    """run command with version flag and parse output for version"""

    ver = None
    if (execname.lower() in execdefs and
            'version_flag' in execdefs[execname.lower()] and
            'version_pattern' in execdefs[execname.lower()]):
        verflag = execdefs[execname.lower()]['version_flag']
        verpat = execdefs[execname.lower()]['version_pattern']

        cmd = "%s %s" % (execname, verflag)
        try:
            process = subprocess.Popen(cmd.split(),
                                       shell=False,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
        except:
            (extype, exvalue, _) = sys.exc_info()
            print("********************")
            print("Unexpected error: %s - %s" % (extype, exvalue))
            print("cmd> %s" % cmd)
            print("Probably could not find %s in path" % cmd.split()[0])
            print("Check for mispelled execname in submit wcl or")
            print("    make sure that the corresponding eups package is in the metapackage ")
            print("    and it sets up the path correctly")
            raise

        process.wait()
        out = process.communicate()[0]
        if process.returncode != 0:
            miscutils.fwdebug_print("INFO:  problem when running code to get version")
            miscutils.fwdebug_print("\t%s %s %s" % (execname, verflag, verpat))
            miscutils.fwdebug_print("\tcmd> %s" % cmd)
            miscutils.fwdebug_print("\t%s" % out)
            ver = None
        else:
            # parse output with verpat
            try:
                pmatch = re.search(verpat, out)
                if pmatch:
                    ver = pmatch.group(1)
                else:
                    if miscutils.fwdebug_check(1, "PFWUTILS_DEBUG"):
                        miscutils.fwdebug_print("re.search didn't find version for exec %s" %
                                                execname)
                    if miscutils.fwdebug_check(3, "PFWUTILS_DEBUG"):
                        miscutils.fwdebug_print("\tcmd output=%s" % out)
                        miscutils.fwdebug_print("\tcmd verpat=%s" % verpat)
            except Exception as err:
                #print type(err)
                ver = None
                print("Error: Exception from re.match.  Didn't find version: %s" % err)
                raise
    else:
        if miscutils.fwdebug_check(3, "PFWUTILS_DEBUG"):
            miscutils.fwdebug_print("INFO: Could not find version info for exec %s" % execname)

    return ver


def run_cmd_qcf(cmd, logfilename, wid, execnames, use_qcf=False, dbh=None, pfwattid=0, patterns={}):
    """ Execute the command piping stdout/stderr to log and QCF """
    bufsize = 1024 * 10

    if miscutils.fwdebug_check(3, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("BEG")
        miscutils.fwdebug_print("working dir = %s" % (os.getcwd()))
        miscutils.fwdebug_print("cmd = %s" % cmd)
        miscutils.fwdebug_print("logfilename = %s" % logfilename)
        miscutils.fwdebug_print("wid = %s" % wid)
        miscutils.fwdebug_print("execnames = %s" % execnames)
        miscutils.fwdebug_print("use_qcf = %s" % use_qcf)

    use_qcf = miscutils.convertBool(use_qcf)

    sys.stdout.flush()
    try:
        messaging = Messaging.Messaging(logfilename, execnames, pfwattid=pfwattid, taskid=wid,
                                        dbh=dbh, usedb=use_qcf, qcf_patterns=patterns)
        process_wrap = subprocess.Popen(shlex.split(cmd),
                                        shell=False,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT)
    except:
        (extype, exvalue, _) = sys.exc_info()
        print("********************")
        print("Unexpected error: %s - %s" % (extype, exvalue))
        print("cmd> %s" % cmd)
        print("Probably could not find %s in path" % cmd.split()[0])
        print("Check for mispelled execname in submit wcl or")
        print("    make sure that the corresponding eups package is in the metapackage ")
        print("    and it sets up the path correctly")
        raise

    try:
        buf = os.read(process_wrap.stdout.fileno(), bufsize)
        while process_wrap.poll() == None or len(buf) != 0:
            messaging.write(buf)
            buf = os.read(process_wrap.stdout.fileno(), bufsize)

    except IOError as exc:
        print("\tI/O error({0}): {1}".format(exc.errno, exc.strerror))

    except:
        (extype, exvalue, _) = sys.exc_info()
        print("\tError: Unexpected error: %s - %s" % (extype, exvalue))
        raise

    sys.stdout.flush()
    if miscutils.fwdebug_check(3, "PFWUTILS_DEBUG"):
        if process_wrap.returncode != 0:
            miscutils.fwdebug_print("\tInfo: cmd exited with non-zero exit code = %s" %
                                    process_wrap.returncode)
            miscutils.fwdebug_print("\tInfo: failed cmd = %s" % cmd)
        else:
            miscutils.fwdebug_print("\tInfo: cmd exited with exit code = 0")

    if miscutils.fwdebug_check(3, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("END")
    return process_wrap.returncode


def index_job_info(jobinfo):
    """ create dictionary of jobs indexed on blk task id """
    job_byblk = {}
    for j, jdict in list(jobinfo.items()):
        blktid = jdict['pfw_block_task_id']
        if blktid not in job_byblk:
            job_byblk[blktid] = {}
        job_byblk[blktid][j] = jdict

    return job_byblk


def index_wrapper_info(wrapinfo):
    """ create dictionaries of wrappers indexed on jobnum and modname """
    wrap_byjob = {}
    wrap_bymod = {}
    for wrap in list(wrapinfo.values()):
        if wrap['pfw_job_task_id'] not in wrap_byjob:
            wrap_byjob[wrap['pfw_job_task_id']] = {}
        wrap_byjob[wrap['pfw_job_task_id']][wrap['wrapnum']] = wrap
        if wrap['modname'] not in wrap_bymod:
            wrap_bymod[wrap['modname']] = {}
        wrap_bymod[wrap['modname']][wrap['wrapnum']] = wrap

    return wrap_byjob, wrap_bymod


def index_jobwrapper_info(jwrapinfo):
    """ create dictionaries of wrappers indexed on jobnum and wrapnum """

    jwrap_byjob = {}
    jwrap_bywrap = {}
    for jwrap in list(jwrapinfo.values()):
        if jwrap['label'] is None:
            print("Missing label for jobwrapper task.")
            print("Make sure you are using print_job.py from same ProcessingFW version as processing attempt")
            sys.exit(1)
        if jwrap['parent_task_id'] not in jwrap_byjob:
            jwrap_byjob[jwrap['parent_task_id']] = {}
        jwrap_byjob[jwrap['parent_task_id']][int(jwrap['label'])] = jwrap
        jwrap_bywrap[int(jwrap['label'])] = jwrap

    return jwrap_byjob, jwrap_bywrap


def should_save_file(mastersave, filesave, exitcode):
    """ Determine whether should save the file """
    msave = mastersave.lower()
    fsave = miscutils.convertBool(filesave)

    if msave == 'failure':
        if exitcode != 0:
            msave = 'always'
        else:
            msave = 'file'

    retval = ((msave == 'always') or (msave == 'file' and fsave))
    return retval


def should_compress_file(mastercompress, filecompress, exitcode):
    """ Determine whether should compress the file """

    if miscutils.fwdebug_check(6, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("BEG: master=%s, file=%s, exitcode=%s" %
                                (mastercompress, filecompress, exitcode))

    mcompress = mastercompress
    if isinstance(mastercompress, str):
        mcompress = mastercompress.lower()

    fcompress = miscutils.convertBool(filecompress)

    if mcompress == 'success':
        if exitcode != 0:
            mcompress = 'never'
        else:
            mcompress = 'file'

    retval = (mcompress == 'file' and fcompress)

    if miscutils.fwdebug_check(6, "PFWUTILS_DEBUG"):
        miscutils.fwdebug_print("END - retval = %s" % retval)
    return retval


def pfw_dynam_load_class(pfw_dbh, wcl, parent_tid, attempt_task_id,
                         label, classname, extra_info):
    """ Dynamically load a class save timing info in task table """

    #task_id = -1
    #if pfw_dbh is not None:
    #    task_id = pfw_dbh.create_task(name='dynclass',
    #                                  info_table=None,
    #                                  parent_task_id=parent_tid,
    #                                  root_task_id=attempt_task_id,
    #                                  label=label,
    #                                  do_begin=True,
    #                                  do_commit=True)

    the_class_obj = None
    try:
        the_class = miscutils.dynamically_load_class(classname)
        valdict = {}
        try:
            valdict = miscutils.get_config_vals(extra_info, wcl, the_class.requested_config_vals())
        except AttributeError: # in case the_class doesn't have requested_config_vals
            pass
        the_class_obj = the_class(valdict, wcl)
    except:
        (extype, exvalue, _) = sys.exc_info()
        msg = "Error: creating %s object - %s - %s" % (label, extype, exvalue)
        print("\n%s" % msg)
        if pfw_dbh is not None:
            Messaging.pfw_message(pfw_dbh, wcl['pfw_attempt_id'], parent_tid, msg, pfw_utils.PFWDB_MSG_ERROR)
        raise

    #if pfw_dbh is not None:
    #    pfw_dbh.end_task(task_id, pfwdefs.PF_EXIT_SUCCESS, True)

    return the_class_obj


def diskusage(path):
    #    """ Calls du to get disk space used by given path """
    #    process = subprocess.Popen(['du', '-s', path], shell=False,
    #                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    #    process.wait()
    #    out = process.communicate()[0]
    #    (diskusage, _) = out.split()
    #    return int(diskusage)
    """ Walks the path returning the sum of the filesizes """
    ### avoids symlinked files, but
    ### doesn't avoid adding hardlinks twice
    usum = 0
    for (dirpath, _, filenames) in os.walk(path):
        for name in filenames:
            if not os.path.islink('%s/%s' % (dirpath, name)):
                fsize = os.path.getsize('%s/%s' % (dirpath, name))
                if miscutils.fwdebug_check(6, "PUDISKU_DEBUG"):
                    miscutils.fwdebug_print("size of %s/%s = %s" % (dirpath, name, fsize))
                usum += fsize
    if miscutils.fwdebug_check(3, "PUDISKU_DEBUG"):
        miscutils.fwdebug_print("usum = %s" % usum)
    return usum
