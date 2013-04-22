#!/usr/bin/env python

import re
import subprocess
import argparse
import sys
import os
import time

from processingfw.fwutils import *
from processingfw.pfwdefs import *

import processingfw.pfwutils as pfwutils
import processingfw.pfwdb as pfwdb
import filemgmt.cache as cache
import intgutils.wclutils as wclutils


VERSION = '$Rev$'

# assumes exit code for version is 0
def getVersion(execname, verflag, verpat):
    """run command with version flag and parse output for version"""

    ver = None
    cmd = "%s %s" % (execname, verflag)
    process = subprocess.Popen(cmd.split(),
                               shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
    process.wait()
    out = process.communicate()[0]
    if process.returncode != 0:
        print "Warning:  problem when trying to get version"
        print "getVersion", execname, verflag, verpat
        print "\tcmd> ",cmd
        print out
        ver = None
    else:
        # parse output with verpat
        try:
            m = re.search(verpat, out)
            if m:
                ver = m.group(1)
            #else:
            #    print "Didn't find version"
        except Exception as err:
            #print type(err)
            ver = None 
            fwdie("Error: Exception from re.match.  Didn't find version: %s" % err, PF_EXIT_FAILURE)

    return ver


def setupwrapper(inwcl, iwfilename, logfilename, useDB=False):
    """ Create output directories, get files from cache, and other setup work """

    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    # make directory for log file
    logdir = os.path.dirname(logfilename)
    if not os.path.exists(logdir):
        os.makedirs(logdir)

    # make directory for outputwcl
    outputwclfile = inwcl[IW_WRAPSECT]['outputwcl']
    outputwcldir = os.path.dirname(outputwclfile)
    if len(outputwcldir) > 0:
        if not os.path.exists(outputwcldir):
            os.makedirs(outputwcldir)
    else:
        print "0 length directory for outputwcl"

    dbh = None
    if useDB:
        dbh = pfwdb.PFWDB()
        inwcl['dbids'] = {}
        inwcl['wrapperid'] = dbh.insert_wrapper(inwcl, iwfilename)


        # register input wcl file and any list files for this wrapper
        pfw_file_metadata = {}
        pfw_file_metadata['file_1'] = {'filename' : wclutils.getFilename(iwfilename), 
                                       'filetype' : 'wcl'}

        cnt = 1
        if IW_LISTSECT in inwcl:
            for flabel, fdict in inwcl[IW_LISTSECT].items():
                cnt += 1
                pfw_file_metadata['file_%d' % (cnt)] = {'filename': wclutils.getFilename(fdict['fullname']),
                                                        'filetype': 'list'}
    
                
        
        try:
            dbh.ingest_file_metadata(pfw_file_metadata, inwcl['filetype_metadata'])
        except Exception as err:
            print err
            wclutils.write_wcl(pfw_file_metadata)
            raise
    else:
        inwcl['wrapperid'] = -1
        inwcl['dbids'] = {}


    # make directories for output files, cache input files
    fwdebug(3, "PFWRUNJOB_DEBUG", "section loop beg")
    execnamesarr = [inwcl['wrapname']]
    outfiles = {}
    execs = pfwutils.get_exec_sections(inwcl, IW_EXECPREFIX)
    for sect in sorted(execs):
        fwdebug(0, "PFWRUNJOB_DEBUG", "section %s" % sect)
        if 'execname' not in inwcl[sect]:
            print "Missing execname in input wcl.  sect =", sect
            print "inwcl[sect] = ", wclutils.write_wcl(inwcl[sect])
                
                
        execname = inwcl[sect]['execname']
        execnamesarr.append(execname)
        if IW_OUTPUTS in inwcl[sect]:
            for outfile in fwsplit(inwcl[sect][IW_OUTPUTS]):
                outfiles[outfile] = True
                print outfile
                fullnames = pfwutils.get_wcl_value(outfile+'.fullname', inwcl)
                #print "fullnames = ", fullnames
                if '$RNMLST{' in fullnames:
                    m = re.search("\$RNMLST{\${(.+)},(.+)}", fullnames)
                    if m:
                        print "Found rnmlst"
                        print m.group(1)
                    pattern = pfwutils.get_wcl_value(m.group(1), inwcl)
                    print pattern
                else:
                    outfile_names = fwsplit(fullnames)
                    for outfile in outfile_names:
                        outfile_dir = os.path.dirname(outfile)
                        if len(outfile_dir) > 0:
                            if not os.path.exists(outfile_dir):
                                os.makedirs(outfile_dir)
                        else:
                            print "0 length directory for output file:", outfile
        else:
            print "Note: 0 output files (%s) in exec section %s" % (IW_OUTPUTS, sect)

        if IW_EXEC_DEF in inwcl:
            if execname.lower() in inwcl[IW_EXEC_DEF]:    # might be a function or just missing
                if ( 'version_flag' in inwcl[IW_EXEC_DEF][execname.lower()]
                   and 'version_pattern' in inwcl[IW_EXEC_DEF][execname.lower()] ):
                    verflag = inwcl[IW_EXEC_DEF][execname.lower()]['version_flag']
                    verpat = inwcl[IW_EXEC_DEF][execname.lower()]['version_pattern']

                    inwcl[sect]['version'] = getVersion(execname, verflag, verpat)
                    #print "inwcl[sect]['version']", sect, inwcl[sect]['version']

        if useDB: 
            if 'execnum' not in inwcl[sect]:
                result = re.match('%s(\d+)' % IW_EXECPREFIX, sect)
                execnum = result.group(1)
                inwcl[sect]['execnum'] = execnum
                inwcl['dbids'][sect] = dbh.insert_exec(inwcl, sect) 

    if 'wrapinputs' in inwcl and inwcl[PF_WRAPNUM] in inwcl['wrapinputs']:
        files2get = {}
        for infile in inwcl['wrapinputs'][inwcl[PF_WRAPNUM]].values():
            if not os.path.exists(infile) and not infile in outfiles:
                files2get[infile] = True
                infile_dir = os.path.dirname(infile)
                if len(infile_dir) > 0:
                    if not os.path.exists(infile_dir):
                        os.makedirs(infile_dir)
                else:
                    print "0 length directory for input file:", inname
                    

        if len(files2get) > 0:
            if 'cachename' in inwcl and IW_DATA_DEF in inwcl:
                filecache = cache.Cache()
                print "execname =", execname
                print "Calling get_within_job_wrapper with: "
                print "First arg: ", files2get.keys()
                print "Second arg: ", inwcl['cachename']

                problemfiles = filecache.get_within_job_wrapper(files2get.keys(), inwcl['cachename'])
            else:  # depricated
                problemfiles = cache.get_from_cache(files2get.keys())

            if len(problemfiles) != 0:
                print "Error: had problems getting input files from cache"
                print "\t", problemfiles
                return(len(problemfiles))
        
    else:
        print "Note: 0 wrapinputs"

    inwcl['execnames'] = ','.join(execnamesarr)
    fwdebug(3, "PFWRUNJOB_DEBUG", "section loop end")

    fwdebug(3, "PFWRUNJOB_DEBUG", "END")

    return(0)



def runwrapper(wrappercmd, logfilename, wrapperid, execnames, bufsize=5000, useQCF=False):
    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")
    print "wrappercmd = ", wrappercmd
    print "\tlogfilename = ", logfilename
    print "\tuseQCF = ", useQCF

    logfh = open(logfilename, 'w', 0)

    processWrap = subprocess.Popen(wrappercmd.split(),
                                   shell=False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
    if useQCF:
        cmdQCF = "qcf_controller.pl -wrapperInstanceId %s -execnames %s" % (wrapperid, execnames)
        processQCF = subprocess.Popen(cmdQCF.split(),
                                      shell=False,
                                      stdin=subprocess.PIPE,
                                      stderr=subprocess.STDOUT)

    try:
        buf = os.read(processWrap.stdout.fileno(), bufsize)
        while processWrap.poll() == None or len(buf) != 0:
            logfh.write(buf)
            if useQCF:
                processQCF.stdin.write(buf)
            buf = os.read(processWrap.stdout.fileno(), bufsize)

        logfh.close()
        if useQCF:
            processQCF.stdin.close()
            while processQCF.poll() == None:
                time.sleep(1)
            if processQCF.returncode != 0:
                print "QCF returned non-zero exit code"
    except IOError as e:
        print "I/O error({0}): {1}".format(e.errno, e.strerror)
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
                fwdie("Error: Unexpected error: %s" % sys.exc_info()[0], PF_EXIT_FAILURE)
                
    except:
        fwdie("Error: Unexpected error: %s" % sys.exc_info()[0], PF_EXIT_FAILURE)

    if processWrap.returncode != 0:
        print "wrapper returned non-zero exit code"
    else:
        print "wrapper exited with zero exit code"

    fwdebug(3, "PFWRUNJOB_DEBUG", "END")
    return processWrap.returncode



######################################################################
def compose_path(dirpat, inwcl, infdict, fdict):
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
        if newvar in inwcl:
            newval = inwcl[newvar]
        elif newvar in infdict:
            newval = inwcl[newvar]
        else:
            fwdie("Error: Could not find value for %s" % newvar, PF_EXIT_FAILURE)

        fwdebug(6, 'PFWRUNJOB_DEBUG',
              "\twhy req: newvar, newval, type(newval): %s %s %s" % (newvar, newval, type(newval)))
        newval = str(newval)
        if len(parts) > 1:
            prpat = "%%0%dd" % int(parts[1])
            try:
                newval = prpat % int(newval)
            except ValueError as err:
                fwdie("Error: Problem padding value (%s, %s, %s): %s" % (var, newval, prpat, err))
        dirpat = re.sub("(?i)\${%s}" % var, newval, dirpat)
        m = re.search("(?i)\$\{([^}]+)\}", dirpat)

    if count >= maxtries:
        fwdie("Error: Aborting from infinite loop\n. Current string: '%s'" % dirpat, PF_EXIT_FAILURE)
    return dirpat





######################################################################
def copy_output_to_cache(inwcl, fileinfo, exitcode):
    """ If requested, copy output file(s) to cache """

    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    if USE_CACHE not in inwcl:    # default to never
        inwcl[USE_CACHE] = 'never';

    inwcl[USE_CACHE] = inwcl[USE_CACHE].lower()

    usecache = False
    if inwcl[USE_CACHE] == 'never':
        usecache = False
    elif inwcl[USE_CACHE] == 'filespecs':
        usecache = True
    elif inwcl[USE_CACHE] == 'filesuccess':
        usecache = (exitcode == 0)
    elif inwcl[USE_CACHE] == 'filetransfer':
        usecache = True
    else:
        print "Warning: unknown value for %s (%s).  Defaulting it to 'never'" %  (USE_CACHE, inwcl[USE_CACHE])

    fwdebug(0, "PFWRUNJOB_DEBUG", "usecache = %s" % usecache)


    if usecache:
        if DATA_DEF not in inwcl:
            fwdie("Error: %s not specified" % DATA_DEF, PF_EXIT_FAILURE)
        if 'cachename' not in inwcl:
            fwdie("Error: cachename not specified", PF_EXIT_FAILURE)
        cachedict = inwcl[DATA_DEF][inwcl['cachename']]

        putinfo = {}
        for (filename, fdict) in fileinfo.items():
            fwdebug(0, "PFWRUNJOB_DEBUG", "file %s" % fdict['fullname'])
            fwdebug(0, "PFWRUNJOB_DEBUG", "\tsection %s" % fdict['sectname'])
            infdict = inwcl[IW_FILESECT][fdict['sectname']]
            if COPY_CACHE in infdict and convertBool(infdict[COPY_CACHE]):
                putinfo[fdict['fullname']] = infdict['cachepath']
            else:
                print "\tcopycache is false or missing"

        # call cache put function
        fwdebug(0, "PFWRUNJOB_DEBUG", "Calling put_within_job for %s files" % len(putinfo))
        wclutils.write_wcl(putinfo)
        filecache = cache.Cache()
        problemfiles = filecache.put_within_job(putinfo, cachedict)
    else:
        print "usecache is false"
                   

######################################################################
def postwrapper(inwcl, logfile, exitcode, useDB=False):
    fwdebug(3, "PFWRUNJOB_DEBUG", "BEG")

    if not os.path.isfile(logfile):
        logfile = None

    outputwclfile = inwcl[IW_WRAPSECT]['outputwcl']
    outputwcl = None
    if not os.path.isfile(outputwclfile):
        outputwclfile = None
    else:
        outwclfh = open(outputwclfile, 'r')
        outputwcl = wclutils.read_wcl(outwclfh)

    # handle copying output files to cache
    if outputwcl is not None and OW_METASECT in outputwcl and len(outputwcl[OW_METASECT]) > 0:
        # separate metadata needed for PFW from DB metadata tables
        finfo = {}
        for fdict in outputwcl[OW_METASECT].values():
            finfo[fdict['filename']] = { 'sectname': fdict['sectname'],
                                         'fullname': fdict['fullname'],
                                         'filename': fdict['filename'] }
            del fdict['sectname']
            del fdict['fullname']
        #wclutils.write_wcl(finfo)
        copy_output_to_cache(inwcl, finfo, exitcode)

    if useDB:
        dbh = pfwdb.PFWDB()
        dbh.update_wrapper_end(inwcl, outputwclfile, logfile, exitcode)
        if outputwcl is not None:    
            execs = pfwutils.get_exec_sections(outputwcl, OW_EXECPREFIX)
            for sect in execs:
                dbh.update_exec_end(outputwcl[sect], inwcl['dbids'][sect], exitcode)
            if OW_METASECT in outputwcl:
                dbh.ingest_file_metadata(outputwcl[OW_METASECT], inwcl['filetype_metadata'])

            pfw_file_metadata = {}
            pfw_file_metadata['file_1'] = {'filename' : wclutils.getFilename(outputwclfile), 
                                           'filetype' : 'wcl'}
            pfw_file_metadata['file_2'] = {'filename' : wclutils.getFilename(logfile), 
                                           'filetype' : 'log'}
            dbh.ingest_file_metadata(pfw_file_metadata, inwcl['filetype_metadata'])

            if OW_PROVSECT in outputwcl and len(outputwcl[OW_PROVSECT].keys()) > 0:
                dbh.ingest_provenance(outputwcl[OW_PROVSECT], inwcl['dbids'])

    fwdebug(3, "PFWRUNJOB_DEBUG", "END")
    



def runtasks(taskfile, useDB=False, jobwcl={}, useQCF=False):
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
                inwcl = wclutils.read_wcl(wclfh)
            inwcl.update(jobwcl)

            exitcode = setupwrapper(inwcl, wclfile, logfile, useDB)
            if exitcode == 0:
                exitcode = runwrapper(wrappercmd, 
                                      logfile,
                                      inwcl['wrapperid'], 
                                      inwcl['execnames'],
                                      5000,
                                      useQCF)
                postwrapper(inwcl, logfile, exitcode, useDB) 
 
                sys.stdout.flush()
                sys.stderr.flush()
                if exitcode:
                    print "Aborting due to non-zero exit code"
                    return(exitcode)
            else:
                print "Aborting due to problems in setup wrapper"
                return(exitcode)
            line = tasksfh.readline()
    return(0)



def runjob(args): 
    """Run tasks inside single job"""

    useDB = False
    useQCF = False
    wcl = {}

    if args.config:
        with open(args.config, 'r') as wclfh:
            wcl = wclutils.read_wcl(wclfh) 
        if 'usedb' in wcl:
            useDB = convertBool(wcl['usedb'])

        if 'useqcf' in wcl:
            useQCF = convertBool(wcl['useqcf'])

    if useDB:
        if 'des_services' in wcl:
            os.environ['DES_SERVICES'] = wcl['des_services']
        if 'des_db_section' in wcl:
            os.environ['DES_DB_SECTION'] = wcl['des_db_section']

        dbh = pfwdb.PFWDB()
        dbh.insert_job(wcl)

    exitcode = runtasks(args.taskfile[0], useDB, wcl, useQCF)

    if useDB:
        dbh.update_job_end(wcl, exitcode)
        

def parseArgs(argv):
    parser = argparse.ArgumentParser(description='pfwrunjob.py')
    parser.add_argument('--version', action='store_true', default=False)
    parser.add_argument('--config', action='store')
    parser.add_argument('taskfile', nargs=1, action='store')

    args = parser.parse_args()

    if args.version:
        print VERSION
        sys.exit(0)

    return args

if __name__ == '__main__':
    print ' '.join(sys.argv)
    print cache.__file__
    sys.exit(runjob(parseArgs(sys.argv)))
