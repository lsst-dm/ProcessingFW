#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

""" Program executed at beginning of processing attempt """

import sys
import os

import coreutils.miscutils as coremisc
import filemgmt.utils as fmutils
import processingfw.pfwdefs as pfwdefs
import processingfw.pfwconfig as pfwconfig


def begrun(argv):
    configfile = argv[0]
    config = pfwconfig.PfwConfig({'wclfile': configfile})

    coremisc.fwdebug(6, 'BEGRUN_DEBUG', 'use_home_archive_output = %s' % config[pfwdefs.USE_HOME_ARCHIVE_OUTPUT])

    if config[pfwdefs.USE_HOME_ARCHIVE_OUTPUT] != 'never':
        # the two wcl files to copy to the home archive
        expwcl = config['expwcl']
        fullcfg = config['fullcfg'] 

        # get home archive info
        home_archive = config['home_archive']
        archive_info = config['archive'][home_archive]

        archdir = '%s/submit' % config.interpolate(config['ops_run_dir'])
        coremisc.fwdebug(6, 'BEGRUN_DEBUG', 'archive rel path = %s' % archdir)

        submit_files_mvmt = config['submit_files_mvmt']
        coremisc.fwdebug(6, 'BEGRUN_DEBUG', 'submit_files_mvmt = %s' % submit_files_mvmt)

        # load filemgmt class
        filemgmt = None
        try:
            filemgmt_class = coremisc.dynamically_load_class(archive_info['filemgmt'])
            valDict = fmutils.get_config_vals(archive_info, config, filemgmt_class.requested_config_vals())
            filemgmt = filemgmt_class(config=valDict)
        except:
            (type, value, traceback) = sys.exc_info()
            msg = "Error: creating filemgmt object %s" % value
            print "ERROR\n%s" % msg
            raise


        # create metadata for submit wcls
        filemeta = {'file_1': {'filename': expwcl, 'filetype': 'wcl'},
                    'file_2': {'filename': fullcfg, 'filetype': 'wcl'}}
        coremisc.fwdebug(6, 'BEGRUN_DEBUG', 'filemeta = %s' % filemeta)
        filemgmt.ingest_file_metadata(filemeta)

        # copy the files to the home archive
        files2copy = {expwcl: {'src':expwcl, 'dst':'%s/%s' % (archdir,expwcl),
                               'filename': expwcl, 'fullname': '%s/%s' % (archdir,expwcl),
                               'filesize': os.path.getsize(expwcl)},
                      fullcfg: {'src':fullcfg, 'dst':'%s/%s' % (archdir,fullcfg),
                               'filename': fullcfg, 'fullname': '%s/%s' % (archdir,fullcfg),
                               'filesize': os.path.getsize(expwcl)}}

        coremisc.fwdebug(6, 'BEGRUN_DEBUG', 'files2copy = %s' % files2copy)
        
        # load file mvmt class
        filemvmt_class = coremisc.dynamically_load_class(submit_files_mvmt)
        valDict = fmutils.get_config_vals(config['job_file_mvmt'], config, filemvmt_class.requested_config_vals())
        filemvmt = filemvmt_class(archive_info, None, None, None, valDict)

        results = filemvmt.job2home(files2copy)
        coremisc.fwdebug(6, 'BEGRUN_DEBUG', 'trans results = %s' % results)

        # save info for files that we just copied into archive
        files2register = {}
        problemfiles = {}
        for f, finfo in results.items():
            if 'err' in finfo:
                problemfiles[f] = finfo
                msg = "Warning: Error trying to copy file %s to archive: %s" % (f, finfo['err'])
                print msg
        else:
            files2register[f] = finfo

        # call function to do the register
        coremisc.fwdebug(6, 'BEGRUN_DEBUG', 'files2register = %s' % files2register)
        coremisc.fwdebug(6, 'BEGRUN_DEBUG', 'archive = %s' % archive_info['name'])
        filemgmt.register_file_in_archive(files2register, {'archive': archive_info['name']})

        # create and save file provenance 
        prov = {'used': {'exec_1': expwcl}, 'was_generated_by': {'exec_1': fullcfg}} 
        coremisc.fwdebug(6, 'BEGRUN_DEBUG', 'task_id = %s' % config['task_id']['attempt'])
        coremisc.fwdebug(6, 'BEGRUN_DEBUG', 'prov = %s' % prov)
        filemgmt.ingest_provenance(prov, {'exec_1': config['task_id']['attempt']})

        filemgmt.commit()


if __name__ == "__main__":
    print ' '.join(sys.argv)  # print command line for debugging
    if len(sys.argv) != 2:
        print 'Usage: begrun.py configfile'
        sys.exit(pfwdefs.PF_EXIT_FAILURE)

    begrun(sys.argv[1:])
