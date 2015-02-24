#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

""" Program executed at beginning of processing attempt """

import sys

import despymisc.miscutils as miscutils
import filemgmt.utils as fmutils
import filemgmt.disk_utils_local as diskutils
import processingfw.pfwdefs as pfwdefs
import processingfw.pfwconfig as pfwconfig

def save_submit_file_info(config, filemgmt, expwcl, fullcfg):
    """ Save meta information and provenance for submit files """
    # get artifact information
    expdict = diskutils.get_single_file_disk_info(expwcl,
                                                  save_md5sum=config['save_md5sum'],
                                                  archive_root=None)
    fulldict = diskutils.get_single_file_disk_info(fullcfg,
                                                   save_md5sum=config['save_md5sum'],
                                                   archive_root=None)
    artifacts = [expdict, fulldict]

    # create metadata for submit wcls
    filemeta = {'file_1': {'filename': expdict['filename'], 'filetype': 'wcl'},
                'file_2': {'filename': fulldict['filename'], 'filetype': 'wcl'}}
    miscutils.fwdebug(6, 'BEGRUN_DEBUG', 'filemeta = %s' % filemeta)

    # create provenance
    prov = {'used': {'exec_1': expdict['filename']},
            'was_generated_by': {'exec_1': fulldict['filename']}}
    miscutils.fwdebug(6, 'BEGRUN_DEBUG', 'prov = %s' % prov)
    miscutils.fwdebug(6, 'BEGRUN_DEBUG', 'task_id = %s' % config['task_id']['attempt'])

    # save file information
    filemgmt.save_file_info(artifacts, filemeta, prov, {'exec_1': config['task_id']['attempt']})



def copy_files_home(config, archive_info, filemgmt, expwcl, fullcfg):
    """ Copy submit files to home archive """

    archdir = '%s/submit' % config.interpolate(config['ops_run_dir'])
    miscutils.fwdebug(6, 'BEGRUN_DEBUG', 'archive rel path = %s' % archdir)

    # copy the files to the home archive
    files2copy = {expwcl: {'src':expwcl, 'dst':'%s/%s' % (archdir, expwcl),
                           'filename': expwcl, 'fullname': '%s/%s' % (archdir, expwcl)},
                  fullcfg: {'src':fullcfg, 'dst':'%s/%s' % (archdir, fullcfg),
                            'filename': fullcfg, 'fullname': '%s/%s' % (archdir, fullcfg)}}

    miscutils.fwdebug(6, 'BEGRUN_DEBUG', 'files2copy = %s' % files2copy)

    # load file mvmt class
    submit_files_mvmt = config['submit_files_mvmt']
    miscutils.fwdebug(6, 'BEGRUN_DEBUG', 'submit_files_mvmt = %s' % submit_files_mvmt)
    filemvmt_class = miscutils.dynamically_load_class(submit_files_mvmt)
    valdict = fmutils.get_config_vals(config['job_file_mvmt'], config,
                                      filemvmt_class.requested_config_vals())
    filemvmt = filemvmt_class(archive_info, None, None, None, valdict)

    results = filemvmt.job2home(files2copy)
    miscutils.fwdebug(6, 'BEGRUN_DEBUG', 'trans results = %s' % results)

    # save info for files that we just copied into archive
    files2register = {}
    problemfiles = {}
    for fname, finfo in results.items():
        if 'err' in finfo:
            problemfiles[fname] = finfo
            print "Warning: Error trying to copy file %s to archive: %s" % (fname, finfo['err'])
        else:
            files2register[fname] = finfo

    # call function to do the register
    miscutils.fwdebug(6, 'BEGRUN_DEBUG', 'files2register = %s' % files2register)
    miscutils.fwdebug(6, 'BEGRUN_DEBUG', 'archive = %s' % archive_info['name'])
    filemgmt.register_file_in_archive(files2register, {'archive': archive_info['name']})



def begrun(argv):
    """ Performs steps executed on submit machine at beginning of processing attempt """
    configfile = argv[0]
    config = pfwconfig.PfwConfig({'wclfile': configfile})

    miscutils.fwdebug(6, 'BEGRUN_DEBUG', 'use_home_archive_output = %s' % \
                                         config[pfwdefs.USE_HOME_ARCHIVE_OUTPUT])

    # if not a dryrun and using a home archive for output
    if (config[pfwdefs.USE_HOME_ARCHIVE_OUTPUT] != 'never' and
            'submit_files_mvmt' in config and
            (pfwdefs.PF_DRYRUN not in config or
             not miscutils.convertBool(config[pfwdefs.PF_DRYRUN]))):

        # the two wcl files to copy to the home archive
        expwcl = config['expwcl']
        fullcfg = config['fullcfg']

        # get home archive info
        home_archive = config['home_archive']
        archive_info = config['archive'][home_archive]

        # load filemgmt class
        filemgmt = None
        try:
            filemgmt_class = miscutils.dynamically_load_class(archive_info['filemgmt'])
            valdict = fmutils.get_config_vals(archive_info, config,
                                              filemgmt_class.requested_config_vals())
            filemgmt = filemgmt_class(config=valdict)
        except:
            value = sys.exc_info()[1]
            msg = "Error: creating filemgmt object %s" % value
            print "ERROR\n%s" % msg
            raise

        save_submit_file_info(config, filemgmt, expwcl, fullcfg)
        copy_files_home(config, archive_info, filemgmt, expwcl, fullcfg)

        filemgmt.commit()


if __name__ == "__main__":
    print ' '.join(sys.argv)  # print command line for debugging
    if len(sys.argv) != 2:
        print 'Usage: begrun.py configfile'
        sys.exit(pfwdefs.PF_EXIT_FAILURE)

    begrun(sys.argv[1:])
