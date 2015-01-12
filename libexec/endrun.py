#!/usr/bin/env python
# Id:
# Rev::                                  :  # Revision of last commit.
# LastChangedBy::                        :  # Author of last commit.
# LastChangedDate::                      :  # Date of last commit.

import sys
import os

import processingfw.pfwdefs as pfwdefs
import processingfw.pfwdb as pfwdb
import desdbmisc.miscutils as miscutils
import processingfw.pfwconfig as pfwconfig
import filemgmt.archive_transfer_utils as archive_transfer_utils



def endrun(configfile):
    miscutils.fwdebug(0, 'PFWBLOCK_DEBUG', "BEG")

    config = pfwconfig.PfwConfig({'wclfile': configfile})
    os.chdir('../uberctrl')

    retval = pfwdefs.PF_EXIT_SUCCESS

    if pfwdefs.USE_HOME_ARCHIVE_OUTPUT in config and \
       config[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower() == 'run':
        if pfwdefs.OPS_RUN_DIR not in config:
            print "Error:  Cannot find %s in config" % pfwdefs.OPS_RUN_DIR
            print "\tIt is needed for the mass copy of the run back to the home archive at the end of the run"
            return(pfwdefs.PF_EXIT_FAILURE)


        archpath = config.interpolate(config[pfwdefs.OPS_RUN_DIR])
        print "archpath =", archpath

            
        # call archive transfer for target archive to home archive
        # check if using target archive
        target_info = None
        if pfwdefs.USE_TARGET_ARCHIVE_OUTPUT in config and \
           config[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT].lower() != 'never':
            if pfwdefs.TARGET_ARCHIVE in config and \
                config[pfwdefs.TARGET_ARCHIVE] in config['archive']:
                target_info = config['archive'][config[pfwdefs.TARGET_ARCHIVE]]
            else:
                print "Error:  cannot determine info for target archive"
                return(pfwdefs.PF_EXIT_FAILURE)
        else:
            print "Error:  Asked to transfer outputs at end of run, but not using target archive"
            return(pfwdefs.PF_EXIT_FAILURE)

        home_info = None
        print config[pfwdefs.HOME_ARCHIVE]
        if pfwdefs.HOME_ARCHIVE in config and \
            config[pfwdefs.HOME_ARCHIVE] in config['archive']:
            home_info = config['archive'][config[pfwdefs.HOME_ARCHIVE]]

        # call transfer
        archive_transfer_utils.archive_copy_dir(target_info, home_info, config['archive_transfer'], archpath, config)


    if miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]):
        miscutils.fwdebug(0, 'PFWENDRUN_DEBUG', "Calling update_attempt_end: retval = %s" % retval)
        dbh = pfwdb.PFWDB(config['submit_des_services'], config['submit_des_db_section'])
        dbh.end_task(config['task_id']['attempt'], retval, True)
        dbh.commit()
        dbh.close()

    miscutils.fwdebug(0, 'PFWBLOCK_DEBUG', "END - exiting with code %s" % retval)
    return(retval)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: endrun.py configfile"
        sys.exit(pfwdefs.PF_EXIT_FAILURE)

    print ' '.join(sys.argv)    # print command so can run by hand if needed
    sys.stdout.flush()

    sys.exit(endrun(sys.argv[1]))
