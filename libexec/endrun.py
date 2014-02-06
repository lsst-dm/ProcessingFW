#!/usr/bin/env python
# Id:
# Rev::                                  :  # Revision of last commit.
# LastChangedBy::                        :  # Author of last commit.
# LastChangedDate::                      :  # Date of last commit.

import sys
import os

from processingfw.pfwdefs import *
from coreutils.miscutils import *
import processingfw.pfwconfig as pfwconfig
import filemgmt.archive_transfer_utils as archive_transfer_utils



def endrun(configfile):
    fwdebug(0, 'PFWBLOCK_DEBUG', "BEG")

    config = pfwconfig.PfwConfig({'wclfile': configfile})
    os.chdir('../uberctrl')

    if USE_HOME_ARCHIVE_OUTPUT in config and config[USE_HOME_ARCHIVE_OUTPUT].lower() == 'run':
        if OPS_RUN_DIR not in config:
            print "Error:  Cannot find %s in config" % OPS_RUN_DIR
            print "\tIt is needed for the mass copy of the run back to the home archive at the end of the run"
            return(PF_EXIT_FAILURE)


        archpath = config.interpolate(config[OPS_RUN_DIR])
        print "archpath =", archpath

            
        # call archive transfer for target archive to home archive
        # check if using target archive
        target_info = None
        print config[USE_TARGET_ARCHIVE_OUTPUT]
        if USE_TARGET_ARCHIVE_OUTPUT in config and convertBool(config[USE_TARGET_ARCHIVE_OUTPUT]):
            print config[TARGET_ARCHIVE]
            if TARGET_ARCHIVE in config and config[TARGET_ARCHIVE] in config['archive']:
                target_info = config['archive'][config[TARGET_ARCHIVE]]
            else:
                print "Error:  cannot determine info for target archive"
                return(PF_EXIT_FAILURE)
        else:
            print "Error:  Asked to transfer outputs at end of run, but not using target archive"
            return(PF_EXIT_FAILURE)

        home_info = None
        print config[HOME_ARCHIVE]
        if HOME_ARCHIVE in config and config[HOME_ARCHIVE] in config['archive']:
            home_info = config['archive'][config[HOME_ARCHIVE]]

        # call transfer
        archive_transfer_utils.archive_copy_dir(target_info, home_info, config['archive_transfer'], archpath, config)

    fwdebug(0, 'PFWBLOCK_DEBUG', "END - exiting with code %s" % PF_EXIT_SUCCESS)
    return(PF_EXIT_SUCCESS)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: endrun.py configfile"
        sys.exit(PF_EXIT_FAILURE)

    print ' '.join(sys.argv)    # print command so can run by hand if needed
    sys.stdout.flush()

    sys.exit(endrun(sys.argv[1]))
