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
from processingfw.runqueries import runqueries
import processingfw.pfwwrappers as pfwwrappers
import processingfw.pfwblock as pfwblock
import processingfw.pfwdb as pfwdb
import filemgmt.archive_transfer_utils as archive_transfer_utils



def endblock(configfile):
    fwdebug(0, 'PFWBLOCK_DEBUG', "BEG")

    config = pfwconfig.PfwConfig({'wclfile': configfile})
    os.chdir('../%s' % config['blockname'])

    if USE_HOME_ARCHIVE_OUTPUT in config and config[USE_HOME_ARCHIVE_OUTPUT].lower() == 'block':
        if os.path.exists('potential_outputfiles.list'):
            filelist = []
            with open('potential_outputfiles.list') as fh:
                for line in fh:
                    filelist.append(line.strip())
            
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
            print "Error:  Asked to transfer outputs at end of block, but not using target archive"
            return(PF_EXIT_FAILURE)

        home_info = None
        print config[HOME_ARCHIVE]
        if HOME_ARCHIVE in config and config[HOME_ARCHIVE] in config['archive']:
            home_info = config['archive'][config[HOME_ARCHIVE]]

        # call transfer
        archive_transfer_utils.archive_copy(target_info, home_info, config['archive_transfer'], filelist, config)

    fwdebug(0, 'PFWBLOCK_DEBUG', "END - exiting with code %s" % PF_EXIT_SUCCESS)
    return(PF_EXIT_SUCCESS)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: endblock.py configfile"
        sys.exit(PF_EXIT_FAILURE)

    print ' '.join(sys.argv)    # print command so can run by hand if needed
    sys.stdout.flush()

    sys.exit(endblock(sys.argv[1]))
