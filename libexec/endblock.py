#!/usr/bin/env python
# Id:
# Rev::                                  :  # Revision of last commit.
# LastChangedBy::                        :  # Author of last commit.
# LastChangedDate::                      :  # Date of last commit.

import sys
import os

import processingfw.pfwdefs as pfwdefs
import despymisc.miscutils as miscutils
import processingfw.pfwconfig as pfwconfig
from processingfw.runqueries import runqueries
import processingfw.pfwblock as pfwblock
import processingfw.pfwdb as pfwdb
import filemgmt.archive_transfer_utils as archive_transfer_utils



def endblock(configfile):
    miscutils.fwdebug_print("BEG")

    config = pfwconfig.PfwConfig({'wclfile': configfile})
    blkdir = config['block_dir']
    os.chdir(blkdir)

    if pfwdefs.USE_HOME_ARCHIVE_OUTPUT in config and \
        config[pfwdefs.USE_HOME_ARCHIVE_OUTPUT].lower() == 'block':
            
        # check if using target archive
        target_info = None
        if pfwdefs.USE_TARGET_ARCHIVE_OUTPUT in config and \
            config[pfwdefs.USE_TARGET_ARCHIVE_OUTPUT].lower() != 'never':
            print config[pfwdefs.TARGET_ARCHIVE]
            if pfwdefs.TARGET_ARCHIVE in config and \
                config[pfwdefs.TARGET_ARCHIVE] in config['archive']:
                target_info = config['archive'][config[pfwdefs.TARGET_ARCHIVE]]
            else:
                print "Error:  cannot determine info for target archive"
                return(pfwdefs.PF_EXIT_FAILURE)
        else:
            print "Error:  Asked to transfer outputs at end of block, but not using target archive"
            return(pfwdefs.PF_EXIT_FAILURE)

        home_info = None
        print config[pfwdefs.HOME_ARCHIVE]
        if pfwdefs.HOME_ARCHIVE in config and config[pfwdefs.HOME_ARCHIVE] in config['archive']:
            home_info = config['archive'][config[pfwdefs.HOME_ARCHIVE]]

        # get file list of files to transfer
        if pfwdefs.PF_USE_DB_OUT in config and miscutils.convertBool(config[pfwdefs.PF_USE_DB_OUT]):
            dbh = pfwdb.PFWDB()
            filelist = dbh.get_run_filelist(config[pfwdefs.REQNUM], config[pfwdefs.UNITNAME],
                        config[pfwdefs.ATTNUM], config[pfwdefs.PF_BLKNUM], config[pfwdefs.TARGET_ARCHIVE])
        else:
            print "Error:  Asked to transfer outputs at end of block, but not using database.   Currently not supported." 
            return(pfwdefs.PF_EXIT_FAILURE)
            

        # call transfer
        archive_transfer_utils.archive_copy(target_info, home_info, config['archive_transfer'], filelist, config)

    miscutils.fwdebug_print("END - exiting with code %s" % pfwdefs.PF_EXIT_SUCCESS)
    return(pfwdefs.PF_EXIT_SUCCESS)



if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: endblock.py configfile"
        sys.exit(pfwdefs.PF_EXIT_FAILURE)

    print ' '.join(sys.argv)    # print command so can run by hand if needed
    sys.stdout.flush()

    sys.exit(endblock(sys.argv[1]))
