#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import coreutils
import processingfw.pfwdb as pfwdb
from processingfw.pfwutils import *
import re
import sys


# verbose 0 = (TBD)  high level campaign summary
# verbose 1 = individual run summary
# verbose 2 = individual run + latest block info
# verbose 3 = individual run + all block info

# verbose > 1 requires DB access and runs using DB


def print_single_block(blknum, blockinfo, job_byblk, wrap_byjob):
    #print "print_single_block(%s,..." % blknum 
    print blockinfo['name']

    if blknum not in job_byblk:
        print "\tNo jobs for this block"
    else:
        for jobnum,jobdict in job_byblk[blknum].items():
            if jobnum not in wrap_byjob:
                print "\t%04d No wrapper instances"
            #print "wrapnum in job =", wrap_byjob[jobnum].keys()
            maxwrap = max(wrap_byjob[jobnum].keys())
            modname = wrap_byjob[jobnum][maxwrap]['modname']    
            
            jobkeys = ""
            if jobdict['jobkeys'] is not None:
                jobkeys = jobdict['jobkeys']
            
            print "\t%04d %d/%d  %s (%s)" % (jobnum, len(wrap_byjob[jobnum]), jobdict['numexpwrap'], modname, jobkeys),
            if 'endtime' in jobdict and jobdict['endtime'] is not None:
                if jobdict['status'] == 0:
                    print "done"
                else:
                    print "fail %s" % jobdict['status']
            else:
                print ""
                

    

def print_job_info(run):
    """    """
    

    m = re.search("([^_]+)_r([^p]+)p([^_]+)", run)
    if m is None:
        print "Error:  cannot parse run", run
        sys.exit(1)

    unitname = m.group(1)
    reqnum = m.group(2)
    attnum = m.group(3)

    #print "unitname =", unitname
    #print "reqnum =", reqnum
    #print "attnum =", attnum

    dbh = pfwdb.PFWDB()

    # get the run info
    attinfo = dbh.get_attempt_info(reqnum, unitname, attnum)
    if 'endtime' in attinfo and attinfo['endtime'] is not None:
        print "Note:  run has finished with status %s" % attinfo['status'] 

    # get the block info
    blockinfo = dbh.get_block_info(reqnum, unitname, attnum)

    # get job info
    jobinfo = dbh.get_job_info({'reqnum':reqnum, 'unitname':unitname, 'attnum':attnum})
    # index jobinfo by blknum
    job_byblk = index_job_info(jobinfo)

    #print job_byblk.keys()
    #for b in job_byblk.keys():
        #print b, job_byblk[b].keys()


    # get wrapper instance information
    wrapinfo = dbh.get_wrapper_info(reqnum, unitname, attnum)
    wrap_byjob, wrap_bymod = index_wrapper_info(wrapinfo)

    verbose = 3

    if verbose == 2:
        blknum = max(blockinfo.keys())
        print_single_block(blknum,  blockinfo[blknum], job_byblk, wrap_byjob)
    elif verbose == 3:
        for blknum in blockinfo.keys():
            print_single_block(blknum, blockinfo[blknum], job_byblk, wrap_byjob)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: print_job_info run"
        print "     grab run from dessubmit or desstat output"
        sys.exit(1)

    print_job_info(sys.argv[1])
