#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import argparse
import re
import sys

from despymisc import miscutils
from processingfw import pfwdb
from processingfw import pfwutils

######################################################################
def parse_attempt_str(attstr):
    """ Parse attempt string for reqnum, unitname, and attnum """
    amatch = re.search(r"(\S+)_r([^p]+)p([^_]+)", attstr)
    if amatch is None:
        print "Error:  cannot parse attempt string", attstr
        sys.exit(1)

    unitname = amatch.group(1)
    reqnum = amatch.group(2)
    attnum = amatch.group(3)

    return reqnum, unitname, attnum

######################################################################
def parse_args(argv):
    """ Parse command line arguments """
    parser = argparse.ArgumentParser(description='Print task information for a processing attempt')
    parser.add_argument('--des_services', action='store', help='')
    parser.add_argument('--section', '-s', action='store',
                        help='Must be specified if DES_DB_SECTION is not set in environment')
    parser.add_argument('attempt_str', nargs='?', action='store')
    parser.add_argument('-r', '--reqnum', action='store')
    parser.add_argument('-u', '--unitname', action='store')
    parser.add_argument('-a', '--attnum', action='store')
    parser.add_argument('--verbose', action='store_true')

    args = vars(parser.parse_args(argv))   # convert to dict

    if args['attempt_str'] is None:
        if args['reqnum'] is None:
            print "Error:  Must specify attempt_str or r,u,a"
            sys.exit(1)
        else:
            args['attempt_str'] = '%s_r%sp%02d' % (args['unitname'], args['reqnum'], int(args['attnum']))

    args['runs'] = miscutils.fwsplit(args['attempt_str'], ',')
    return args

def print_single_block(blknum, blockinfo, job_byblk, wrap_byjob, verbose=False):
    #print "print_single_block(%s,..." % blknum 
    print blockinfo['name']

    if blknum not in job_byblk:
        print "\tNo jobs for this block"
    else:
        for jobnum,jobdict in job_byblk[blknum].items():
            maxwrap = None
            modname = None
            wrapkeys = ""
            if jobnum in wrap_byjob:
                maxwrap = max(wrap_byjob[jobnum].keys())
            else:
                print "No wrappers found for this job (j=%i), perhapse none have run yet." % (jobnum)
                return
            for wrapnum in wrap_byjob[jobnum].keys():
                # skip completed wrappers unless specifically requested
                if wrapnum != maxwrap and 'end_time' in wrap_byjob[jobnum][wrapnum] and wrap_byjob[jobnum][wrapnum]['end_time'] is not None and not verbose:
                    continue
                modname = wrap_byjob[jobnum][wrapnum]['modname']    
                if 'wrapkeys' in wrap_byjob[jobnum][wrapnum]:  # 1.1 compat
                    wrapkeys = wrap_byjob[jobnum][wrapnum]['wrapkeys']
                    #print "WRAPKEYS", wrapkeys
            
                jobkeys = ""
                if jobdict['jobkeys'] is not None:
                    jobkeys = jobdict['jobkeys']
                #print "JOBKEYS",jobkeys

                expnumwrap = 0
                if 'expect_num_wrap' in jobdict and jobdict['expect_num_wrap'] is not None:
                    expnumwrap = jobdict['expect_num_wrap']
                #print "ENUMW",expnumwrap

                #numwraps = 0
                #if jobnum in wrap_byjob and wrap_byjob[jobnum] is not None:
                #    numwraps = len(wrap_byjob[jobnum])
            
                print "\t%s %d/%d  %s - %s (jk=%s)" % (pfwutils.pad_jobnum(jobdict['jobnum']), wrapnum, expnumwrap, modname, wrapkeys, jobkeys),
                if 'end_time' in jobdict and jobdict['end_time'] is not None:
                    if jobdict['status'] == 0:
                        print "done"
                    else:
                        print "fail %s" % jobdict['status']
                elif wrapnum == expnumwrap and 'end_time' in wrap_byjob[jobnum][maxwrap] and wrap_byjob[jobnum][maxwrap]['end_time'] is not None:
                    print "end job tasks"
                else:
                    print ""

def print_job_info(argv):
    """    """

    args = parse_args(argv)

    dbh = pfwdb.PFWDB(args['des_services'], args['section'])

    # get the run info
    for run in args['runs']:
        print run
        reqnum, unitname, attnum = parse_attempt_str(run)
        attinfo = dbh.get_attempt_info(reqnum, unitname, attnum)
        if attinfo is None:
            print "No DB information about the processing attempt"
            print "(Double check which DB querying vs which DB the attempt used)"
        else:
            if 'endtime' in attinfo and attinfo['endtime'] is not None:
                print "Note:  run has finished with status %s" % attinfo['status'] 

            # get the block info
            blockinfo = dbh.get_block_info(pfw_attempt_id=attinfo['id'])

            # get job info
            jobinfo = dbh.get_job_info({'pfw_attempt_id':attinfo['id']})
            # index jobinfo by blknum
            job_byblk = pfwutils.index_job_info(jobinfo)

            # get wrapper instance information
            wrapinfo = dbh.get_wrapper_info(pfw_attempt_id=attinfo['id'])
            wrap_byjob, wrap_bymod = pfwutils.index_wrapper_info(wrapinfo)

            for blknum in blockinfo.keys():
                print_single_block(blknum, blockinfo[blknum], job_byblk, wrap_byjob, args['verbose'])

if __name__ == "__main__":
    print_job_info(sys.argv[1:])
