#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import argparse
import re
import sys

#import despymisc.miscutils
import processingfw.pfwdb as pfwdb
import processingfw.pfwutils as pfwutils


# verbose 0 = (TBD)  high level campaign summary
# verbose 1 = individual run summary
# verbose 2 = individual run + latest block info
# verbose 3 = individual run + all block info

# verbose > 1 requires DB access and runs using DB


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

    args = vars(parser.parse_args(argv))   # convert to dict

    if args['attempt_str'] is not None:
        args['reqnum'], args['unitname'], args['attnum'] = parse_attempt_str(args['attempt_str'])
    elif args['reqnum'] is None:
        print "Error:  Must specify attempt_str or r,u,a"
        sys.exit(1)

    return args




def print_single_block(blknum, blockinfo, job_byblk, wrap_byjob):
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
                #print "wrapnum in job =", wrap_byjob[jobnum].keys()
                maxwrap = max(wrap_byjob[jobnum].keys())
                modname = wrap_byjob[jobnum][maxwrap]['modname']    
                if 'wrapkeys' in wrap_byjob[jobnum][maxwrap]:  # 1.1 compat
                    wrapkeys = wrap_byjob[jobnum][maxwrap]['wrapkeys']
            
            jobkeys = ""
            if jobdict['jobkeys'] is not None:
                jobkeys = jobdict['jobkeys']

            expnumwrap = 0
            if 'expect_num_wrap' in jobdict and jobdict['expect_num_wrap'] is not None:
                expnumwrap = jobdict['expect_num_wrap']

            numwraps = 0
            if jobnum in wrap_byjob and wrap_byjob[jobnum] is not None:
                numwraps = len(wrap_byjob[jobnum])
    
            
            print "\t%s %d/%d  %s - %s (jk=%s)" % (pfwutils.pad_jobnum(jobnum), numwraps, expnumwrap, modname, wrapkeys, jobkeys),
            if 'end_time' in jobdict and jobdict['end_time'] is not None:
                if jobdict['status'] == 0:
                    print "done"
                else:
                    print "fail %s" % jobdict['status']
            elif numwraps == expnumwrap and 'end_time' in wrap_byjob[jobnum][maxwrap] and wrap_byjob[jobnum][maxwrap]['end_time'] is not None:
                    print "end job tasks"
            else:
                print ""
                

    

def print_job_info(argv):
    """    """

    args = parse_args(argv)
    unitname = args['unitname']
    reqnum = args['reqnum']
    attnum = args['attnum']

    dbh = pfwdb.PFWDB(args['des_services'], args['section'])

    # get the run info
    attinfo = dbh.get_attempt_info(reqnum, unitname, attnum)
    if attinfo is None:
        print "No DB information about the processing attempt"
        print "(Double check which DB querying vs which DB the attempt used)"
        sys.exit(0)

    if 'endtime' in attinfo and attinfo['endtime'] is not None:
        print "Note:  run has finished with status %s" % attinfo['status'] 

    # get the block info
    blockinfo = dbh.get_block_info(pfw_attempt_id=attinfo['id'])

    # get job info
    jobinfo = dbh.get_job_info({'pfw_attempt_id':attinfo['id']})
    # index jobinfo by blknum
    job_byblk = pfwutils.index_job_info(jobinfo)

    #print job_byblk.keys()
    #for b in job_byblk.keys():
        #print b, job_byblk[b].keys()


    # get wrapper instance information
    wrapinfo = dbh.get_wrapper_info(pfw_attempt_id=attinfo['id'])
    wrap_byjob, wrap_bymod = pfwutils.index_wrapper_info(wrapinfo)

    verbose = 3

    if verbose == 2:
        blknum = max(blockinfo.keys())
        print_single_block(blknum,  blockinfo[blknum], job_byblk, wrap_byjob)
    elif verbose == 3:
        for blknum in blockinfo.keys():
            print_single_block(blknum, blockinfo[blknum], job_byblk, wrap_byjob)


if __name__ == "__main__":
    print_job_info(sys.argv[1:])
