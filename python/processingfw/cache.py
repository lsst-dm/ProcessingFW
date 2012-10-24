#!/usr/bin/env python

""" Dummy cache routine for early PFW milestones """

##import shutil
import os
import sys

def get_from_cache(filelist):
    """ Dummy get routine:   assumes cache is same directory as runtime """
    probfiles = []
    #cacheroot = "/Users/mgower/cache"
    cacheroot = "/work/devel/mgower/cache"
    for f in filelist:
        print "Getting file", f
        fdir = os.path.dirname(f)
        if len(fdir) > 0 and not os.path.exists(fdir):
            os.makedirs(fdir)
        cfile = "%s/%s" % (cacheroot, f)
        if os.path.exists(cfile):
            os.symlink(cfile, f)
            #shutil.copy2(cfile, f)
        else:
            print "Error: Could not find %s in cache (%s)" % (f ,cfile)
            probfiles.append(f)
    return probfiles


def main(argv):
    # for now, assume single file to get from cache if called on command line
    if len(argv) != 1:
        print "Usage: cache.py <runtime_path/filename>"
        exit(1)

    return len(get_from_cache([argv[0]]))
    

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
