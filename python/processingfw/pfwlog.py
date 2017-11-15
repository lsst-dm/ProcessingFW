#!/usr/bin/env python

"""Functions that handle a processing framework execution event.
"""

import os
import time


def get_timestamp():
    """Create timestamp in a particular format.
    """
    tstamp = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    return tstamp


def log_pfw_event(config, block=None, subblock=None,
                  subblocktype=None, info=None):
    """Write info for a PFW event to a log file.
    """
    if block:
        block = block.replace('"', '')
    else:
        block = ''

    if subblock:
        subblock = subblock.replace('"', '')
    else:
        subblock = ''

    if subblocktype:
        subblocktype = subblocktype.replace('"', '')
    else:
        subblocktype = ''

    runsite = config.getfull('run_site')
    run = config.getfull('submit_run')
    logdir = config.getfull('uberctrl_dir')

    dagid = os.getenv('CONDOR_ID')
    if not dagid:
        dagid = 0

    deslogfh = open("%s/%s.deslog" % (logdir, run), "a", 0)
    deslogfh.write("%s %s %s %s %s %s %s" % (get_timestamp(), dagid, run,
                                             runsite, block, subblocktype, subblock))
    if isinstance(info, list):
        for col in info:
            deslogfh.write(",%s" % col)
    else:
        deslogfh.write(",%s" % info)

    deslogfh.write("\n")
    deslogfh.close()
