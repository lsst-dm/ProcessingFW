#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

import argparse
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwcheck as pfwcheck
from coreutils.miscutils import *

from processingfw.pfwdefs import *

import os
import sys


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run check on given submit wcl')
    parser.add_argument('--verbose', action='store', default=True)
    parser.add_argument('--des_db_section', action='store')
    parser.add_argument('--des_services', action='store')
    parser.add_argument('--expandwcl', action='store', default=True, help='set to False if running on an uberctrl/config.des')
    parser.add_argument('wclfile', action='store')

    args = vars(parser.parse_args())   # convert dict

    args['verbose'] = convertBool(args['verbose'])
    args['usePFWconfig'] = convertBool(args['expandwcl'])
    args['get_db_config'] = convertBool(args['expandwcl'])

    # usePFWconfig and get_db_config set to True because dessubmit does (work only done at submit time)
    #   use_db_in=False in submit wcl overrides get_db_config
    print "Gathering wcl..."
    config = pfwconfig.PfwConfig(args)

    config[ATTNUM] = '0'   # must be string as if read from wcl file
    testcnts = pfwcheck.check(config, '')

    print "\nTest Summary"
    print "\tErrors: %d" % testcnts[0]
    print "\tWarnings: %d" % testcnts[1]
    print "\tItems fixed: %d" % testcnts[2]
