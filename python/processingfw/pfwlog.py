#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

""" Functions that handle a processing framework execution event """

import os
import sys
import time

#######################################################################
def get_timestamp():
    """Create timestamp in a particular format"""
    tstamp = time.strftime("%m/%d/%Y %H:%M:%S", time.localtime())
    return tstamp


#######################################################################
def log_pfw_event(config, block=None, subblock=None, 
                  subblocktype=None, info=None):
    """Write info for a PFW event to a log file"""
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
  
    runsite = config['run_site']
    run = config['submit_run']
    logdir = config['uberctrl_dir']

    dagid = os.getenv('CONDOR_ID')
    if not dagid:
        dagid = 0 

    deslogfh = open("%s/%s.deslog" % (logdir, run), "a", 0)
    deslogfh.write("%s %s %s %s %s %s %s" % (get_timestamp(), dagid, run, 
                   runsite, block, subblocktype, subblock))
    if type(info) is list:
        for col in info: 
            deslogfh.write(",%s" % col)
    else:
        deslogfh.write(",%s" % info)

    deslogfh.write("\n")
    deslogfh.close()

##==============================================================================
##==============================================================================
#def parsePFWlog(run, iwd, *blockinfo):
#    (upperdir, runf, lowerdir) = iwd.partition(run)
#
#    nextinfonum = 0
#
#    # read .deslog
#    pfwlog = "%s/%s/runtime/uberctrl/%s.deslog" % (upperdir, run, run) 
#
#    fh = open(pfwlog, "r") 
#    line = fh.readline()
#    while line:
#        line.repl
#        $Line =~ s/,,/, ,/g
#        my @Info = split /,/, $Line
#        $Date = $Info[0]
#        $DagID = $Info[1]
#        $Run = $Info[2]
#        $RunSite = $Info[3]
#        $Block = $Info[4]
#        $Block =~ s/mngr//; # for backwards compatibility
#
#        if (($Block !~ /submit/i) && ($Block !~ /complete/i) && ($Block !~ /restart/i)) {
#            $SubBlockType = $Info[5]
#            $SubBlock = $Info[6]
#            $Type = $Info[7]
#
#            my $KeyStr = makeKeyStr("", $Run, $RunSite, $Block, $SubBlock, $SubBlockType)
#            if ($Type =~ /pretask/) {
#                $BlockNum{$KeyStr} = $NextInfoNum
#                $InfoNum = $NextInfoNum
#
#                $$BlockInfoArrRef[$InfoNum]{"starttime"} = $Date
#                $$BlockInfoArrRef[$InfoNum]{"subblocktype"} = $SubBlockType
#                $$BlockInfoArrRef[$InfoNum]{"subblock"} = $SubBlock
#                $$BlockInfoArrRef[$InfoNum]{"run"} = $Run
#                $$BlockInfoArrRef[$InfoNum]{"runsite"} = $RunSite
#                $$BlockInfoArrRef[$InfoNum]{"block"} = $Block
#                $$BlockInfoArrRef[$InfoNum]{"parent"} = $DagID
#                $$BlockInfoArrRef[$InfoNum]{"jobstat"} = "PRE"
#                $$BlockInfoArrRef[$InfoNum]{"clusterid"} = ""
#                $$BlockInfoArrRef[$InfoNum]{"jobid"} = ""
#                $$BlockInfoArrRef[$InfoNum]{"exitval"} = PF_EXIT_FAILURE
#                $$BlockInfoArrRef[$InfoNum]{"endtime"} = ""
#
#                $NextInfoNum++
#            }
#            elsif ($Type =~ /cid/) {
#                if (defined($BlockNum{$KeyStr})) {
#                    $InfoNum = $BlockNum{$KeyStr}
#                    $$BlockInfoArrRef[$InfoNum]{"clusterid"} = $Info[8]
#                }
#                else {
#                    print "Error: Could not find matching job $KeyStr for cid line\n'$Line'\n"
#                    exit PF_EXIT_FAILURE
#                }
#            }
#            elsif ($Type =~ /jobid/) {
#                if (defined($BlockNum{$KeyStr})) {
#                    $InfoNum = $BlockNum{$KeyStr}
#                    $$BlockInfoArrRef[$InfoNum]{"jobid"} = $Info[8]
#                }
#                else {
#                    print "Error: Could not find matching job $KeyStr for jobid line\n'$Line'\n"
#                    exit PF_EXIT_FAILURE
#                }
#            }
#            elsif ($Type =~ /posttask/) {
#                if (defined($BlockNum{$KeyStr})) {
#                    $InfoNum = $BlockNum{$KeyStr}
#                    $$BlockInfoArrRef[$InfoNum]{"exitval"} = $Info[8]
#                    $$BlockInfoArrRef[$InfoNum]{"endtime"} = $Date
#                }
#                else {
#                    print "Error: Could not find matching job $KeyStr for posttask line\n"
#                    exit PF_EXIT_FAILURE
#                }
#
#            }
#        }
#    }
#        line = fh.readline()
#    fh.close()
#}
#
#
##==============================================================================
##==============================================================================
#sub makeKeyStr {
#    my $Num = shift
#    my $Run = shift
#    my $RunSite = shift
#    my $Block = shift
#    my $SubBlock = shift
#    my $SubBlockType = shift
#
#    my $Key = ""
#
#    if (defined($Num)) {
#        $Key .= $Num
#    }
#    $Key .= "__"
#    if (defined($Run)) {
#        $Key .= $Run
#    }
#    $Key .= "__"
#    if (defined($RunSite)) {
#        $Key .= $RunSite
#    }
#    $Key .= "__"
#    if (defined($Block)) {
#        $Key .= $Block
#    }
#    $Key .= "__"
#    if (defined($SubBlock)) {
#        $Key .= $SubBlock
#    }
#    $Key .= "__"
#    if (defined($SubBlockType)) {
#        $Key .= $SubBlockType
#    }
#
#    return $Key
#}
#
#
#def logOrchEvent(config, event):
#    print event
#    uberdir = config['uberctrl_dir']
#    fh = open("%s/orchevents.log" % (config['uberctrl_dir']), "a+")
#    fh.write("%s: %s" % (getTimeStamp(), event))
#    fh.close()
#
#
#
##sub diffTimes {
##    my ($Date1,$Date2,$Year1,$Year2) = @_
##    my ($Mon, $MDay, $Year, $Hours, $Min, $Secs)
##    my ($Difference, $Seconds, $Minutes, $Days)
##
##    $Year = undef
##    if ($Date1 =~ /(\d+)\/(\d+)\/(\d+)\s+(\d+):(\d+):(\d+)/) {
##        ($Mon, $MDay, $Year, $Hours, $Min, $Secs) = ($1, $2, $3, $4, $5, $6)
##    }
##    elsif ($Date1 =~ /(\d+)\/(\d+)\s+(\d+):(\d+):(\d+)/) {
##        ($Mon, $MDay, $Hours, $Min, $Secs) = ($1, $2, $3, $4, $5)
##    }
##    if (!defined($Year)) {
##        $Year = $Year1
##    }
##    $EpochDate1 = timelocal($Secs, $Min, $Hours, $MDay, $Mon-1, $Year-1900)
##    
##    $Year = undef
##    if ($Date2 =~ /(\d+)\/(\d+)\/(\d+)\s+(\d+):(\d+):(\d+)/) {
##        ($Mon, $MDay, $Year, $Hours, $Min, $Secs) = ($1, $2, $3, $4, $5, $6)
##    }
##    elsif ($Date2 =~ /(\d+)\/(\d+)\s+(\d+):(\d+):(\d+)/) {
##        ($Mon, $MDay, $Hours, $Min, $Secs) = ($1, $2, $3, $4, $5)
##    }
##    if (!defined($Year)) {
##        $Year = $Year2
##    }
##    $EpochDate2 = timelocal($Secs, $Min, $Hours, $MDay, $Mon-1, $Year-1900)
##
##    $Difference = $Date1 - $Date2
##    return $Difference
##}
##
