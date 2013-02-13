#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

import coreutils.desdbi as desdbi
import intgutils.wclutils as wclutils

from processingfw.pfwdefs import *
import processingfw.pfwlog as pfwlog
import processingfw.pfwutils as pfwutils
import processingfw.pfwdb as pfwdb

###########################################################
def gen_file_list(query, debug = 3):
    """ Return list of files retrieved from the database using given query dict """

#    query['location']['key_vals']['archivesites'] = '[^N]'
#    query['location']['select_fields'] = 'all'
#    query['location']['hash_key'] = 'id'

    if debug:
        print "gen_file_list: calling gen_file_query with", query
    
    dbh = pfwdb.PFWDB()
    results = dbh.gen_file_query(query)

    pfwutils.debug(1, 'PFWFILELIST_DEBUG', "number of files in list from query = %s" % len(results))

    pfwutils.debug(3, 'PFWFILELIST_DEBUG', "list from query = %s" % results)

    return results


###########################################################
def convert_single_files_to_lines(filelist):
    """ Convert single files to dict of lines in prep for output """

    count = 1
    linedict = {'list': {}}

    if type(filelist) is dict and len(files) > 1:
        filelist = filelist.values()
    elif type(filelist) is dict:  # single file
        filelist = [filelist]

    linedict = {'list': {PF_LISTENTRY: {}}}
    for onefile in filelist:
        fname = "file%05d" % (count)
        lname = "line%05d" % (count)
        linedict['list'][PF_LISTENTRY][lname] = {'file': {fname: onefile}}
        count += 1
    return linedict


###########################################################
def output_lines(filename, lines, outtype='xml'):
    """ Writes dataset to file in specified output format """

    if outtype == 'xml':
        output_lines_xml(filename, lines)
    elif outtype == 'wcl':
        output_lines_wcl(filename, lines)
    else:
        raise Exception('Invalid outtype (%s).  Valid outtypes: xml, wcl' % outtype)
        

###########################################################
def output_lines_xml(filename, lines):
    """Writes dataset to file in XML format"""

    xmlfh = open(filename, 'w')
    xmlfh.write("<list>\n")
    for k, line in lines.items():
        xmlfh.write("\t<line>\n")
        for name, file in line.items():
            xmlfh.write("\t\t<file nickname='%s'>\n" % name)
            for key,val in file.items():
                if key.lower() == 'ccd':
                    val = "%02d" % (ccd)
                xmlfh.write("\t\t\t<%s>%s</%s>" % (k,val,k))
            xmlfh.write("\t\t\t<fileid>%s</fileid>\n" % (file['id']))
            xmlfh.write("\t\t</file>\n")
        xmlfh.write("\t</line>\n")
    xmlfh.write("</list>\n")
    xmlfh.close()

###########################################################
def output_lines_wcl(filename, dataset):
    """ Writes dataset to file in WCL format """

    fh = open(filename, "w")
    wclutils.write_wcl(dataset, fh, True, 4)  # print it sorted
    fh.close()
