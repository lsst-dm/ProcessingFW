#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

import argparse
import sys
import re
import coreutils.desdbi as desdbi
from processingfw.pfwdefs import *
from coreutils.miscutils import *
import processingfw.pfwconfig as pfwconfig
import processingfw.pfwfilelist as pfwfilelist
    
def main(argv):    
    parser = argparse.ArgumentParser(description='genquery.py')
    parser.add_argument('--qoutfile', action='store')
    parser.add_argument('--qouttype', action='store')
    parser.add_argument('--config', action='store', dest='configfile')
    parser.add_argument('--module', action='store', dest='modulename')
    parser.add_argument('--search', action='store', dest='searchname')
    args = parser.parse_args(argv)
    
    if args.modulename is None:
        raise Exception("Error: Must specify module\n")
    
    print args.configfile
    config = pfwconfig.PfwConfig({'wclfile':args.configfile})
    
    if args.modulename not in config[SW_MODULESECT]:
        raise Exception("Error: module '%s' does not exist.\n" % (args.modulename))
    
    module_dict = config[SW_MODULESECT][args.modulename]
    
    if args.searchname is not None:
        if SW_LISTSECT in module_dict and args.searchname in module_dict[SW_LISTSECT]:
            search_dict = module_dict[SW_LISTSECT][args.searchname]
        elif SW_FILESECT in module_dict and args.searchname in module_dict[SW_FILESECT]:
            search_dict = module_dict[SW_FILESECT][args.searchname]
        else:
            raise Exception("Error: Could not find either list or file by name %s in module %s\n" % (args.searchname, args.modulename))
        nickname = args.searchname
    else:
        raise Exception("Error: need to define either list or file or search\n")
    
    fields = fwsplit(search_dict[SW_QUERYFIELDS].lower())
    
    if ('query_run' in config and 'fileclass' in search_dict and 
        'fileclass' in config and search_dict['fileclass'] == config['fileclass']):
        query_run = config['query_run'].lower()
        if query_run == 'current':
            fields.append('run')
        elif query_run == 'allbutfirstcurrent':
            if 'current' not in config:
                raise Exception("Internal Error:  Current object doesn't exist\n")
            elif 'curr_blocknum' not in config['current']:
                raise Exception("Internal Error:  current->curr_blocknum doesn't exist\n")
            else:
                block_num = config['current']['curr_blocknum']
                if block_num > 0:
                    fields.append('run')
    
    query = {}
    qtable = search_dict['query_table']
    for f in fields:
        table = qtable
        if '.' in f:
            table, f = f.split('.')

        if f in search_dict:
            value = search_dict[f]
        elif f in module_dict:
            value = module_dict[f]
        elif f in config:
            value = config[f]
        else:
            raise Exception("Error: blockmain could not find value for query field %s\n" % (f))
    
        value = config.interpolate(value)
        if ',' in value:
            value = fwsplit(value)

        if ':' in value:
            value = fwsplit(value)
    
        if table not in query:
            query[table] = {}

        if 'key_vals' not in query[table]:
            query[table]['key_vals'] = {}
        
        query[table]['key_vals'][f] = value
    
    
    # if specified, insert join into query hash
    if 'join' in search_dict:
        #joins = fwsplit(search_dict['join'].lower())
        #for j in joins:
        #    m = re.search("(\S+)\.(\S+)\s*=\s*(\S+)", j)
        #    if m:
        #        print "1", m.group(1)
        #        print "2", m.group(2)
        #        print "3", m.group(3)
        #        table = m.group(1)
        #        if table not in query:
        #            query[table] = {}
        #        if 'join' not in query[table]:
        #            query[table]['join'] = {}
        #        query[m.group(1)]['join'][m.group(2)] = m.group(3)
        query[table]['join']=search_dict['join']


    query[qtable]['select_fields'] = ['filename']

    # check output fields for fields from other tables.
    if 'output_fields' in search_dict:
        output_fields = fwsplit(search_dict['output_fields'].lower())

        for ofield in output_fields:
            m = re.search("(\S+)\.(\S+)", ofield)
            if m:
                table = m.group(1)
                field = m.group(2)
            else:
                table = qtable
                field = ofield
            if field not in query[table]['select_fields']:
                query[table]['select_fields'].append(field)


    for t in query:
        if 'select_fields' in query[t]:
            query[t]['select_fields'] = ','.join(query[t]['select_fields'])

    print "Calling gen_file_list with the following query\n", query
    files = pfwfilelist.gen_file_list(query)
    
    if len(files) == 0:
        raise Exception("genquery: query returned zero results for %s\nAborting\n" % args.searchname)
    
    
#    ## if asked, parse values from filenames
#    #    set up pattern outside loop
#    parsename = None
#    if 'parsename' in search_dict:
#        parsename = search_dict['parsename']
#    
#        parsevars = []
#        m = re.search('\$\{(\w+)\}', parsename)
#        while m:
#            pvar = m.group(1)
#            parsename = parsename.replace('\$\{'+pvar+'\}/', '\(\\S+\)')
#            parsevars.append(pvar)
#            m = re.search('\$\{(\w+)\}', parsename)
#    
#        for fname, filedict in files.items():
#            vals = re.search(parsename, filedict['filename'])
#            if vals is None:
#                raise Exception("Problems finding pattern '%s' in filename %s\n" % (parsename, filedict['filename']))
#    
#            for var in vals:
#                if not var in filedict or filedict[var] is None or filedict[var] == 0:
#                    if var.lower() == 'ccd':
#                        filedict[var] = "%02d" % val
#                    else:
#                        filedict[var] = val
#                    #print "Saving %s=%s" % (var, filedict[var])
#                else:
#                    print "Var already exists: %s '%s'\n" % (var, filedict[var])
    
    ## output list
    lines = pfwfilelist.convert_single_files_to_lines(files)
    pfwfilelist.output_lines(args.qoutfile, lines, args.qouttype)

    return(0)

if __name__ == "__main__":
    print ' '.join(sys.argv)
    sys.exit(main(sys.argv[1:]))
