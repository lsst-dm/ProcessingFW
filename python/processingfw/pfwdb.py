# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
    Define a database utility class extending coreutils.DesDbi

    Developed at: 
    The National Center for Supercomputing Applications (NCSA).
  
    Copyright (C) 2012 Board of Trustees of the University of Illinois. 
    All rights reserved.
"""

__version__ = "$Rev$"

import sys
from collections import OrderedDict

import coreutils
from processingfw.pfwutils import debug

from errors import DuplicateDBFiletypeError
from errors import DuplicateDBHeaderError
from errors import IdMetadataHeaderError

class PFWDB (coreutils.DesDbi):
    """
        Extend coreutils.DesDbi to add database access methods

        Add methods to retrieve the metadata headers required for one or more
        filetypes and to ingest metadata associated with those headers.
    """

    def __init__ (self, *args, **kwargs):
        coreutils.DesDbi.__init__ (self, *args, **kwargs)

    def get_filename_patterns(self):
        """
        Return ops_filename_patterns table as dictionary
        """
        curs = self.cursor()
        sql = "select * from ops_filename_patterns"
        curs.execute(sql)
        result = OrderedDict()
        for line in curs:
            result[line[0].lower()] = line[1]

        curs.close()
        return result

    def get_directory_patterns(self):
        """
        Return directory_patterns table as dictionary
        """
        curs = self.cursor()
        sql = "select * from ops_directory_patterns"
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]
        result = OrderedDict()
        for line in curs:
            d = dict(zip(desc, line))
            result[d['name']] = d

        curs.close()
        return result

    def get_sites_info(self):
        """
        Return ops_sites table as dictionary
        """
        curs = self.cursor()
        sql = "select * from ops_sites"
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]
        result = OrderedDict()
        for line in curs:
            d = dict(zip(desc, line))
            result[d['name']] = d

        curs.close()
        debug(3, 'PFWDB_DEBUG', "Query of ops_sites table %s\n" % result)
        return result


    def get_archive_nodes(self):
        """
        Return ops_archive_nodes table as dictionary
        """
        curs = self.cursor()
        sql = "select * from ops_archive_nodes"
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]
        result = OrderedDict()
        for line in curs:
            d = dict(zip(desc, line))
            result[d['name']] = d

        curs.close()
        debug(3, 'PFWDB_DEBUG', "Query of ops_archive_nodes table %s\n" % result)
        return result

    ##### request, unit, attempt #####
    def insert_run(self, wcl):
        """ Insert entries into the pfw_request, pfw_unit, pfw_attempt tables for a single run submission """
        maxtries = 3
        from_dual = self.from_dual()

        # loop to try again, esp. for race conditions
        loopcnt = 1
        done = False
        while not done and loopcnt <= maxtries:
            curs = self.cursor()

            # pfw_request
            debug(3, 'PFWDB_DEBUG', "Inserting to pfw_request table\n")
            reqnum = wcl.search('reqnum', {'interpolate': True})[1]
            project = wcl.search('project', {'interpolate': True})[1]
            jiraid = wcl.search('jira_id', {'interpolate': True})[1]
            pipeline = wcl.search('pipeline', {'interpolate': True})[1]
        
            try:
                sql = "insert into pfw_request (reqnum, project, jira_id, pipeline) select %s, '%s', '%s', '%s' %s where not exists (select null from pfw_request where reqnum=%s)" % (reqnum, project, jiraid, pipeline, from_dual, reqnum)
                debug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)
                curs.execute(sql)
            except Exception as e:
                if loopcnt <= maxtries:
                    debug(3, 'PFWDB_DEBUG', "\t%s\n" % str(e))
                    loopcnt = loopcnt + 1
                    self.rollback()
                    continue
                else:
                    raise e

            # pfw_unit
            debug(3, 'PFWDB_DEBUG', "Inserting to pfw_unit table\n")
            unitname = wcl.search('unitname', {'interpolate': True})[1]
            try:
                curs = self.cursor()
                sql = "insert into pfw_unit (reqnum, unitname) select %s, '%s' %s where not exists (select null from pfw_unit where reqnum=%s and unitname='%s')" % (reqnum, unitname, from_dual, reqnum, unitname)
                debug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)
                curs.execute(sql)
            except Exception as e:
                if loopcnt <= maxtries:
                    debug(3, 'PFWDB_DEBUG', "\t%s\n" % str(e))
                    loopcnt = loopcnt + 1
                    self.rollback()
                    continue
                else:
                    raise e

            # pfw_attempt
            debug(3, 'PFWDB_DEBUG', "Inserting to pfw_attempt table\n")
            operator = wcl.search('operator', {'interpolate': True})[1]

            ## get current max attnum and try next value
            sql = "select max(attnum) from pfw_attempt where reqnum='%s' and unitname = '%s'" % (reqnum, unitname)
            debug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)
            curs.execute(sql)
            maxarr = curs.fetchall()
            if len(maxarr) == 0:
                maxatt = 0
            elif maxarr[0][0] == None:
                maxatt = 0
            else:
                maxatt = int(maxarr[0][0])

            try:
                sql = "insert into pfw_attempt (reqnum, unitname, attnum, operator) select %s, '%s', '%s', '%s' %s where not exists (select null from pfw_attempt where reqnum=%s and unitname='%s' and attnum=%s)" % (reqnum, unitname, maxatt+1, operator, from_dual, reqnum, unitname, maxatt+1)
                debug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)
                curs.execute(sql)
            except Exception as e:
                if loopcnt <= maxtries:
                    debug(3, 'PFWDB_DEBUG', "\t%s\n" % str(e))
                    loopcnt = loopcnt + 1
                    self.rollback()
                    continue
                else:
                    raise e

            wcl['attnum'] = maxatt+1
            done = True

        curs.close()
        self.commit()


    def update_attempt_cid (self, wcl, condorid):
        """ update row in pfw_attempt with condorid """

        updatevals = {}
        updatevals['condorid'] = condorid

        wherevals = {}
        wherevals['reqnum'] = wcl.search('reqnum', {'interpolate': True})[1]
        wherevals['unitname'] = wcl.search('unitname', {'interpolate': True})[1]
        wherevals['attnum'] = wcl.search('attnum', {'interpolate': False})[1]

        self.update_PFW_row ('PFW_ATTEMPT', wherevals, updatevals)


    def update_attempt_beg (self, wcl):
        """ update row in pfw_attempt with end of attempt info """

        updatevals = {}
        updatevals['starttime'] = 'CURRENT_TIMESTAMP'

        wherevals = {}
        wherevals['reqnum'] = wcl.search('reqnum', {'interpolate': True})[1]
        wherevals['unitname'] = wcl.search('unitname', {'interpolate': True})[1]
        wherevals['attnum'] = wcl.search('attnum', {'interpolate': False})[1]
        
        self.update_PFW_row ('PFW_ATTEMPT', wherevals, updatevals)


    def update_attempt_end (self, wcl, exitcode):
        """ update row in pfw_attempt with end of attempt info """

        updatevals = {}
        updatevals['endtime'] = 'CURRENT_TIMESTAMP'
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['reqnum'] = wcl.search('reqnum', {'interpolate': True})[1]
        wherevals['unitname'] = wcl.search('unitname', {'interpolate': True})[1]
        wherevals['attnum'] = wcl.search('attnum', {'interpolate': False})[1]
        
        self.update_PFW_row ('PFW_ATTEMPT', wherevals, updatevals)


    ##### BLOCK #####
    def insert_block (self, wcl):
        """ Insert an entry into the pfw_block table """
        debug(3, 'PFWDB_DEBUG', "Inserting to pfw_block table\n")


        row = {}
        row['reqnum'] = wcl.search('reqnum', {'interpolate': True})[1]
        row['unitname'] = wcl.search('unitname', {'interpolate': True})[1]
        row['attnum'] = wcl.search('attnum', {'interpolate': False})[1]
        row['blknum'] = wcl.search('blknum', {'interpolate': False})[1]
        row['name'] = wcl.search('blockname', {'interpolate': True})[1]
        row['modulelist'] = wcl.search('modulelist', {'interpolate': True})[1]
        row['starttime'] = 'CURRENT_TIMESTAMP'
        self.insert_pfw_row('PFW_BLOCK', row)
        
    def update_block_end (self, wcl, exitcode):
        """ update row in pfw_block with end of block info"""

        updatevals = {}
        updatevals['endtime'] = 'CURRENT_TIMESTAMP'
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['reqnum'] = wcl.search('reqnum', {'interpolate': True})[1]
        wherevals['unitname'] = wcl.search('unitname', {'interpolate': True})[1]
        wherevals['attnum'] = wcl.search('attnum', {'interpolate': False})[1]
        wherevals['blknum'] = wcl['blknum']

        self.update_PFW_row ('PFW_BLOCK', wherevals, updatevals)


    ##### JOB #####
    def insert_job (self, wcl):
        """ Insert an entry into the pfw_job table """
        debug(3, 'PFWDB_DEBUG', "Inserting to pfw_job table\n")

        row = {}
        row['reqnum'] = wcl.search('reqnum', {'interpolate': True})[1]
        row['unitname'] = wcl.search('unitname', {'interpolate': True})[1]
        row['attnum'] = wcl.search('attnum', {'interpolate': True})[1]
        row['blknum'] = wcl.search('blknum', {'interpolate': True})[1]
        row['jobnum'] = wcl.search('jobnum', {'interpolate': True})[1]
        row['starttime'] = 'CURRENT_TIMESTAMP'
        row['numexpwrap'] = wcl.search('numexpwrap')[1]

        self.insert_pfw_row('PFW_JOB', row)


    def update_job_end (self, wcl, jobnum, exitcode):
        """ update row in pfw_job with end of job info"""

        updatevals = {}
        updatevals['endtime'] = 'CURRENT_TIMESTAMP'
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['reqnum'] = wcl.search('reqnum', {'interpolate': True})[1]
        wherevals['unitname'] = wcl.search('unitname', {'interpolate': True})[1]
        wherevals['attnum'] = wcl.search('attnum', {'interpolate': True})[1]
        wherevals['blknum'] = wcl.search('blknum', {'interpolate': True})[1]
        wherevals['jobnum'] = jobnum

        self.update_PFW_row ('PFW_JOB', wherevals, updatevals)



    ##### WRAPPER #####
    def insert_wrapper (self, inputwcl):
        """ insert row into pfw_wrapper """

        row = {}
        row['reqnum'] = inputwcl['reqnum']
        row['unitname'] = inputwcl['unitname']
        row['attnum'] = inputwcl['attnum']
        row['wrapnum'] = inputwcl['wrapnum']
        row['name'] = inputwcl['wrapname']
        row['id'] = self.get_seq_next_value('pfw_wrapper_seq')
        row['blknum'] = inputwcl['blknum']
        row['jobnum'] = inputwcl['jobnum']
        row['starttime'] = 'CURRENT_TIMESTAMP'

        self.insert_pfw_row('PFW_WRAPPER', row)
        return row['id']


    def update_wrapper_end (self, inputwcl, exitcode):
        """ update row in pfw_wrapper with end of wrapper info """

        updatevals = {}
        updatevals['endtime'] = 'CURRENT_TIMESTAMP'
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['id'] = inputwcl['wrapperid']

        self.update_PFW_row ('PFW_WRAPPER', wherevals, updatevals)
        



    def insert_pfw_row (self, pfwtable, row):
        """ Insert a row into a table and return any specified cols """ 
        cols = [] 
        vals = []
        for c,v in row.items():
            cols.append(c)
            vals.append(self.quote(v))

        sql = "insert into %s (%s) values (%s)" % (pfwtable, 
                                                   ','.join(cols), 
                                                   ','.join(vals))
    
        debug(3, 'PFWDB_DEBUG', sql)

        curs = self.cursor()
        curs.execute(sql)
        self.commit()
                                                   

    def quote(self, val):
        retval = ""
        if str(val).upper() == 'CURRENT_TIMESTAMP':
            retval = val
        else:
            retval = "'%s'" % val
        return retval
            
            
    def update_PFW_row (self, pfwtable, wherevals, updatevals):
        """ update a row into pfw_wrapper """

        whclause = []
        for c,v in wherevals.items():
            whclause.append("%s=%s" % (c,self.quote(v)))

        upclause = []
        for c,v in updatevals.items():
            upclause.append("%s=%s" % (c,self.quote(v)))

    
        sql = "update %s set %s where %s" % (pfwtable, 
                                             ','.join(upclause),
                                             ' and '.join(whclause))

        debug(3, 'PFWDB_DEBUG', sql)
        curs = self.cursor()
        curs.execute(sql)
        curs.close()
        self.commit()


    def get_required_metadata_headers (self, filetypes = None):
        """
        Return the metadata headers for the indicated filetype(s).

        The filetypes argument may be None (or omitted) to retrieve headers for
        all types, a string containing a single filetype, or a sequence of
        filetypes.  Filetypes are case insensitive.

        In all (successful) cases, the return value is a dictionary with the
        following structure:
            {<str:filetype>: {'filename_pattern'    : <str:filename pattern>,
                              'ops_dir_pattern'     : <str:dir pattern>,
                              'derived_header_names': [<str:name>...],
                              'other_header_names'  : [<str:name>...]
                             }
            }
        Note that either of the derived_header_names or other_header_names
        lists may be empty, but filetypes that have neither will not be
        retrieved from the database.

        """

        bindstr = self.get_positional_bind_string ()

        if filetypes is None:
            args  = []
            where = ''
        elif type (filetypes) in (str, unicode):
            args  = [ filetypes.lower () ]
            where = 'WHERE LOWER (r.filetype) = ' + bindstr
        else: # Allow any sort of sequence
            args  = [ f.lower () for f in filetypes ]
            s     = ','.join ([bindstr for b in args])
            where = 'WHERE LOWER (r.filetype) IN (' + s + ')'

        # Note that ORDER BY isn't really necessary here, but it stablizes
        # the header name lists so that callers will operate consistently.
        stmt = ("SELECT t.filetype, t.filename_pattern, t.ops_dir_pattern,"
                "       file_header_name, m.derived "
                "FROM   filetype t"
                "       JOIN required_metadata r ON r.filetype = t.filetype"
                "       JOIN metadata m USING (file_header_name) "
                ) + where + " ORDER BY t.filetype, file_header_name"

        cursor = self.cursor ()

        cursor.execute (stmt, args)

        retval = OrderedDict()
        for row in cursor.fetchall ():
            ftype = row [0]
            if ftype not in retval:
                retval [ftype] = {'filename_pattern'    : row [1],
                                  'ops_dir_pattern'     : row [2],
                                  'derived_header_names': [],
                                  'other_header_names'  : []
                                 }

            if row [4] == 1:
                retval [ftype] ['derived_header_names'].append (row [3])
            else:
                retval [ftype] ['other_header_names'].append (row [3])

        cursor.close ()

        # The file_header_name column is case sensitive in the database, but
        # header names are meant to be case insensitive; this can lead to
        # duplicate header names in the database.  In addition, various mis-
        # configuration of the metadata mapping tables could lead to duplicate
        # rows returned from the query above.  Check for this problem.

        for ftype in retval:
            hdrs = {hdr for hdr in retval [ftype]['derived_header_names'] +
                                   retval [ftype]['other_header_names']}
            if len ({hdr.lower () for hdr in hdrs}) != len (hdrs):
                raise DuplicateDBHeaderError ()

        # The filetype column in the filetype table is case sensitive in the
        # database, but this method forces case insensitive matching.  This
        # could lead to multiple filetypes being returned for a single
        # requested filetype.  Check for this.

        if len ({ftype.lower () for ftype in retval}) != len (retval):
            raise DuplicateDBFiletypeError ()

        return retval

    def get_metadata_id_from_filename (self, filename):
        """
        Create a unique identifier for the metadata row for the specified file.

        The current implementation extracts the next value from the
        location_seq sequence in the database; however, a standalone algorithm
        taking the filename as input is expected and will ultimately replace
        this implementation.
        """

        return self.get_seq_next_value ('location_seq')

    def get_filetype_metadata_map (self, filetype):
        """
        Retrieve the metadata to table and column mapping for a filetype.

        The returned dictionary contains two keys:
            table       value is name of the database table for the filetype
            id_column   value is name of id column for the table
            hdr_to_col  value is a dictionary mapping metadata header name to
                        database column name
        """

        tab, idcol = self.get_filetype_metadata_table (filetype)

        fmap = {'table': tab, 'id_column': idcol, 'hdr_to_col': {}}

        cursor = self.cursor ()
        bindstr = self.get_positional_bind_string ()

        stmt = ("SELECT file_header_name, m.column_name "
                "FROM   metadata m "
                "       JOIN required_metadata r USING (file_header_name) "
                "WHERE  LOWER (r.filetype) = " + bindstr)

        cursor.execute (stmt, (filetype.lower (), ))

        for row in cursor:
            fmap ['hdr_to_col'][row [0]] = row [1]

        return fmap

    def get_filetype_metadata_table (self, filetype):
        """
        Retrieve the metadata table name and id column name for the specified
        filetype.
 
        Filetypes are considered case insensitive, but may appear multiple
        times with different case in the database.  This condition is detected
        and reported.  Other mis-configurations of the metadata mapping tables
        may lead to this report as well, however.
        """

        cursor  = self.cursor ()
        bindstr = self.get_positional_bind_string ()

        stmt = ("SELECT f.metadata_table, LOWER (m.id_column) "
                "FROM   filetype f "
                "       JOIN metadata_table m "
                "           ON m.table_name = f.metadata_table "
                "WHERE  LOWER (f.filetype) = " + bindstr)

        try:
            cursor.execute (stmt, (filetype.lower (), ))
            res = cursor.fetchall ()
        finally:
            cursor.close ()

        if len (res) == 1:
            return res [0][0], res [0][1]
        elif len (res) == 0:
            return None, None
        else:
            raise DuplicateDBFiletypeError ()

    def metadata_ingest (self, filetype, metadata_by_filename):
        """
        Insert metadata from files of a particular type.

        The filetype argument is case insensitive.

        The metadata_by_filename argument is a dictionary of metadata indexed
        by source filename.  The filename will be used to generate a primary
        key for the file's metadata row.  The metadata for each file is
        specified as a dictionary indexed by metadata header.  The header names
        are case insensitive.

        The return value is a dictionary identifying certain types of problems
        with the following keys:
            bad_filetypes   list of bad filetypes (at most one).
            bad_file_ids    list of filenames for which ids could not be made
            missing_hdrs    dict of lists of missing headers per filename
            extra_hdrs      dict of lists of extra headers per filename
            duplicate_hdrs  dict of lists of duplicate headers per filename
        The keys are always present, but the values are non-empty only for the
        indicated conditions.

        Headers are considered duplicate if they are different only by case, so
        such duplication can exist in the metadata_by_filename parameter and is
        reported when detected.

        Metadata is not ingested for any files listed in bad_file_ids or
        duplicate_hdrs.  No metadata was ingested if bad_filetypes is not
        empty.
        """

        retval = {'bad_filetypes' : [],
                  'bad_file_ids'  : [],
                  'missing_hdrs'  : {},
                  'extra_hdrs'    : {},
                  'duplicate_hdrs': {}
                 }

        if not hasattr (self, 'md_map_cache'):
            # Create a cache so that information for a file type need by
            # collected from the database only once.
            self.md_map_cache = {}

        if filetype not in self.md_map_cache:
            # Haven't seen the filetype yet; get its map.
            fmap = self.get_filetype_metadata_map (filetype)
            self.md_map_cache [filetype] = fmap

        fmap = self.md_map_cache [filetype]

        if not fmap ['table']:
            # Specified filetype doesn't exist or doesn't have a table; punt
            retval ['bad_filetypes'].append (filetype)
            return retval

        # Using positional bind strings means that the columns and values need
        # to be in the same order, so construct a list of columns and and a
        # list of headers that are in the same order.  Start with "id" since
        # that must be added, but shouldn't be in the database.
        columns  = [fmap ['id_column']]
        hdr_list = ['id']
        for hdr, col in fmap ['hdr_to_col'].items ():
            columns.append (col)
            h = hdr.lower()
            if h == 'id':
                raise IdMetadataHeaderError ()
            hdr_list.append (h)

        # Make a set of expected headers for easy comparison to provided
        # headers.
        expected_hdrs = {h for h in hdr_list}

        if len (expected_hdrs) != len (hdr_list):
            raise DuplicateDBHeaderError ()

        expected_hdrs.discard ('id')

        # Loop through the provided files, adding a row for each.

        rows = []

        for filename, metadata in metadata_by_filename.items ():
            # Construct a copy of the metadata for this filename that uses
            # lowercase keys to implement case insenstive matching.

            mdLow         = {k.lower (): v for k, v in metadata.items ()}
            provided_hdrs = {hdr for hdr in mdLow}

            if len (provided_hdrs) != len (metadata):
                # Construct a list of tuples which identify the duplicated
                # headers.
                lowToGiven = {hdr: [] for hdr in provided_hdrs}
                for hdr in metadata:
                    lowToGiven [hdr.lower ()].append (hdr)

                duphdrs = []
                for val in lowToGiven.values ():
                    if len(val) > 1:
                        duphdrs.append (tuple (sorted (val)))

                retval ['duplicate_hdrs'][filename] = duphdrs
                continue

            # Record any issues with this file.

            extra_hdrs    = provided_hdrs - expected_hdrs
            missing_hdrs  = expected_hdrs - provided_hdrs

            if extra_hdrs:
                retval ['extra_hdrs'][filename] = sorted (list (extra_hdrs))
            if missing_hdrs:
                retval ['missing_hdrs'][filename] = sorted (list (missing_hdrs))

            fid = self.get_metadata_id_from_filename (filename)

            if fid is None:
                retval ['bad_file_ids'].append (filename)
            else:
                # Construct a row for this file and add to the list of rows.

                row = [fid if h == 'id' else mdLow.get (h) for h in hdr_list]

                rows.append (row)

        # If there're any rows, insert them.
        if rows:
            self.insert_many (fmap ['table'], columns, rows)

        return retval
