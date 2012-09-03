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

import coreutils

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

        retval = {}
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
