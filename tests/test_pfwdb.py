#!/usr/bin/env python

# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
    Test PFWDB via unittest

    Synopsis:
        test dbtype [unittest_parameters]

    dbtype must be either "oracle" or "postgres".  A DES services file will be
    found using methods defined in DESDM-3.  The file is expected to contain a
    section named according to dbtype:

        oracle      db-oracle-unittest
        postgres    db-postgres-unittest

    The database user thus identified should have permission to create
    sequences and tables within its own schema.

    Any unittest_parameters are passed on to the python unittest module.

    Classes:
        PFWDBTest - Subclass of PFWDB providing additional methods useful for
                     the test cases.

        TestPFWDB - Defines the test cases.

    Developed at: 
    The National Center for Supercomputing Applications (NCSA).
  
    Copyright (C) 2012 Board of Trustees of the University of Illinois. 
    All rights reserved.

"""

__version__ = "$Rev$"

import copy
import sys
import unittest

from decimal import Decimal

import coreutils

import processingfw.pfwdb

from processingfw.errors import DuplicateDBFiletypeError
from processingfw.errors import DuplicateDBHeaderError
from processingfw.errors import IdMetadataHeaderError

dbType = None

# Define test data for populating the FILETYPE, METADATA, and REQUIRED_METADATA
# tables.  Note that the ID headers will be excluded by the query so they are
# invisible to output.
testMHSource = {
'no_derived' : {'filename_pattern'    : 'no_derived_filename_pat',
                'ops_dir_pattern'     : 'no_derived_ops_pat',
                'metadata_table'      : 'test_no_derived',
                'derived_header_names': {},
                'other_header_names'  : {'hdr1': ('hdr1_col', 'string'),
                                         'hdr2': ('hdr2_col', 'float')}
               },
'no_other'   : {'filename_pattern'    : 'no_other_filename_pat',
                'ops_dir_pattern'     : 'no_other_ops_pat',
                'metadata_table'      : 'test_no_other',
                'derived_header_names': {'der1': ('der1_col', 'smallint'),
                                         'der2': ('der2_col', 'double')},
                'other_header_names'  : {}
               },
'no_patterns': {'filename_pattern'    : None,
                'ops_dir_pattern'     : None,
                'metadata_table'      : 'test_no_patterns',
                'derived_header_names': {'der4': ('der4_col', 'float'),
                                         'der5': ('der5_col', 'double')},
                'other_header_names'  : {'hdr1': ('hdr1_col', 'string'),
                                         'hdr4': ('hdr4_col', 'smallint')}
               },
'both'       : {'filename_pattern'    : 'no_other_filename_pat',
                'ops_dir_pattern'     : 'no_other_ops_pat',
                'metadata_table'      : 'test_both',
                'derived_header_names': {'der1': ('der1_col', 'smallint'),
                                         'der2': ('der2_col', 'double'),
                                         'der3': ('der3_col', 'string')},
                'other_header_names'  : {'hdr2': ('hdr2_col', 'float'),
                                         'hdr3': ('hdr3_col', 'numeric'),
                                         'hdr5': ('hdr5_col', 'integer')}
               },
# This type is meant only test_metadata_ingest_header_corrupt().  Dedicating
# the type to that test prevents a good copy from being cached by
# metadata_ingest()
'corrupt_hdr': {'filename_pattern'    : 'no_other_filename_pat',
                'ops_dir_pattern'     : 'no_other_ops_pat',
                'metadata_table'      : 'test_both',
                'derived_header_names': {'der1': ('der1_col', 'smallint'),
                                         'der2': ('der2_col', 'double')},
                'other_header_names'  : {'hdr2': ('hdr2_col', 'float'),
                                         'hdr3': ('hdr3_col', 'numeric')}
               },
'id_hdr':      {'filename_pattern'    : 'no_other_filename_pat',
                'ops_dir_pattern'     : 'no_other_ops_pat',
                'metadata_table'      : 'test_both',
                'derived_header_names': {'der1': ('der1_col', 'smallint'),
                                         'der2': ('der2_col', 'double')},
                'other_header_names'  : {'hdr2': ('hdr2_col', 'float'),
                                         'id':   ('id_col',   'integer')}
               },
'neither'    : {'filename_pattern'    : 'no_other_filename_pat',
                'ops_dir_pattern'     : 'no_other_ops_pat',
                'metadata_table'      : 'test_neither',
                'derived_header_names': {},
                'other_header_names'  : {}
               },
'one_each'   : {'filename_pattern'    : 'no_other_filename_pat',
                'ops_dir_pattern'     : 'no_other_ops_pat',
                'metadata_table'      : 'test_one_each',
                'derived_header_names': {'der3': ('der3_col', 'string')},
                'other_header_names'  : {'hdr2': ('hdr2_col', 'float')}
               },
'UPPERCASE'  : {'filename_pattern'    : 'no_other_filename_pat',
                'ops_dir_pattern'     : 'no_other_ops_pat',
                'metadata_table'      : 'test_uppercase',
                'derived_header_names': {'DER9': ('der9_col', 'integer')},
                'other_header_names'  : {'HDR9': ('hdr9_col', 'integer')}
               }
}

# Derive expected output data from the source.

testMHOutput = copy.deepcopy (testMHSource)

del testMHOutput ['neither'] # This filetype has no headers, so won't be output.

for ft in testMHOutput:
    del testMHOutput [ft]['metadata_table']
    # Change the header to column mappings for each filetype to a sorted list
    # of headers excluding the ID header since that shouldn't be returned, but
    # is currently required to be present for the ingest side of things.
    hdrs = [h for h in testMHOutput [ft]['derived_header_names'] if h != 'ID']
    testMHOutput [ft]['derived_header_names'] = sorted (hdrs)
    hdrs = [h for h in testMHOutput [ft]['other_header_names'] if h != 'ID']
    testMHOutput [ft]['other_header_names'] = sorted (hdrs)


class PFWDBTest (processingfw.pfwdb.PFWDB, coreutils.DBTestMixin):
    """
    This is a convenience class.  It adds a few database-related methods used
    by the test cases.
    """

    def __init__ (self, *args, **kwargs):
        processingfw.pfwdb.PFWDB.__init__ (self, *args, **kwargs)

    def add_test_metadata_headers (self, MHSource):
        "Populate the database tables with the test data."

        # Add a row for each file type.

        cols = ['filetype', 'metadata_table', 'filename_pattern',
                'ops_dir_pattern']
        data = [(key, val ['metadata_table'], val ['filename_pattern'],
                 val ['ops_dir_pattern']) for key, val in MHSource.items ()]
        self.insert_many ('filetype', cols, data)

        # Add a row for each metadata table.

        cols = ['table_name', 'id_column']
        data = {(val ['metadata_table'], 'id') for val in MHSource.values ()}
        data = [row for row in data]
        self.insert_many ('metadata_table', cols, data)

        # Add a row for each metadata header.

        data = set ()   # Use a set to remove duplicates.
        for val in MHSource.values ():
            for hdr, col in val ['other_header_names'].items ():
                data.add ((hdr, col [0], 0, 1, 0))

            for hdr, col in val ['derived_header_names'].items ():
                data.add ((hdr, col [0], 1, 1, 0))

        data = [i for i in data]    # DB API doesn't accept sets
        cols = ['file_header_name', 'column_name', 'derived', 'position',
                'bands_for_coadd']
        self.insert_many ('metadata', cols, data)

        # Add a link between each file type and each of its metadata headers.

        data = []
        for (ftype, val) in MHSource.items ():
            for hdr in val ['other_header_names']:
                data.append ((ftype, hdr))

            for hdr in val ['derived_header_names']:
                data.append ((ftype, hdr))

        cols = ['filetype', 'file_header_name']
        self.insert_many ('required_metadata', cols, data)

    def filetype_get_rows (self, filetype, hdrs):
        "Return the rows in the specified metadata table."

        map = dict ([('id', ('id', 'integer'))] +
                    testMHSource [filetype]['other_header_names'].items () +
                    testMHSource [filetype]['derived_header_names'].items ())
        colstr = ','.join ([map [hdr][0] for hdr in hdrs])
        cursor = self.cursor ()
        table = testMHSource [filetype]['metadata_table']
        stmt = 'SELECT %s FROM %s' % (colstr, table)
        cursor.execute (stmt)
        rows = cursor.fetchall ()
        cursor.close ()
        return rows

    def metadata_header_add (self, header, column, derived, filetype = None):
        "Add a metadata header to database and link to filetype if provided."

        bindstr = self.get_positional_bind_string() 
        cursor  = self.cursor ()
        stmt = ("INSERT INTO metadata (file_header_name, column_name, "
                "                      derived, position, bands_for_coadd) "
                "VALUES (%s, %s, %s, %s, %s)" %
                                (bindstr, bindstr, bindstr, bindstr, bindstr))
        cursor.execute (stmt, (header, column, derived, 1, 0))

        if filetype:
            stmt = ("INSERT INTO required_metadata (filetype, file_header_name)"
                    "VALUES (%s, %s)" % (bindstr, bindstr))
            cursor.execute (stmt, (filetype, header))
        cursor.close ()

    def metadata_header_remove (self, header):
        "Remove a metadata header and any links to filetypes."

        bindstr = self.get_positional_bind_string() 
        cursor  = self.cursor ()

        stmt = ("DELETE FROM required_metadata "
                "WHERE file_header_name = " + bindstr)
        cursor.execute (stmt, (header, ))

        stmt = ("DELETE FROM metadata "
                "WHERE file_header_name = " + bindstr)
        cursor.execute (stmt, (header, ))

        cursor.close ()

    def metadata_remove (self, filetype):
        "Remove metadata associated with a filetype."

        cursor = self.cursor ()
        stmt = 'DELETE FROM ' + testMHSource [filetype]['metadata_table']
        cursor.execute (stmt)
        cursor.close ()

    def remove_a_filetype (self, filetype):
        "Rmove all rows associated with a filetype."

        bindstr = self.get_positional_bind_string() 
        cursor  = self.cursor ()

        stmt = 'DELETE FROM required_metadata WHERE filetype = ' + bindstr
        cursor.execute (stmt, (filetype, ))

        stmt = 'DELETE FROM filetype WHERE filetype = ' + bindstr
        cursor.execute (stmt, (filetype, ))


class TestPFWDB (unittest.TestCase):
    """
    Test a class.

    """

    @classmethod
    def setUpClass (cls):
        # Open a connection for use by all tests.  This opens the possibility of
        # tests interfering with one another, but connecting for each test
        # seems a bit excessive.

        if dbType == 'oracle':
            cls.testSection = 'db-oracle-unittest'
        elif dbType == 'postgres':
            cls.testSection = 'db-postgres-unittest'

        cls.dbh = PFWDBTest (section=cls.testSection)

        # Map various generic column types to dialect-specific types.

        if dbType == 'oracle':
            cls.typeMap = {'bigint'   : 'NUMBER (38)',
                            'smallint' : 'NUMBER (3)',
                            'integer'  : 'INTEGER',
                            'numeric'  : 'NUMBER (8,5)',
                            'float'    : 'BINARY_FLOAT',
                            'double'   : 'BINARY_DOUBLE',
                            'string'   : 'VARCHAR2 (20)'
                           }
        elif dbType == 'postgres':
            cls.typeMap = {'bigint'   : 'bigint',
                            'smallint' : 'smallint',
                            'integer'  : 'integer',
                            'numeric'  : 'numeric (8,5)',
                            'float'    : 'real',
                            'double'   : 'double precision',
                            'string'   : 'varchar (20)'
                           }
        else:
            raise NotImplementedError (
                        'Have no typeMap definition for dbtype: %s.' % dbType)

        # Create test tables, sequences, etc. and populate with test data.

        try:
            cls.dbh.sequence_drop ('location_seq')
        except Exception:
            pass

        cls.dbh.sequence_create ('location_seq')
        cls.dbh.commit ()

        cls.dbh.table_drop ('required_metadata')
        cls.dbh.table_drop ('metadata_table')
        cls.dbh.table_drop ('filetype')
        cls.dbh.table_drop ('metadata')

        cls.dbh.table_copy_empty ('metadata_table', 'metadata_table')
        cls.dbh.table_copy_empty ('filetype', 'filetype')
        cls.dbh.table_copy_empty ('metadata', 'metadata')
        cls.dbh.table_copy_empty ('required_metadata', 'required_metadata')

        cls.dbh.add_test_metadata_headers (testMHSource)
        cls.dbh.commit ()

        # Drop and re-create the metadata tables to which test data will be
        # ingested.

        table = testMHSource ['both']['metadata_table']
        cls.dbh.table_drop (table)

        val    = testMHSource ['both']
        cols   = ([('id', 'integer')]                +
                  val ['other_header_names'].values () +
                  val ['derived_header_names'].values ())
        cols   = [(col [0], cls.typeMap [col [1]]) for col in cols]
        colstr = ','.join (['%s %s' % col for col in cols])
        cls.dbh.table_create (table, colstr)
        cls.dbh.commit ()

        table = testMHSource ['UPPERCASE']['metadata_table']
        cls.dbh.table_drop (table)

        val    = testMHSource ['UPPERCASE']
        cols   = ([('id', 'integer')]                +
                  val ['other_header_names'].values () +
                  val ['derived_header_names'].values ())
        cols   = [(col [0], cls.typeMap [col [1]]) for col in cols]
        colstr = ','.join (['%s %s' % col for col in cols])
        cls.dbh.table_create (table, colstr)

        cls.dbh.commit()

    @classmethod
    def tearDownClass (cls):
        try:
            cls.dbh.sequence_drop ('location_seq')
        except Exception:
            pass

        cls.dbh.table_drop ('required_metadata')
        cls.dbh.table_drop ('metadata_table')
        cls.dbh.table_drop ('filetype')
        cls.dbh.table_drop ('metadata')
        cls.dbh.table_drop (testMHSource ['both']['metadata_table'])
        cls.dbh.table_drop (testMHSource ['UPPERCASE']['metadata_table'])

        cls.dbh.commit ()
        cls.dbh.close ()

    def setUp (self):
        self.maxDiff = None

    def test_corrupt_filetype (self):
        "Filetypes that differ only in case should result in an exception."

        # Add a "duplicate" filetype.  Note that the add method isn't expecting
        # any of the headers in its input to exist, so be sure there is no
        # overlap there.

        ftype = 'NO_DERIVED'
        new_ftype = {ftype : {'filename_pattern'    : 'no_derived_filename_pat',
                              'ops_dir_pattern'     : 'no_derived_ops_pat',
                              'metadata_table'      : 'test_no_derived2',
                              'derived_header_names': {},
                              'other_header_names'  : {'casehdr':'case_col'}
                             }
                    }
        self.dbh.add_test_metadata_headers (new_ftype)

        try:
            self.assertRaises (DuplicateDBFiletypeError,
                               self.dbh.get_required_metadata_headers,
                               ftype.lower ())
        finally:
            self.dbh.remove_a_filetype (ftype)

    def test_get_all_types (self):
        "All variations of filetype-metadata header relationships should work."

        # Even when getting all the filetypes, any filetypes that do not have
        # any required metadata headers should be left out.

        res = self.dbh.get_required_metadata_headers ()
        self.assertEqual (res, testMHOutput)

    def test_get_filetype_metadata_map (self):
        "Attempt to retrieve map for a valid filetype."

        h2c = dict (testMHSource ['both']['derived_header_names'].items () +
                    testMHSource ['both']['other_header_names'].items ())
        d = {'table'     : testMHSource ['both']['metadata_table'],
             'id_column' : 'id',
             'hdr_to_col': {key : val [0] for key, val in h2c.items ()}
            }
        map = self.dbh.get_filetype_metadata_map ('both')
        self.assertEqual (d, map)

    def test_get_filetype_metadata_map_bad (self):
        "Attempt to retrieve map for an unknwon filetype."

        d = {'table': None, 'id_column': None, 'hdr_to_col': {}}
        map = self.dbh.get_filetype_metadata_map ('unknown file type')
        self.assertEqual (d, map)

    def test_get_filetype_metadata_table (self):
        "Attempt to retrieve a table for a valid filetype."

        res = self.dbh.get_filetype_metadata_table ('both')
        self.assertEqual ((testMHSource ['both']['metadata_table'],'id'), res)

    def test_get_filetype_metadata_table_bad (self):
        "Attempt to retrieve table for unknown filetype."

        res = self.dbh.get_filetype_metadata_table ('unknown file type')
        self.assertEqual (res, (None, None))

    def test_get_neither_type (self):
        "Specifying a filetype with no associated headers should work."

        res = self.dbh.get_required_metadata_headers ('neither')
        self.assertEqual (res, {})

    def test_get_nonexistent_type (self):
        "Attempt to retrieve an unknown filetype."

        res = self.dbh.get_required_metadata_headers ('no such thing')
        self.assertEqual (res, {})

    def test_get_one_type (self):
        "Attempt to retrieve a single filetype."

        res = self.dbh.get_required_metadata_headers ('both')
        headers = {'both' : testMHOutput ['both']}
        self.assertEqual (res, headers)

    def test_get_several_types (self):
        "Attempt to retrieve multiple filetypes."

        names = ['no_derived', 'no_other', 'one_each']
        res = self.dbh.get_required_metadata_headers (names)

        headers = {h: testMHOutput [h] for h in names}

        self.assertEqual (res, headers)

    def test_get_type_lower_with_upper (self):
        "Attempt to retrieve an uppercase filetype using lowercase letters."

        res = self.dbh.get_required_metadata_headers ('ONE_EACH')
        headers = {'one_each' : testMHOutput ['one_each']}
        self.assertEqual (res, headers)

    def test_get_type_upper_with_lower (self):
        "Attempt to retrieve an uppercase filetype using lowercase letters."

        res = self.dbh.get_required_metadata_headers ('uppercase')
        headers = {'UPPERCASE' : testMHOutput ['UPPERCASE']}
        self.assertEqual (res, headers)

    def test_get_several_types_with_case (self):
        "A list of types with cases that don't match actual values should work."

        request_names = ['no_derived', 'no_other', 'ONE_EACH', 'uppercase']
        actual_names  = ['no_derived', 'no_other', 'one_each', 'UPPERCASE']

        res = self.dbh.get_required_metadata_headers (request_names)

        headers = {h: testMHOutput [h] for h in actual_names}

        self.assertEqual (res, headers)

    def test_metadata_ingest (self):
        "Ingesting a set of metadata headers should work."

        filetype = 'both'
        md_ingest, retval, hdrs, expected_rows = self.get_test_data_both ()

        try:
            try:
                res = self.dbh.metadata_ingest (filetype, md_ingest)
            except Exception:
                self.dbh.rollback ()
                raise

            self.assertEqual (retval, res)

            try:
                actual_rows = set (self.dbh.filetype_get_rows (filetype, hdrs))
            except Exception:
                self.dbh.rollback ()
                raise

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_filetype_unknown (self):
        "An attempt to ingest an unknown filetype should be reported."

        ftype = 'unknown file type'
        d = {'bad_filetypes' : [ftype],
             'bad_file_ids'  : [],
             'missing_hdrs'  : {},
             'extra_hdrs'    : {},
             'duplicate_hdrs': {}
            }

        self.assertEqual (d, self.dbh.metadata_ingest (ftype, None))

    def test_metadata_ingest_filetype_lower (self):
        "Ingesting a lowercase version of an uppercase filetype should work."

        filetype = 'UPPERCASE'
        cols = ['DER9', 'HDR9']
        meta = {'file1': {'DER9': 11, 'HDR9': 12},
                'file2': {'DER9': 21, 'HDR9': 22}
               }
        md_ingest = self.md2str (meta)
        expected_rows = set ()
        for fn in meta:
            expected_rows.add (tuple ([meta [fn].get (c) for c in cols]))
        d = {'bad_filetypes' : [],
             'bad_file_ids'  : [],
             'missing_hdrs'  : {},
             'extra_hdrs'    : {},
             'duplicate_hdrs': {}
            }

        try:
            upft = filetype.lower ()
            self.assertEqual (d, self.dbh.metadata_ingest (upft, md_ingest))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, cols))

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_filetype_upper (self):
        "Ingest with an uppercase version of a lowercase filetype should work."

        filetype = 'both'
        md_ingest, retval, hdrs, expected_rows = self.get_test_data_both ()

        try:
            upft = filetype.upper ()
            self.assertEqual (retval, self.dbh.metadata_ingest(upft, md_ingest))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, hdrs))

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_corrupt (self):
        "Metadata header rows that differ only by case should cause exception."

        filetype = 'corrupt_hdr'
        # Use base data for "both" filetype since it shouldn't matter
        md_ingest, retval, hdrs, expected_rows = self.get_test_data_both ()

        self.dbh.metadata_header_add ('DER1', 'der1_col', 1, filetype)

        try:
            self.assertRaises (DuplicateDBHeaderError,
                               self.dbh.metadata_ingest, filetype, md_ingest)
        finally:
            self.dbh.metadata_header_remove ('DER1')
            # Remove the metadata if the exception wasn't raised.
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_corrupt_id (self):
        "An id column in the METADATA should raise an exception."

        filetype = 'id_hdr'
        # Use base data for "both" filetype since it shouldn't matter
        md_ingest, retval, hdrs, expected_rows = self.get_test_data_both ()

        try:
            self.assertRaises (IdMetadataHeaderError,
                               self.dbh.metadata_ingest, filetype, md_ingest)
        finally:
            # Remove the metadata if the exception wasn't raised.
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_duplicate (self):
        "Ingest metadata with headers that differ only by case should fail."

        filetype = 'both'
        meta, retval, hdrs, expected_rows = self.get_test_data_both ()

        # Add another file that shouldn't be ingested.
        meta ['file3'] = {'DER1': '45', 'Der1': '78', 'der1': '12'}
        retval ['duplicate_hdrs'] = {'file3': [('DER1', 'Der1', 'der1')]}

        try:
            self.assertEqual (retval, self.dbh.metadata_ingest (filetype, meta))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, hdrs))

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_extra (self):
        "Extra metadata headers should be reported."

        filetype = 'both'
        meta, retval, hdrs, expected_rows = self.get_test_data_both ()

        # Add some extra headers to input and expected return value.

        meta ['file1']['ext1'] = '34'
        meta ['file1']['ext2'] = '85'
        meta ['file2']['ext3'] = '45'

        retval ['extra_hdrs'] = {'file1': ['ext1', 'ext2'], 'file2': ['ext3']}

        try:
            self.assertEqual (retval, self.dbh.metadata_ingest (filetype, meta))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, hdrs))

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_id (self):
        "An input id header should be reported as extra and not inserted."

        filetype = 'both'
        meta, retval, hdrs, expected_rows = self.get_test_data_both ()

        # Add an id header to each file in input and output.  Create a set of
        # rows that would match the ingested id columns if the provided id
        # metdata values were actually used.
        idlist = [4, 5]
        meta ['file1']['id'] = idlist [0]
        meta ['file2']['id'] = idlist [1]
        bad_id_rows = {(id, ) for id in idlist}
        retval ['extra_hdrs'] = {'file1': ['id'], 'file2': ['id']}

        try:
            self.assertEqual (retval, self.dbh.metadata_ingest (filetype, meta))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, hdrs))
            actual_id_rows = set (self.dbh.filetype_get_rows (filetype, ['id']))

            self.assertEqual (expected_rows, actual_rows)
            self.assertNotEqual (bad_id_rows, actual_id_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_lower (self):
        "Ingesting a lowercase version of an uppercase header name should work."

        filetype = 'UPPERCASE'
        cols = ['DER9', 'HDR9']
        meta = {'file1': {'der9': 11, 'hdr9': 12},
                'file2': {'DER9': 21, 'HDR9': 22}
               }
        md_ingest = self.md2str (meta)
        expected_rows = set ()
        for fn in meta:
            upkeys = {key.upper (): val for key, val in meta [fn].items()}
            expected_rows.add (tuple ([upkeys.get (c) for c in cols]))
        d = {'bad_filetypes' : [],
             'bad_file_ids'  : [],
             'missing_hdrs'  : {},
             'extra_hdrs'    : {},
             'duplicate_hdrs': {}
            }

        try:
            self.assertEqual (d, self.dbh.metadata_ingest (filetype, md_ingest))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, cols))

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_missing (self):
        "Missing metadata headers should be reported."

        filetype = 'both'
        x = [('file1', 'der2'), ('file2', 'der2'), ('file2', 'hdr2')]
        meta, retval, hdrs, expected_rows = self.get_test_data_both (exclude=x)
        retval ['missing_hdrs'] = {'file1': ['der2'], 'file2': ['der2', 'hdr2']}

        try:
            self.assertEqual (retval, self.dbh.metadata_ingest(filetype, meta))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, hdrs))

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_upper (self):
        "Ingesting an uppercase version of a lowercase header name should work."

        filetype = 'both'
        meta, retval, hdrs, expected_rows = self.get_test_data_both ()
        meta ['file1'] ['HDR2'] = meta ['file1'] ['hdr2']
        meta ['file1'] ['HDR3'] = meta ['file1'] ['hdr3']
        meta ['file2'] ['DER1'] = meta ['file2'] ['der1']
        meta ['file2'] ['DER2'] = meta ['file2'] ['der2']
        del meta ['file1'] ['hdr2']
        del meta ['file1'] ['hdr3']
        del meta ['file2'] ['der1']
        del meta ['file2'] ['der2']

        try:
            self.assertEqual (retval, self.dbh.metadata_ingest (filetype, meta))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, hdrs))

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def md2str (self, meta):
        "Return a metadata input structure with all values as strings."

        mdstr = {}
        for fname, colvalues in meta.items ():
            mdstr [fname] = {h : str (val) for h, val in colvalues.items ()}

        return mdstr

    def get_test_data_both (self, exclude = None):
        """
        Return base test data for the 'both' filetype.

        Returned tuple contains the following items in order
            - dictionary of metadata for all headers indexed by filename
            - dictionary expected as return value from ingest function
            - list of metadata headers to retrieve after ingest (in the same
              order as in the tuples in the expected rows)
            - set of rows (tuples) expected in the metadata table after the
              ingest (assumes table is empty before ingest)

        The exclude parameter may be a sequence of tuples of the form
        (filename, header) which will be excluded from the returned metadata
        and expected rows, but still included in the columns to retrieve. This
        is useful for testing the omission of some metadata and difficult to
        set up with the normally-returned data.
        """

        hdrs = ['der1', 'der2', 'der3', 'hdr2', 'hdr3', 'hdr5']

        meta = {'file1': {'der1': 11,   'der2': 12.3,           'der3': 'str1',
                          'hdr2': 13.5, 'hdr3': Decimal (14.5), 'hdr5': 12345},
                'file2': {'der1': 21,   'der2': 22.3,           'der3': 'str2',
                          'hdr2': 23.5, 'hdr3': Decimal (24.5), 'hdr5': 54321}
             }

        if exclude:
            for fname, header in exclude:
                del meta [fname] [header]

        expected_rows = set ()
        for fn in meta:
            expected_rows.add (tuple ([meta [fn].get (c) for c in hdrs]))

        md_ingest = self.md2str (meta)

        retval = {'bad_filetypes' : [], 'bad_file_ids' : [],
                  'missing_hdrs'  : {}, 'extra_hdrs'   : {},
                  'duplicate_hdrs': {}}

        return md_ingest, retval, hdrs, expected_rows

if __name__ == '__main__':
    if sys.hexversion < 0x02070000:
        sys.exit (sys.argv [0] + ': Error: Python version >= 2.7 and < 3.0 '
                  'required.') 

    usage = 'Usage: %s oracle|postgres [unittest_args...]' % sys.argv [0]

    try:
        dbType = sys.argv [1]
    except IndexError:
        sys.exit (usage) 

    if dbType not in ['oracle', 'postgres']:
        sys.exit (usage) 

    del sys.argv [1]

    unittest.main ()
