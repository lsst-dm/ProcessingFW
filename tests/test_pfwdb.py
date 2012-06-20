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


class PFWDBTest (processingfw.pfwdb.PFWDB):
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
        data = [(k, v ['metadata_table'], v ['filename_pattern'],
                 v ['ops_dir_pattern']) for k, v in MHSource.items ()]
        self.insert_many ('filetype', cols, data)

        # Add a row for each metadata header.

        data = set ()
        for v in MHSource.values ():
            for hdr, col in v ['other_header_names'].items ():
                data.add ((hdr, col [0], 0))

            for hdr, col in v ['derived_header_names'].items ():
                data.add ((hdr, col [0], 1))

        data = [i for i in data]
        cols = ['file_header_name', 'column_name', 'derived']
        self.insert_many ('metadata', cols, data)

        # Add a link between each file type and each of its metadata headers.

        data = []
        for (ft, v) in MHSource.items ():
            for hdr in v ['other_header_names']:
                data.append ((ft, hdr))

            for hdr in v ['derived_header_names']:
                data.append ((ft, hdr))

        cols = ['filetype', 'file_header_name']
        self.insert_many ('required_metadata', cols, data)

    def filetype_get_rows (self, filetype, hdrs):
        "Return the rows in the specified metadata table."

        map = dict ([('id', ('id', 'integer'))] +
                    testMHSource [filetype]['other_header_names'].items () +
                    testMHSource [filetype]['derived_header_names'].items ())
        colStr = ','.join ([map [hdr][0] for hdr in hdrs])
        cursor = self.cursor ()
        table = testMHSource [filetype]['metadata_table']
        stmt = 'SELECT %s FROM %s' % (colStr, table)
        cursor.execute (stmt)
        rows = cursor.fetchall ()
        cursor.close ()
        return rows

    def metadata_header_add (self, header, column, derived, filetype = None):
        "Add a metadata header to database and link to filetype if provided."

        bindStr = self.get_positional_bind_string() 
        cursor  = self.cursor ()
        stmt = ("INSERT INTO metadata (file_header_name, column_name, derived) "
                "VALUES (%s, %s, %s)" % (bindStr, bindStr, bindStr))
        cursor.execute (stmt, (header, column, derived))

        if filetype:
            stmt = ("INSERT INTO required_metadata (filetype, file_header_name)"
                    "VALUES (%s, %s)" % (bindStr, bindStr))
            cursor.execute (stmt, (filetype, header))
        cursor.close ()

    def metadata_header_remove (self, header):
        "Remove a metadata header and any links to filetypes."

        bindStr = self.get_positional_bind_string() 
        cursor  = self.cursor ()

        stmt = ("DELETE FROM required_metadata "
                "WHERE file_header_name = " + bindStr)
        cursor.execute (stmt, (header, ))

        stmt = ("DELETE FROM metadata "
                "WHERE file_header_name = " + bindStr)
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

        bindStr = self.get_positional_bind_string() 
        cursor  = self.cursor ()

        stmt = 'DELETE FROM required_metadata WHERE filetype = ' + bindStr
        cursor.execute (stmt, (filetype, ))

        stmt = 'DELETE FROM filetype WHERE filetype = ' + bindStr
        cursor.execute (stmt, (filetype, ))

    def sequence_create (self, sequence):
        cursor = self.cursor ()
        try:
            cursor.execute ('CREATE SEQUENCE %s' % sequence)
        except Exception as e:
            # Postgres requires that the connection be reset before it can be
            # used again.
            self.rollback ()
            raise
        finally:
            cursor.close ()

    def sequence_drop (self, sequence):
        cursor = self.cursor ()
        try:
            cursor.execute ('DROP SEQUENCE %s' % sequence)
        except Exception as e:
            # Postgres requires that the connection be reset before it can be
            # used again.
            self.rollback ()
            raise
        finally:
            cursor.close ()

    def table_copy_empty (self, dest, src):
        "Create an empty copy of the source table."

        # Note that the copy will not have any constraints, triggers, indexes,
        # etc. except for NOT NULL constraints.  This means that DML tests
        # may succeed while production operation fails, but a portable table
        # copy is much more difficult to create.

        stmt = 'CREATE TABLE %s AS SELECT * FROM %s WHERE 0 = 1' % (dest, src)
        cursor = self.cursor ()
        cursor.execute (stmt)
        cursor.close ()

    def table_create (self, table, columns):
        cursor = self.cursor ()
        try:
            cursor.execute ('CREATE TABLE %s (%s)' % (table, columns))
        except Exception:
            # Postgres requires that the connection be reset before it can be
            # used again.
            self.rollback ()
            raise
        finally:
            cursor.close ()


class TestPFWDB (unittest.TestCase):
    """
    Test a class.

    """

    @classmethod
    def setUpClass (self):
        # Open a connection for use by all tests.  This opens the possibility of
        # tests interfering with one another, but connecting for each test
        # seems a bit excessive.

        if dbType == 'oracle':
            self.testSection = 'db-oracle-unittest'
        elif dbType == 'postgres':
            self.testSection = 'db-postgres-unittest'

        self.dbh = PFWDBTest (section=self.testSection)

        # Map various generic column types to dialect-specific types.

        if dbType == 'oracle':
            self.typeMap = {'bigint'   : 'NUMBER (38)',
                            'smallint' : 'NUMBER (3)',
                            'integer'  : 'INTEGER',
                            'numeric'  : 'NUMBER (8,5)',
                            'float'    : 'BINARY_FLOAT',
                            'double'   : 'BINARY_DOUBLE',
                            'string'   : 'VARCHAR2 (20)'
                           }
        elif dbType == 'postgres':
            self.typeMap = {'bigint'   : 'bigint',
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
            self.dbh.sequence_drop ('location_seq')
        except Exception:
            pass

        self.dbh.sequence_create ('location_seq')
        self.dbh.commit ()

        self.dbh.table_drop ('required_metadata')
        self.dbh.table_drop ('filetype')
        self.dbh.table_drop ('metadata')

        self.dbh.table_copy_empty ('filetype', 'filetype')
        self.dbh.table_copy_empty ('metadata', 'metadata')
        self.dbh.table_copy_empty ('required_metadata', 'required_metadata')

        self.dbh.add_test_metadata_headers (testMHSource)
        self.dbh.commit ()

        # Drop and re-create the metadata tables to which test data will be
        # ingested.

        table = testMHSource ['both']['metadata_table']
        self.dbh.table_drop (table)

        v      = testMHSource ['both']
        cols   = ([('id', 'integer')]                +
                  v ['other_header_names'].values () +
                  v ['derived_header_names'].values ())
        cols   = [(col [0], self.typeMap [col [1]]) for col in cols]
        colStr = ','.join (['%s %s' % col for col in cols])
        self.dbh.table_create (table, colStr)
        self.dbh.commit ()

        table = testMHSource ['UPPERCASE']['metadata_table']
        self.dbh.table_drop (table)

        v      = testMHSource ['UPPERCASE']
        cols   = ([('id', 'integer')]                +
                  v ['other_header_names'].values () +
                  v ['derived_header_names'].values ())
        cols   = [(col [0], self.typeMap [col [1]]) for col in cols]
        colStr = ','.join (['%s %s' % col for col in cols])
        self.dbh.table_create (table, colStr)

        self.dbh.commit()

    @classmethod
    def tearDownClass (self):
        try:
            self.dbh.sequence_drop ('location_seq')
        except Exception:
            pass

        self.dbh.table_drop ('required_metadata')
        self.dbh.table_drop ('filetype')
        self.dbh.table_drop ('metadata')
        self.dbh.table_drop (testMHSource ['both']['metadata_table'])
        self.dbh.table_drop (testMHSource ['UPPERCASE']['metadata_table'])

        self.dbh.commit ()
        self.dbh.close ()

    def setUp (self):
        self.maxDiff = None

    def test_corrupt_filetype (self):
        "Filetypes that differ only in case should result in an exception."

        # Add a "duplicate" filetype.  Note that the add method isn't expecting
        # any of the headers in its input to exist, so be sure there is no
        # overlap there.

        ft = 'NO_DERIVED'
        newFiletype = {ft : {'filename_pattern'    : 'no_derived_filename_pat',
                             'ops_dir_pattern'     : 'no_derived_ops_pat',
                             'metadata_table'      : 'test_no_derived2',
                             'derived_header_names': {},
                             'other_header_names'  : {'casehdr':'case_col'}
                            }
                      }
        self.dbh.add_test_metadata_headers (newFiletype)

        try:
            self.assertRaises (DuplicateDBFiletypeError,
                               self.dbh.get_required_metadata_headers,
                               ft.lower ())
        finally:
            self.dbh.remove_a_filetype (ft)

    def test_get_all_types (self):
        "All variations of filetype-metadata header relationships should work."

        # Even when getting all the filetypes, any filetypes that do not have
        # any required metadata headers should be left out.

        res = self.dbh.get_required_metadata_headers ()
        self.assertEqual (res, testMHOutput)

    def test_get_filetype_metadata_map (self):
        h2c = dict (testMHSource ['both']['derived_header_names'].items () +
                    testMHSource ['both']['other_header_names'].items ())
        d = {'table'     : testMHSource ['both']['metadata_table'],
             'hdr_to_col': {k : v [0] for k, v in h2c.items ()}
            }
        map = self.dbh.get_filetype_metadata_map ('both')
        self.assertEqual (d, map)

    def test_get_filetype_metadata_map_bad (self):
        d = {'table': None, 'hdr_to_col': {}}
        map = self.dbh.get_filetype_metadata_map ('unknown file type')
        self.assertEqual (d, map)

    def test_get_filetype_metadata_table (self):
        table = self.dbh.get_filetype_metadata_table ('both')
        self.assertEqual (testMHSource ['both']['metadata_table'], table)

    def test_get_filetype_metadata_table_bad (self):
        table = self.dbh.get_filetype_metadata_table ('unknown file type')
        self.assertIsNone (table)

    def test_get_neither_type (self):
        """
        Specifying a filetype with no associated headers should work.
        """
        res = self.dbh.get_required_metadata_headers ('neither')
        self.assertEqual (res, {})

    def test_get_nonexistent_type (self):
        res = self.dbh.get_required_metadata_headers ('no such thing')
        self.assertEqual (res, {})

    def test_get_one_type (self):
        res = self.dbh.get_required_metadata_headers ('both')
        headers = {'both' : testMHOutput ['both']}
        self.assertEqual (res, headers)

    def test_get_several_types (self):
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
        filetype = 'both'
        mdIngest, retVal, hdrs, expected_rows = self.get_test_data_both ()

        try:
            try:
                res = self.dbh.metadata_ingest (filetype, mdIngest)
            except Exception:
                self.dbh.rollback ()
                raise

            self.assertEqual (retVal, res)

            try:
                actual_rows = set (self.dbh.filetype_get_rows (filetype, hdrs))
            except Exception:
                self.dbh.rollback ()
                raise

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_filetype_unknown (self):
        ft = 'unknown file type'
        d = {'bad_filetypes' : [ft],
             'bad_file_ids'  : [],
             'missing_hdrs'  : {},
             'extra_hdrs'    : {},
             'duplicate_hdrs': {}
            }

        self.assertEqual (d, self.dbh.metadata_ingest (ft, None))

    def test_metadata_ingest_filetype_lower (self):
        "Ingesting a lowercase version of an uppercase filetype should work."
        filetype = 'UPPERCASE'
        cols = ['DER9', 'HDR9']
        md = {'file1': {'DER9': 11, 'HDR9': 12},
              'file2': {'DER9': 21, 'HDR9': 22}
             }
        mdIngest = self.md2str (md)
        expected_rows = set ()
        for fn in md:
            expected_rows.add (tuple ([md [fn].get (c) for c in cols]))
        d = {'bad_filetypes' : [],
             'bad_file_ids'  : [],
             'missing_hdrs'  : {},
             'extra_hdrs'    : {},
             'duplicate_hdrs': {}
            }

        try:
            upFT = filetype.lower ()
            self.assertEqual (d, self.dbh.metadata_ingest (upFT, mdIngest))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, cols))

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_filetype_upper (self):
        "Ingest with an uppercase version of a lowercase filetype should work."
        filetype = 'both'
        mdIngest, retVal, hdrs, expected_rows = self.get_test_data_both ()

        try:
            upFT = filetype.upper ()
            self.assertEqual (retVal, self.dbh.metadata_ingest (upFT, mdIngest))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, hdrs))

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_corrupt (self):
        "Metadata header rows that differ only by case should cause exception."
        filetype = 'corrupt_hdr'
        # Use base data for "both" filetype since it shouldn't matter
        mdIngest, retVal, hdrs, expected_rows = self.get_test_data_both ()

        self.dbh.metadata_header_add ('DER1', 'der1_col', 1, filetype)

        try:
            self.assertRaises (DuplicateDBHeaderError,
                               self.dbh.metadata_ingest, filetype, mdIngest)
        finally:
            self.dbh.metadata_header_remove ('DER1')
            # Remove the metadata if the exception wasn't raised.
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_corrupt_id (self):
        "An id column in the METADATA should raise an exception."
        filetype = 'id_hdr'
        # Use base data for "both" filetype since it shouldn't matter
        mdIngest, retVal, hdrs, expected_rows = self.get_test_data_both ()

        try:
            self.assertRaises (IdMetadataHeaderError,
                               self.dbh.metadata_ingest, filetype, mdIngest)
        finally:
            # Remove the metadata if the exception wasn't raised.
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_duplicate (self):
        "Ingest metadata with headers that differ only by case should fail."
        filetype = 'both'
        md, retVal, hdrs, expected_rows = self.get_test_data_both ()

        # Add another file that shouldn't be ingested.
        md ['file3'] = {'DER1': '45', 'Der1': '78', 'der1': '12'}
        retVal ['duplicate_hdrs'] = {'file3': [('DER1', 'Der1', 'der1')]}

        try:
            self.assertEqual (retVal, self.dbh.metadata_ingest (filetype, md))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, hdrs))

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_extra (self):
        filetype = 'both'
        md, retVal, hdrs, expected_rows = self.get_test_data_both ()

        # Add some extra headers to input and expected return value.

        md ['file1']['ext1'] = '34'
        md ['file1']['ext2'] = '85'
        md ['file2']['ext3'] = '45'

        retVal ['extra_hdrs'] = {'file1': ['ext1', 'ext2'], 'file2': ['ext3']}

        try:
            self.assertEqual (retVal, self.dbh.metadata_ingest (filetype, md))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, hdrs))

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_id (self):
        "An input id header should be reported as extra and not inserted."
        filetype = 'both'
        md, retVal, hdrs, expected_rows = self.get_test_data_both ()

        # Add an id header to each file in input and output.  Create a set of
        # rows that would match the ingested id columns if the provided id
        # metdata values were actually used.
        idList = [4, 5]
        md ['file1']['id'] = idList [0]
        md ['file2']['id'] = idList [1]
        bad_id_rows = {(id, ) for id in idList}
        retVal ['extra_hdrs'] = {'file1': ['id'], 'file2': ['id']}

        try:
            self.assertEqual (retVal, self.dbh.metadata_ingest (filetype, md))

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
        md = {'file1': {'der9': 11, 'hdr9': 12},
              'file2': {'DER9': 21, 'HDR9': 22}
             }
        mdIngest = self.md2str (md)
        expected_rows = set ()
        for fn in md:
            upKeys = {k.upper (): v for k, v in md [fn].items()}
            expected_rows.add (tuple ([upKeys.get (c) for c in cols]))
        d = {'bad_filetypes' : [],
             'bad_file_ids'  : [],
             'missing_hdrs'  : {},
             'extra_hdrs'    : {},
             'duplicate_hdrs': {}
            }

        try:
            self.assertEqual (d, self.dbh.metadata_ingest (filetype, mdIngest))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, cols))

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_missing (self):
        filetype = 'both'
        x = [('file1', 'der2'), ('file2', 'der2'), ('file2', 'hdr2')]
        md, retVal, hdrs, expected_rows = self.get_test_data_both (exclude = x)
        retVal ['missing_hdrs'] = {'file1': ['der2'], 'file2': ['der2', 'hdr2']}

        try:
            self.assertEqual (retVal, self.dbh.metadata_ingest(filetype, md))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, hdrs))

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def test_metadata_ingest_header_upper (self):
        "Ingesting an uppercase version of a lowercase header name should work."
        filetype = 'both'
        md, retVal, hdrs, expected_rows = self.get_test_data_both ()
        md ['file1'] ['HDR2'] = md ['file1'] ['hdr2']
        md ['file1'] ['HDR3'] = md ['file1'] ['hdr3']
        md ['file2'] ['DER1'] = md ['file2'] ['der1']
        md ['file2'] ['DER2'] = md ['file2'] ['der2']
        del md ['file1'] ['hdr2']
        del md ['file1'] ['hdr3']
        del md ['file2'] ['der1']
        del md ['file2'] ['der2']

        try:
            self.assertEqual (retVal, self.dbh.metadata_ingest (filetype, md))

            actual_rows = set (self.dbh.filetype_get_rows (filetype, hdrs))

            self.assertEqual (expected_rows, actual_rows)
        finally:
            self.dbh.metadata_remove (filetype)

    def md2str (self, md):
        "Return a metadata input structure with all values as strings."

        mdStr = {}
        for file, colValues in md.items ():
            mdStr [file] = {h : str (v) for h, v in colValues.items ()}

        return mdStr

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

        md = {'file1': {'der1': 11,   'der2': 12.3,           'der3': 'str1',
                        'hdr2': 13.5, 'hdr3': Decimal (14.5), 'hdr5': 12345},
              'file2': {'der1': 21,   'der2': 22.3,           'der3': 'str2',
                        'hdr2': 23.5, 'hdr3': Decimal (24.5), 'hdr5': 54321}
             }

        if exclude:
            for file, header in exclude:
                del md [file] [header]

        expected_rows = set ()
        for fn in md:
            expected_rows.add (tuple ([md [fn].get (c) for c in hdrs]))

        mdIngest = self.md2str (md)

        retVal = {'bad_filetypes' : [], 'bad_file_ids' : [],
                  'missing_hdrs'  : {}, 'extra_hdrs'   : {},
                  'duplicate_hdrs': {}}

        return mdIngest, retVal, hdrs, expected_rows

if __name__ == '__main__':
    usage = 'Usage: test.py oracle|postgres [unittest_args...]'

    try:
        dbType = sys.argv [1]
    except IndexError:
        raise Exception (usage) 

    if dbType not in ['oracle', 'postgres']:
        raise Exception (usage) 

    del sys.argv [1]

    unittest.main ()
