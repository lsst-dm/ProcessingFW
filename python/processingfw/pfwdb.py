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

import os
import sys
import socket
from collections import OrderedDict

import coreutils
from processingfw.pfwdefs import *
from processingfw.fwutils import *

from errors import DuplicateDBFiletypeError
from errors import DuplicateDBHeaderError
from errors import IdMetadataHeaderError
from errors import FileMetadataIngestError
from errors import RequiredMetadataMissingError
from errors import DBMetadataNotFoundError

class PFWDB (coreutils.DesDbi):
    """
        Extend coreutils.DesDbi to add database access methods

        Add methods to retrieve the metadata headers required for one or more
        filetypes and to ingest metadata associated with those headers.
    """

    def __init__ (self, *args, **kwargs):
        fwdebug(3, 'PFWDB_DEBUG', args)
        try:
            coreutils.DesDbi.__init__ (self, *args, **kwargs)
        except Exception as err:
            fwdie("Error: problem connecting to database: %s\n\tCheck desservices file and environment variables" % err, PF_EXIT_FAILURE)
            

    def get_database_defaults(self):
        """ Grab default configuration information stored in database """

        result = OrderedDict()
        
        result['archive'] = self.get_database_table('OPS_ARCHIVE_NODE', 'NAME') 
        result[DIRPATSECT] = self.get_database_table('OPS_DIRECTORY_PATTERN', 'NAME')
        result[SW_FILEPATSECT] = self.get_filename_pattern()
        result['site'] = self.get_database_table('OPS_SITE', 'NAME')
        result['filetype_metadata'] = self.get_all_filetype_metadata()
        result[SW_EXEC_DEF] = self.get_database_table('OPS_EXEC_DEF', 'NAME')
        result[DATA_DEF] = self.get_database_table('OPS_DATA_DEFS', 'NAME')
        result['file_header'] = self.get_database_table('OPS_FILE_HEADER', 'NAME')

        return result

    def get_database_table(self, tname, tkey):
        sql = "select * from %s" % tname
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        result = OrderedDict()
        for line in curs:
            d = dict(zip(desc, line))
            result[d[tkey.lower()].lower()] = d

        curs.close()
        return result

    def get_filename_pattern(self):
        sql = "select * from OPS_FILENAME_PATTERN"
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        result = OrderedDict()
        for line in curs:
            d = dict(zip(desc, line))
            result[d['name'].lower()] = d['pattern']

        curs.close()
        return result

    def get_filetype_metadata(self):
        sql = "select * from ops_filetype_metadata"
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        result = OrderedDict()
        for line in curs:
            d = dict(zip(desc, line))
            filetype = d['filetype'].lower()
            headername = d['file_header_name'].lower()
            if filetype not in result:
                result[filetype] = OrderedDict()
            if headername not in result[filetype]:
                result[filetype][headername] = d
            else:
                raise Exception("Found duplicate row in filetype_metadata (%s, %s)" % (filetype, headername))

        curs.close()
        return result
    
        
    def get_metadata(self):
        sql = "select * from ops_metadata"
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        result = OrderedDict()
        for line in curs:
            d = dict(zip(desc, line))
            headername = d['file_header_name'].lower()
            columnname = d['column_name'].lower()
            if headername not in result:
                result[headername] = OrderedDict()
            if columnname not in result[headername]:
                result[headername][columnname] = d
            else:
                raise Exception("Found duplicate row in metadata (%s, %s)" % (headername, columnname))

        curs.close()
        return result
    
    def get_all_filetype_metadata(self):
        """
        Gets a dictionary of dictionaries or string=value pairs representing
        data from the OPS_METADATA, OPS_FILETYPE, and OPS_FILETYPE_METADATA tables.
        This is intended to provide a complete set of filetype metadata required
        during a run.
        Note that the returned dictionary is nested based on the order of the
        columns in the select clause.  Values in columns contained in the
        "collections" list will be turned into dictionaries keyed by the value,
        while the remaining columns will become "column_name=value" elements
        of the parent dictionary.  Thus the sql query and collections list can be
        altered without changing the rest of the code.
        """
        sql = """select f.filetype,f.metadata_table,fm.status,m.derived,
                    fm.file_header_name,m.position,m.column_name
                from OPS_METADATA m, OPS_FILETYPE f, OPS_FILETYPE_METADATA fm 
                where m.file_header_name=fm.file_header_name 
                    and f.filetype=fm.filetype 
                order by 1,2,3,4,5,6 """
        collections = ['filetype','status','derived']
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]
        result = OrderedDict()

        for row in curs:
            ptr = result
            for col, value in enumerate(row):
                normvalue = str(value).lower()
                if col >= (len(row)-3):
                    if normvalue not in ptr:
                        ptr[normvalue] = str(row[col+2]).lower()
                    else:
                        ptr[normvalue] += "," + str(row[col+2]).lower()
                    break
                if normvalue not in ptr:
                    if desc[col] in collections:
                        ptr[normvalue] = OrderedDict()
                    else:
                        ptr[desc[col]] = normvalue
                if desc[col] in collections:
                    ptr = ptr[normvalue]
        curs.close()
        return result

    ##### request, unit, attempt #####
    def insert_run(self, config):
        """ Insert entries into the pfw_request, pfw_unit, pfw_attempt tables for a single run submission """
        maxtries = 3
        from_dual = self.from_dual()

        # loop to try again, esp. for race conditions
        loopcnt = 1
        done = False
        while not done and loopcnt <= maxtries:
            curs = self.cursor()

            # pfw_request
            fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_request table\n")
            reqnum = config.search(REQNUM, {'interpolate': True})[1]
            project = config.search('project', {'interpolate': True})[1]
            jiraid = config.search('jira_id', {'interpolate': True})[1]
            pipeline = config.search('pipeline', {'interpolate': True})[1]
        
            try:
                sql = "insert into pfw_request (reqnum, project, jira_id, pipeline) select %s, '%s', '%s', '%s' %s where not exists (select null from pfw_request where reqnum=%s)" % (reqnum, project, jiraid, pipeline, from_dual, reqnum)
                fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)
                curs.execute(sql)
            except Exception as e:
                if loopcnt <= maxtries:
                    fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % str(e))
                    loopcnt = loopcnt + 1
                    self.rollback()
                    continue
                else:
                    raise e

            # pfw_unit
            fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_unit table\n")
            unitname = config.search(UNITNAME, {'interpolate': True})[1]
            try:
                curs = self.cursor()
                sql = "insert into pfw_unit (reqnum, unitname) select %s, '%s' %s where not exists (select null from pfw_unit where reqnum=%s and unitname='%s')" % (reqnum, unitname, from_dual, reqnum, unitname)
                fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)
                curs.execute(sql)
            except Exception as e:
                if loopcnt <= maxtries:
                    fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % str(e))
                    loopcnt = loopcnt + 1
                    self.rollback()
                    continue
                else:
                    raise e

            # pfw_attempt
            fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_attempt table\n")
            operator = config.search('operator', {'interpolate': True})[1]

            ## get current max attnum and try next value
            sql = "select max(attnum) from pfw_attempt where reqnum='%s' and unitname = '%s'" % (reqnum, unitname)
            fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)
            curs.execute(sql)
            maxarr = curs.fetchall()
            if len(maxarr) == 0:
                maxatt = 0
            elif maxarr[0][0] == None:
                maxatt = 0
            else:
                maxatt = int(maxarr[0][0])

            numexpblk = len(fwsplit(config[SW_BLOCKLIST]))

            try:
                sql = "insert into pfw_attempt (reqnum, unitname, attnum, operator, submittime, numexpblk) select %s, '%s', '%s', '%s', %s, %s %s where not exists (select null from pfw_attempt where reqnum=%s and unitname='%s' and attnum=%s)" % (reqnum, unitname, maxatt+1, operator, self.get_current_timestamp_str(), numexpblk, from_dual, reqnum, unitname, maxatt+1)
                fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)
                curs.execute(sql)
            except Exception as e:
                if loopcnt <= maxtries:
                    fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % str(e))
                    loopcnt = loopcnt + 1
                    self.rollback()
                    continue
                else:
                    raise e

            config[ATTNUM] = maxatt+1
            done = True

        if not done:
            raise Exception("Exceeded max tries for inserting into pfw_attempt table")
        curs.close()
        self.commit()


    def update_attempt_cid (self, config, condorid):
        """ update row in pfw_attempt with condorid """

        updatevals = {}
        updatevals['condorid'] = condorid

        wherevals = {}
        wherevals['reqnum'] = self.quote(config.search(REQNUM, {'interpolate': True})[1])
        wherevals['unitname'] = self.quote(config.search(UNITNAME, {'interpolate': True})[1])
        wherevals['attnum'] = self.quote(config.search(ATTNUM, {'interpolate': False})[1])

        self.update_PFW_row ('PFW_ATTEMPT', wherevals, updatevals)


    def update_attempt_beg (self, config):
        """ update row in pfw_attempt with end of attempt info """

        updatevals = {}
        #updatevals['starttime'] = 'CURRENT_TIMESTAMP'
        updatevals['starttime'] = self.get_current_timestamp_str()

        wherevals = {}
        wherevals['reqnum'] = self.quote(config.search(REQNUM, {'interpolate': True})[1])
        wherevals['unitname'] = self.quote(config.search(UNITNAME, {'interpolate': True})[1])
        wherevals['attnum'] = self.quote(config.search(ATTNUM, {'interpolate': False})[1])
        
        self.update_PFW_row ('PFW_ATTEMPT', wherevals, updatevals)


    def update_attempt_end (self, reqnum, unitname, attnum, exitcode):
        """ update row in pfw_attempt with end of attempt info """

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = self.quote(exitcode)

        wherevals = {}
        wherevals['reqnum'] = self.quote(reqnum)
        wherevals['unitname'] = self.quote(unitname)
        wherevals['attnum'] = self.quote(attnum)
        
        fwdebug(0, 'PFWDB_DEBUG', "%s %s" % (wherevals, updatevals))
        self.update_PFW_row ('PFW_ATTEMPT', wherevals, updatevals)


    ##### BLOCK #####
    def insert_block (self, config):
        """ Insert an entry into the pfw_block table """
        fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_block table\n")

        blknum = config.search(PF_BLKNUM, {'interpolate': False})[1]
        if blknum == '1':  # attempt is starting
            updatevals = {}
            updatevals['starttime'] = self.get_current_timestamp_str()

            wherevals = {}
            wherevals['reqnum'] = self.quote(config.search(REQNUM, {'interpolate': True})[1])
            wherevals['unitname'] = self.quote(config.search(UNITNAME, {'interpolate': True})[1])
            wherevals['attnum'] = self.quote(config.search(ATTNUM, {'interpolate': False})[1])
            self.update_PFW_row ('PFW_ATTEMPT', wherevals, updatevals)
            


        row = {}
        row['reqnum'] = self.quote(config.search(REQNUM, {'interpolate': True})[1])
        row['unitname'] = self.quote(config.search(UNITNAME, {'interpolate': True})[1])
        row['attnum'] = self.quote(config.search(ATTNUM, {'interpolate': False})[1])
        row['blknum'] = self.quote(blknum)
        row['name'] = self.quote(config.search('blockname', {'interpolate': True})[1])
        row['modulelist'] = self.quote(config.search(SW_MODULELIST, {'interpolate': True})[1])
        row['starttime'] = self.get_current_timestamp_str()
        self.insert_pfw_row('PFW_BLOCK', row)

    def update_block_numexpjobs (self, config, numexpjobs):
        """ update numexpjobs in pfw_block """
        updatevals = {}
        updatevals['numexpjobs'] = self.quote(numexpjobs)

        wherevals = {}
        wherevals['reqnum'] = self.quote(config.search(REQNUM, {'interpolate': True})[1])
        wherevals['unitname'] = self.quote(config.search(UNITNAME, {'interpolate': True})[1])
        wherevals['attnum'] = self.quote(config.search(ATTNUM, {'interpolate': False})[1])
        wherevals['blknum'] = self.quote(config.search(PF_BLKNUM, {'interpolate': False})[1])

        self.update_PFW_row ('PFW_BLOCK', wherevals, updatevals)

        
    def update_block_end (self, config, exitcode):
        """ update row in pfw_block with end of block info"""

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = self.quote(exitcode)

        wherevals = {}
        wherevals['reqnum'] = self.quote(config.search(REQNUM, {'interpolate': True})[1])
        wherevals['unitname'] = self.quote(config.search(UNITNAME, {'interpolate': True})[1])
        wherevals['attnum'] = self.quote(config.search(ATTNUM, {'interpolate': False})[1])
        wherevals['blknum'] = self.quote(config.search(PF_BLKNUM, {'interpolate': False})[1])

        self.update_PFW_row ('PFW_BLOCK', wherevals, updatevals)

    #### BLKTASK #####
    def insert_blktask(self, config, modname, taskname):
        """ Insert an entry into the pfw_blktask table """
        fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_blktask table\n")
        row = {}
        row['reqnum'] = self.quote(config.search(REQNUM, {'interpolate': True})[1])
        row['unitname'] = self.quote(config.search(UNITNAME, {'interpolate': True})[1])
        row['attnum'] = self.quote(config.search(ATTNUM, {'interpolate': False})[1])
        row['blknum'] = self.quote(config.search(PF_BLKNUM, {'interpolate': False})[1])
        tasknum = config.inc_tasknum(1)
        row['tasknum'] = self.quote(tasknum)
        row['name'] = self.quote("%s_%s" % (taskname, modname))
        row['starttime'] = self.get_current_timestamp_str()
        self.insert_pfw_row('PFW_BLKTASK', row)
        
    def update_blktask_end (self, config, modname, taskname, status):
        """ update row in pfw_block with end of block info"""

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = self.quote(status)

        wherevals = {}
        wherevals['reqnum'] = self.quote(config.search(REQNUM, {'interpolate': True})[1])
        wherevals['unitname'] = self.quote(config.search(UNITNAME, {'interpolate': True})[1])
        wherevals['attnum'] = self.quote(config.search(ATTNUM, {'interpolate': False})[1])
        wherevals['blknum'] = self.quote(config.search(PF_BLKNUM, {'interpolate': False})[1])
        wherevals['name'] = self.quote("%s_%s" % (taskname, modname))

        self.update_PFW_row ('PFW_BLKTASK', wherevals, updatevals)



    ##### JOB #####
    def insert_job (self, wcl):
        """ Insert an entry into the pfw_job table """
        fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_job table\n")

        row = {}
        row['reqnum'] = self.quote(wcl[REQNUM])
        row['unitname'] = self.quote(wcl[UNITNAME])
        row['attnum'] = self.quote(wcl[ATTNUM])
        row['blknum'] = self.quote(wcl[PF_BLKNUM])
        row['jobnum'] = self.quote(wcl[PF_JOBNUM])
        row['starttime'] = self.get_current_timestamp_str()
        row['numexpwrap'] = self.quote(wcl['numexpwrap'])
        row['exechost'] = self.quote(socket.gethostname())
        row['pipeprod'] = self.quote(wcl['pipeprod'])
        row['pipever'] = self.quote(wcl['pipever'])

        if 'jobkeys' in wcl:
            row['jobkeys'] = self.quote(wcl['jobkeys'])
            

        # batchid 
        if "PBS_JOBID" in os.environ:
            row['batchid'] = self.quote(os.environ['PBS_JOBID'].split('.')[0])
        elif 'LSB_JOBID' in os.environ:
            row['batchid'] = self.quote(os.environ['LSB_JOBID']) 
        elif 'LOADL_STEP_ID' in os.environ:
            row['batchid'] = self.quote(os.environ['LOADL_STEP_ID'].split('.').pop())
        elif 'SUBMIT_CONDORID' in os.environ:
            row['batchid'] = self.quote(os.environ['SUBMIT_CONDORID'])

        if 'SUBMIT_CONDORID' in os.environ:
            row['condorid'] = self.quote(os.environ['SUBMIT_CONDORID'])
        
        self.insert_pfw_row('PFW_JOB', row)


    def update_job_end (self, wcl, exitcode):
        """ update row in pfw_job with end of job info"""

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = self.quote(exitcode)

        wherevals = {}
        wherevals['reqnum'] = self.quote(wcl[REQNUM])
        wherevals['unitname'] = self.quote(wcl[UNITNAME])
        wherevals['attnum'] = self.quote(wcl[ATTNUM])
        wherevals['jobnum'] = self.quote(wcl[PF_JOBNUM])

        self.update_PFW_row ('PFW_JOB', wherevals, updatevals)



    ##### WRAPPER #####
    def insert_wrapper (self, inputwcl, iwfilename):
        """ insert row into pfw_wrapper """

        wrapid = self.get_seq_next_value('pfw_wrapper_seq')

        row = {}
        row['reqnum'] = self.quote(inputwcl[REQNUM])
        row['unitname'] = self.quote(inputwcl[UNITNAME])
        row['attnum'] = self.quote(inputwcl[ATTNUM])
        row['wrapnum'] = self.quote(inputwcl[PF_WRAPNUM])
        row['modname'] = self.quote(inputwcl['modname'])
        row['name'] = self.quote(inputwcl['wrapname'])
        row['id'] = self.quote(wrapid)
        row['blknum'] = self.quote(inputwcl[PF_BLKNUM])
        row['jobnum'] = self.quote(inputwcl[PF_JOBNUM])
        row['inputwcl'] = self.quote(os.path.split(iwfilename)[-1])
        row['starttime'] = self.get_current_timestamp_str()

        self.insert_pfw_row('PFW_WRAPPER', row)
        return wrapid


    def update_wrapper_end (self, inputwcl, owclfile, logfile, exitcode):
        """ update row in pfw_wrapper with end of wrapper info """

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        if owclfile is not None:
            updatevals['outputwcl'] = self.quote(os.path.split(owclfile)[-1])
        if logfile is not None:
            updatevals['log'] = self.quote(os.path.split(logfile)[-1])
        updatevals['status'] = self.quote(exitcode)

        wherevals = {}
        wherevals['id'] = self.quote(inputwcl['wrapperid'])

        self.update_PFW_row ('PFW_WRAPPER', wherevals, updatevals)
        


    ##### PFW_EXEC
    def insert_exec (self, inputwcl, sect):
        """ insert row into pfw_exec """

        fwdebug(3, 'PFWDB_DEBUG', sect)
        fwdebug(3, 'PFWDB_DEBUG', inputwcl[sect])

        execid = self.get_seq_next_value('pfw_exec_seq')

        row = {}
        row['reqnum'] = self.quote(inputwcl[REQNUM])
        row['unitname'] = self.quote(inputwcl[UNITNAME])
        row['attnum'] = self.quote(inputwcl[ATTNUM])
        row['wrapnum'] = self.quote(inputwcl[PF_WRAPNUM])
        row['id'] = self.quote(execid)
        row['execnum'] = self.quote(inputwcl[sect]['execnum'])
        row['name'] = self.quote(inputwcl[sect]['execname'])
        if 'version' in inputwcl[sect] and inputwcl[sect]['version'] is not None:
            row['version'] = self.quote(inputwcl[sect]['version'])

        self.insert_pfw_row('PFW_EXEC', row)
        fwdebug(3, 'PFWDB_DEBUG', "end")
        return execid


    def update_exec_end (self, execwcl, execid, exitcode):
        """ update row in pfw_exec with end of exec info """
        fwdebug(3, 'PFWDB_DEBUG', execid)

        updatevals = {}
        updatevals['cmdargs'] = self.quote(execwcl['cmdlineargs'])
        updatevals['walltime'] = self.quote(execwcl['walltime'])
        updatevals['status'] = self.quote(exitcode)

        wherevals = {}
        wherevals['id'] = self.quote(execid)

        self.update_PFW_row ('PFW_EXEC', wherevals, updatevals)


    ##########
    def insert_pfw_row (self, pfwtable, row):
        """ Insert a row into a table and return any specified cols """ 
        sql = "insert into %s (%s) values (%s)" % (pfwtable, 
                                                   ','.join(row.keys()), 
                                                   ','.join(row.values()))
    
        fwdebug(3, 'PFWDB_DEBUG', sql)

        curs = self.cursor()
        fwdebug(3, 'PFWDB_DEBUG', "cursor")
        try:
            curs.execute(sql)
        except Exception as err:
            fwdie("Error: %s\nsql> %s\n" % (err,sql), PF_EXIT_FAILURE)

        fwdebug(3, 'PFWDB_DEBUG', "execute")
        self.commit()
        fwdebug(3, 'PFWDB_DEBUG', "end")
            
    def update_PFW_row (self, pfwtable, wherevals, updatevals):
        """ update a row into pfw_wrapper """

        whclause = []
        for c,v in wherevals.items():
            whclause.append("%s=%s" % (c,v))

        upclause = []
        for c,v in updatevals.items():
            upclause.append("%s=%s" % (c,v))

    
        sql = "update %s set %s where %s" % (pfwtable, 
                                             ','.join(upclause),
                                             ' and '.join(whclause))

        fwdebug(3, 'PFWDB_DEBUG', sql)
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

    def get_required_headers(self, filetypeDict):
        """
        For use by ingest_file_metadata. Collects the list of required header values.
        """
        REQUIRED = "r"
        allReqHeaders = set()
        for category,catDict in filetypeDict[REQUIRED].iteritems():
            allReqHeaders = allReqHeaders.union(catDict.keys())
        return allReqHeaders


    def get_column_map(self, filetypeDict):
        """
        For use by ingest_file_metadata. Creates a lookup from column to header.
        """
        columnMap = OrderedDict()
        for statusDict in filetypeDict.values():
            if type(statusDict) in (OrderedDict,dict):
                for catDict in statusDict.values():
                    for header, columns in catDict.iteritems():
                        collist = columns.split(',')
                        for position, column in enumerate(collist):
                            if len(collist) > 1:
                                columnMap[column] = header + ":" + str(position)
                            else:
                                columnMap[column] = header
        return columnMap


    def ingest_file_metadata(self, filemeta, dbdict):
        """
        Ingests the file metadata stored in <filemeta> into the database,
        using <dbdict> to determine where each element belongs.
        This wil throw an error and abort if any of the following are missing
        for any file: the filename, filetype, or other required header value.
        It will also throw an error if the filetype given in the input data
        is not found in <dbdict>
        Any exception will abort the entire upload.
        """
        FILETYPE  = "filetype"
        FILENAME  = "filename"
        METATABLE = "metadata_table"
        COLMAP    = "column_map"
        ROWS      = "rows"
        metadataTables = OrderedDict()
        fullMessage = []
        
        try:
            for key, filedata in filemeta.iteritems():
                foundError = 0
                if FILENAME not in filedata.keys():
                    fullMessage.append("ERROR: cannot upload file <" + key + ">, no FILENAME provided.")
                    continue
                if FILETYPE not in filedata.keys():
                    fullMessage.append("ERROR: cannot upload file " + filedata[FILENAME] + ": no FILETYPE provided.")
                    continue
                if filedata[FILETYPE] not in dbdict:
                    fullMessage.append("ERROR: cannot upload " + filedata[FILENAME] + ": " + \
                            filedata[FILETYPE] + " is not a known FILETYPE.")
                    continue
                # check that all required are present
                allReqHeaders = self.get_required_headers(dbdict[filedata[FILETYPE]])
                for dbkey in allReqHeaders:
                    if dbkey not in filedata.keys() or filedata[dbkey] == "":
                        fullMessage.append("ERROR: " + filedata[FILENAME] + " missing required data for " + dbkey)
                
                # any error should then skip all upload attempts
                if len(fullMessage) > 0:
                    continue

                # now load structures needed for upload
                rowdata = OrderedDict()
                mappedHeaders = set()
                fileMetaTable = dbdict[filedata[FILETYPE]][METATABLE]

                if fileMetaTable not in metadataTables.keys():
                    metadataTables[fileMetaTable] = OrderedDict()
                    metadataTables[fileMetaTable][COLMAP] = self.get_column_map(dbdict[filedata[FILETYPE]])
                    metadataTables[fileMetaTable][ROWS] = []
                
                colmap = metadataTables[fileMetaTable][COLMAP]
                for column, header in colmap.iteritems():
                    compheader = header.split(':')
                    if len(compheader) > 1:
                        hdr = compheader[0]
                        pos = int(compheader[1])
                        if hdr in filedata:
                            rowdata[column] = filedata[hdr].split(',')[pos]
                            mappedHeaders.add(hdr)
                    else:
                        if header in filedata:
                            rowdata[column] = filedata[header]
                            mappedHeaders.add(header)
                        else:
                            rowdata[column] = None
                
                # report elements that were in the file that do not map to a DB column
                for notmapped in (set(filedata.keys()) - mappedHeaders):
                    if notmapped != 'fullname':
                        print "WARN: file " + filedata[FILENAME] + " header item " \
                            + notmapped + " does not match column for filetype " \
                            + filedata[FILETYPE]
                
                # add the new data to the table set of rows
                metadataTables[fileMetaTable][ROWS].append(rowdata)
            # end looping through files
            
            for metaTable, dict in metadataTables.iteritems():
                self.insert_many(metaTable, dict[COLMAP].keys(), dict[ROWS])
            self.commit()
            
        except:
            raise

        if len(fullMessage) > 0:
            print >> sys.stderr, "\n".join(fullMessage)
            raise RequiredMetadataMissingError("\n".join(fullMessage))

    # end ingest_file_metadata


    def getFilenameIdMap(self, prov):
        DELIM = ","
        USED  = "used"
        WGB   = "was_generated_by"
        WDF   = "was_derived_from"
        FILENAME = "FILENAME"
        TABLENAME = "FILENAME_TMP"
        SQLSTR = "SELECT f.filename, a.ID FROM OPM_ARTIFACT a, FILENAME_TMP f WHERE a.name=f.filename"
        colmap = [FILENAME]
        allfiles = set()
        result = []
        rows = []
        if USED in prov:
            for filenames in prov[USED].values():
                for file in filenames.split(DELIM):
                    allfiles.add(file.strip())
        if WGB in prov:
            for filenames in prov[WGB].values():
                for file in filenames.split(DELIM):
                    allfiles.add(file.strip())
        if WDF in prov:
            for tuples in prov[WDF].values():
                for filenames in tuples.values():
                    for file in filenames.split(DELIM):
                        allfiles.add(file.strip())
        for file in allfiles:
            rows.append(dict({FILENAME:file}))
        if len(allfiles) > 0:
            self.insert_many(TABLENAME,colmap,rows)
            cursor = self.cursor()
            cursor.execute(SQLSTR)
            result = cursor.fetchall()
            cursor.close()
            self.commit()
            return dict(result)
        else:
            return result
        # end getFilenameIdMap


    def ingest_provenance(self, prov, execids):
        DELIM = ","
        USED  = "used"
        WGB   = "was_generated_by"
        WDF   = "was_derived_from"
        OPM_PROCESS_ID = "OPM_PROCESS_ID"
        OPM_ARTIFACT_ID = "OPM_ARTIFACT_ID"
        PARENT_OPM_ARTIFACT_ID = "PARENT_OPM_ARTIFACT_ID"
        CHILD_OPM_ARTIFACT_ID  = "CHILD_OPM_ARTIFACT_ID"
        USED_TABLE = "OPM_USED"
        WGB_TABLE  = "OPM_WAS_GENERATED_BY"
        WDF_TABLE  = "OPM_WAS_DERIVED_FROM"
        COLMAP_USED_WGB = [OPM_PROCESS_ID,OPM_ARTIFACT_ID]
        COLMAP_WDF = [PARENT_OPM_ARTIFACT_ID,CHILD_OPM_ARTIFACT_ID]
        PARENTS = "parents"
        CHILDREN = "children"

        insertSQL = """insert into %s d (%s) select %s,%s %s where not exists(
                    select * from %s n where n.%s=%s and n.%s=%s)"""

        data = []
        bindStr = self.get_positional_bind_string()
        cursor = self.cursor()
        filemap = self.getFilenameIdMap(prov)
        
        if USED in prov:
            for execname, filenames in prov[USED].iteritems():
                for file in filenames.split(DELIM):
                    rowdata = []
                    rowdata.append(execids[execname])
                    rowdata.append(filemap[file.strip()])
                    rowdata.append(execids[execname])
                    rowdata.append(filemap[file.strip()])
                    data.append(rowdata)
            execSQL = insertSQL % (USED_TABLE,OPM_PROCESS_ID + "," + OPM_ARTIFACT_ID, bindStr, bindStr,self.from_dual(), USED_TABLE, OPM_PROCESS_ID,bindStr,OPM_ARTIFACT_ID,bindStr)
            cursor.executemany(execSQL, data)
            data = []
        
        if WGB in prov:
            for execname, filenames in prov[WGB].iteritems():
                for file in filenames.split(DELIM):
                    rowdata = []
                    rowdata.append(execids[execname])
                    rowdata.append(filemap[file.strip()])
                    rowdata.append(execids[execname])
                    rowdata.append(filemap[file.strip()])
                    data.append(rowdata)
            execSQL = insertSQL % (WGB_TABLE,OPM_PROCESS_ID + "," + OPM_ARTIFACT_ID, bindStr, bindStr,self.from_dual(), WGB_TABLE, OPM_PROCESS_ID,bindStr,OPM_ARTIFACT_ID,bindStr)
            cursor.executemany(execSQL, data)
            data = []
        
        if WDF in prov:
            for tuples in prov[WDF].values():
                for parentfile in tuples[PARENTS].split(DELIM):
                    for childfile in tuples[CHILDREN].split(DELIM):
                        rowdata = []
                        rowdata.append(filemap[parentfile.strip()])
                        rowdata.append(filemap[childfile.strip()])
                        rowdata.append(filemap[parentfile.strip()])
                        rowdata.append(filemap[childfile.strip()])
                        data.append(rowdata)
            execSQL = insertSQL % (WDF_TABLE,PARENT_OPM_ARTIFACT_ID + "," + CHILD_OPM_ARTIFACT_ID, bindStr, bindStr,self.from_dual(), WDF_TABLE, PARENT_OPM_ARTIFACT_ID,bindStr,CHILD_OPM_ARTIFACT_ID,bindStr)
            cursor.executemany(execSQL, data)
            self.commit()
    #end_ingest_provenance


    def get_job_info(self, reqnum, unitname, attnum, blknum = None):
        sql = "select * from pfw_job where reqnum=%s and attnum=%s and unitname='%s'" % (reqnum, attnum, unitname)
        if blknum == -1:  # want only latest block
           blknum = len(blockinfo) - 1 
        if blknum is not None:
            sql += " and blknum=%s" % blknum

        fwdebug(1, 'PFWDB_DEBUG', sql)
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        jobinfo = {}
        for line in curs:
            d = dict(zip(desc, line))
            jobinfo[d['jobnum']] = {}
            jobinfo[d['jobnum']] = d
        return jobinfo


    def get_attempt_info(self, reqnum, unitname, attnum):
        """ """
        # get the run info
        sql = "select * from pfw_attempt where reqnum=%s and attnum=%s and unitname='%s'"  % (reqnum, attnum, unitname)
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]
        attinfo = dict(zip(desc, curs.fetchall()[0]))
        return attinfo


    def get_block_info(self, reqnum, unitname, attnum, blknum = None):
        # get the block info
        sql = "select * from pfw_block where reqnum=%s and attnum=%s and unitname='%s'"  % (reqnum, attnum, unitname)
        if blknum == -1:  # want only latest block
            sql += " and blknum = (select max blknum from pfw_block where reqnum=%s and attnum=%s and unitname='%s')"  % (reqnum, attnum, unitname)
        elif blknum is not None:
            sql += " and blknum=%s" % blknum
            
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]
        blockinfo = {}
        for line in curs:
            b = dict(zip(desc, line))
            blockinfo[b['blknum']] = b
        return blockinfo


    def get_wrapper_info(self, reqnum, unitname, attnum, blknum = None):
        # get wrapper instance information
        sql = "select * from pfw_wrapper where reqnum=%s and attnum=%s and unitname='%s'" % (reqnum, attnum, unitname)
        if blknum == -1:  # want only latest block
            sql += " and blknum = (select max blknum from pfw_block where reqnum=%s and attnum=%s and unitname='%s')"  % (reqnum, attnum, unitname)
        elif blknum is not None:
            sql += " and blknum=%s" % blknum
            
        #print "sql =", sql
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]
        wrappers = {}
        for line in curs:
            d = dict(zip(desc, line))
            wrappers[d['wrapnum']] = d

        return wrappers
