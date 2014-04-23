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
import traceback
from collections import OrderedDict

import coreutils
from processingfw.pfwdefs import *
from coreutils.miscutils import *
from processingfw.pfwutils import next_tasknum

#from errors import DuplicateDBFiletypeError
#from errors import DuplicateDBHeaderError
#from errors import IdMetadataHeaderError
#from errors import FileMetadataIngestError
#from errors import RequiredMetadataMissingError
#from errors import DBMetadataNotFoundError

PFW_MSG_ERROR = 3
PFW_MSG_WARN = 2
PFW_MSG_INFO = 1

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
        
        result['archive'] = self.get_archive_info()
        result['archive_transfer'] = self.get_archive_transfer_info()
        result['job_file_mvmt'] = self.get_job_file_mvmt_info()

        result[DIRPATSECT] = self.get_database_table('OPS_DIRECTORY_PATTERN', 'NAME')
        result[SW_FILEPATSECT] = self.get_filename_pattern()

        result['site'] = self.get_site_info()
        result[SW_EXEC_DEF] = self.get_database_table('OPS_EXEC_DEF', 'NAME')

        result['filetype_metadata'] = self.get_all_filetype_metadata()
        result['file_header'] = self.query_results_dict('select * from OPS_FILE_HEADER', 'name')

        return result


    def get_database_table(self, tname, tkey):
        sql = "select * from %s" % tname
        results = self.query_results_dict(sql, tkey)
        return results

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


    ##### request, unit, attempt #####
    def insert_run(self, config):
        """ Insert entries into the pfw_request, pfw_unit, pfw_attempt tables for a single run submission """
        maxtries = 1
        from_dual = self.from_dual()


        allparams = {}
        allparams['reqnum'] = config[REQNUM]
        allparams['unitname'] =  config.search(UNITNAME, {'interpolate': True})[1]
        allparams['project'] = config.search('project', {'interpolate': True})[1]
        allparams['jiraid'] = config.search('jira_id', {'interpolate': True})[1]
        allparams['pipeline'] = config.search('pipeline', {'interpolate': True})[1]
        allparams['operator'] =  config.search('operator', {'interpolate': True})[1]
        allparams['numexpblk'] = len(fwsplit(config[SW_BLOCKLIST]))
        (exists, value) = config.search('basket', {'interpolate': True})
        if exists:
            allparams['basket'] = value
        else:
            allparams['basket'] = None
        (exists, value) = config.search('group_submit_id', {'interpolate': True})
        if exists:
            allparams['group_submit_id'] = value
        else:
            allparams['group_submit_id'] = None

        # create named bind strings for all parameters
        namebinds = {}
        for k in allparams.keys():
            namebinds[k] = self.get_named_bind_string(k)

        # loop to try again, esp. for race conditions
        loopcnt = 1
        done = False
        while not done and loopcnt <= maxtries:
            sql = None
            params = None
            try:
                curs = self.cursor()

                # pfw_request
                fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_request table\n")
                sql =  "insert into pfw_request (reqnum, project, jira_id, pipeline) " 
                sql += "select %s, %s, %s, %s %s where not exists (select null from pfw_request where reqnum=%s)" % \
                       (namebinds['reqnum'], namebinds['project'], namebinds['jiraid'], namebinds['pipeline'], 
                       from_dual, namebinds['reqnum'])
                fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)

                params = {}
                for k in ['reqnum', 'project', 'jiraid', 'pipeline']:
                    params[k]=allparams[k]
                fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % params)
                curs.execute(sql, params)

                # pfw_unit
                fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_unit table\n")
                curs = self.cursor()
                sql = "insert into pfw_unit (reqnum, unitname) select %s, %s %s where not exists (select null from pfw_unit where reqnum=%s and unitname=%s)" % (namebinds['reqnum'], namebinds['unitname'], from_dual, namebinds['reqnum'], namebinds['unitname'])
                fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)
                params = {}
                for k in ['reqnum', 'unitname']:
                    params[k]=allparams[k]
                fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % params)
                curs.execute(sql, params)

                # pfw_attempt
                fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_attempt table\n")
                ## get current max attnum and try next value
                sql = "select max(attnum) from pfw_attempt where reqnum=%s and unitname=%s" % (namebinds['reqnum'], namebinds['unitname'])
                fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)
                params = {}
                for k in ['reqnum', 'unitname']:
                    params[k]=allparams[k]
                fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % params)
                curs.execute(sql, params)
                maxarr = curs.fetchall()
                if len(maxarr) == 0:
                    maxatt = 0
                elif maxarr[0][0] == None:
                    maxatt = 0
                else:
                    maxatt = int(maxarr[0][0])

                fwdebug(3, 'PFWDB_DEBUG', "maxatt = %s" % maxatt)
                allparams['attnum'] = maxatt + 1
                namebinds['attnum'] = self.get_named_bind_string('attnum')

                # execute will fail if extra params
                params = {}
                for k in ['reqnum', 'unitname', 'attnum', 'operator', 'numexpblk', 'basket', 'group_submit_id']:
                    params[k]=allparams[k]
                fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % params)

                sql = "insert into pfw_attempt (reqnum, unitname, attnum, operator, submittime, numexpblk, basket, group_submit_id) select %s, %s, %s, %s, %s, %s, %s, %s %s where not exists (select null from pfw_attempt where reqnum=%s and unitname=%s and attnum=%s)" % (namebinds['reqnum'], namebinds['unitname'], namebinds['attnum'], namebinds['operator'], self.get_current_timestamp_str(), namebinds['numexpblk'], namebinds['basket'], namebinds['group_submit_id'], from_dual, namebinds['reqnum'], namebinds['unitname'], namebinds['attnum'])
                fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)

                curs.execute(sql, params)

                config[ATTNUM] = allparams['attnum']
                done = True
            except Exception:
                print "\n\n"
                print "sql> ", sql
                print "params> ", params
                print "namebinds> ", namebinds
                (type, value, traceback) = sys.exc_info()
                if loopcnt < maxtries:
                    fwdebug(0, 'PFWDB_DEBUG', "Warning: %s" % value)
                    fwdebug(0, 'PFWDB_DEBUG', "Retrying inserting run into database\n\n")
                    loopcnt = loopcnt + 1
                    self.rollback()
                    continue
                else:
                    raise

        if not done:
            raise Exception("Exceeded max tries for inserting into pfw_attempt table")


        curs.close()
        self.commit()  # not calling insert_PFW_row so must commit here

    def insert_attempt_label(self, config):
        fwdebug(3, 'PFWDB_DEBUG', "Inserting into pfw_attempt_label table\n")

        row = {}
        row['reqnum'] = config[REQNUM]
        row['unitname'] =  config.search(UNITNAME, {'interpolate': True})[1]
        row['attnum'] = config[ATTNUM]

        if SW_LABEL in config:
            labels = config.search(SW_LABEL, {'interpolate': True})[1]
            labels = fwsplit(labels,',')
            for label in labels:
                row['label'] = label
                self.insert_PFW_row('PFW_ATTEMPT_LABEL', row)


    def update_attempt_cid (self, config, condorid):
        """ update row in pfw_attempt with condorid """

        updatevals = {}
        updatevals['condorid'] = condorid

        wherevals = {}
        wherevals['reqnum'] = config[REQNUM]
        wherevals['unitname'] = config[UNITNAME]
        wherevals['attnum'] = config[ATTNUM]

        self.update_PFW_row ('PFW_ATTEMPT', wherevals, updatevals)


    def update_attempt_beg (self, config):
        """ update row in pfw_attempt with beg of attempt info """


        updatevals = {}
        updatevals['starttime'] = self.get_current_timestamp_str()

        wherevals = {}
        wherevals['reqnum'] = config[REQNUM]
        wherevals['unitname'] = config[UNITNAME]
        wherevals['attnum'] = config[ATTNUM]

        self.update_PFW_row ('PFW_ATTEMPT', wherevals, updatevals)


    def update_attempt_end (self, config, exitcode):
        """ update row in pfw_attempt with end of attempt info """

        self.update_attempt_end_vals(config[REQNUM], config[UNITNAME], config[ATTNUM], exitcode)
        

    def update_attempt_end_vals (self, reqnum, unitname, attnum, exitcode):
        """ update row in pfw_attempt with end of attempt info """

        #self.end_timing(config, 'attempt', config['submit_run'], exitcode)

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['reqnum'] = reqnum
        wherevals['unitname'] = unitname
        wherevals['attnum'] = attnum
        
        self.update_PFW_row ('PFW_ATTEMPT', wherevals, updatevals)

    #### ATTEMPT_TASK #####
    def insert_attempt_task(self, config, taskname):
        """ Insert an entry into the pfw_attempt_task table """
        fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_attempt_task table\n")

        row = {}
        row['reqnum'] = config[REQNUM]
        row['unitname'] = config[UNITNAME]
        row['attnum'] = config[ATTNUM]
        row['tasknum'] = next_tasknum(config, 'attempt')
        row['taskname'] = taskname
        row['starttime'] = self.get_current_timestamp_str()
        self.insert_PFW_row('PFW_BLOCK_TASK', row)
        return row['tasknum']


    def update_attempt_task_end (self, config, tasknum, exitcode):
        """ update row in pfw_attempt_task with end of task info """

        #self.end_timing(config, 'attempt', config['submit_run'], exitcode)

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['reqnum'] = config[REQNUM]
        wherevals['unitname'] = config[UNITNAME]
        wherevals['attnum'] = config[ATTNUM]
        wherevals['tasknum'] = tasknum

        self.update_PFW_row ('PFW_ATTEMPT_TASK', wherevals, updatevals)


    ##### BLOCK #####
    def insert_block (self, config):
        """ Insert an entry into the pfw_block table """
        fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_block table\n")

        blknum = config.search(PF_BLKNUM, {'interpolate': False})[1]
        if blknum == '1':  # attempt is starting
            self.update_attempt_beg(config)

        blkname = config.search('blockname', {'interpolate': True})[1]
        #self.start_timing(config, 'block', blkname, blknum=blknum)
            
        row = {}
        row['reqnum'] = config[REQNUM]
        row['unitname'] = config[UNITNAME]
        row['attnum'] = config[ATTNUM]
        row['blknum'] = blknum
        row['name'] = config.search('blockname', {'interpolate': True})[1]
        row['target_site'] = config.search('target_site', {'interpolate': True})[1]
        row['modulelist'] = config.search(SW_MODULELIST, {'interpolate': True})[1]
        row['starttime'] = self.get_current_timestamp_str()
        self.insert_PFW_row('PFW_BLOCK', row)

    def update_block_numexpjobs (self, config, numexpjobs):
        """ update numexpjobs in pfw_block """
        updatevals = {}
        updatevals['numexpjobs'] = numexpjobs

        wherevals = {}
        wherevals['reqnum'] = config[REQNUM]
        wherevals['unitname'] = config[UNITNAME]
        wherevals['attnum'] = config[ATTNUM]
        wherevals['blknum'] = config.search(PF_BLKNUM, {'interpolate': False})[1]

        self.update_PFW_row ('PFW_BLOCK', wherevals, updatevals)

        
    def update_block_end (self, config, exitcode):
        """ update row in pfw_block with end of block info"""
    
        #self.end_timing(config, 'block', blkname, exitcode, blknum=blknum)

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['reqnum'] = config[REQNUM]
        wherevals['unitname'] = config[UNITNAME]
        wherevals['attnum'] = config[ATTNUM]
        wherevals['blknum'] = config.search(PF_BLKNUM, {'interpolate': False})[1]

        self.update_PFW_row ('PFW_BLOCK', wherevals, updatevals)

    #### BLOCK_TASK #####
    def insert_block_task(self, config, taskname):
        """ Insert an entry into the pfw_blktask table """

        row = {}
        row['reqnum'] = config[REQNUM]
        row['unitname'] = config[UNITNAME]
        row['attnum'] = config[ATTNUM]
        row['blknum'] = config.search(PF_BLKNUM, {'interpolate': False})[1]
        row['tasknum'] = next_tasknum(config, 'block')
        row['taskname'] = taskname
        row['starttime'] = self.get_current_timestamp_str()
        self.insert_PFW_row('PFW_BLOCK_TASK', row)
        return row['tasknum']

        
    def update_block_task_end (self, config, tasknum, status):
        """ update row in pfw_block with end of block info"""

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = status

        wherevals = {}
        wherevals['reqnum'] = config[REQNUM]
        wherevals['unitname'] = config[UNITNAME]
        wherevals['attnum'] = config[ATTNUM]
        wherevals['blknum'] = config[PF_BLKNUM]
        wherevals['tasknum'] = tasknum

        self.update_PFW_row ('PFW_BLOCK_TASK', wherevals, updatevals)



    ##### JOB #####
    def insert_job (self, wcl, jobnum):
        """ Insert an entry into the pfw_job table """
        fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_job table\n")

        #self.start_timing(wcl, 'job', '%4d' % int(wcl[PF_JOBNUM]), blknum=wcl[PF_BLKNUM], jobnum=wcl[PF_JOBNUM])

        row = {}
        row['reqnum'] = wcl[REQNUM]
        row['unitname'] = wcl[UNITNAME]
        row['attnum'] = wcl[ATTNUM]
        row['blknum'] = wcl[PF_BLKNUM]
        row['jobnum'] = jobnum
        row['starttime'] = self.get_current_timestamp_str()
        row['numexpwrap'] = wcl['numexpwrap']
        row['pipeprod'] = wcl['pipeprod']
        row['pipever'] = wcl['pipever']

        if 'jobkeys' in wcl:
            row['jobkeys'] = wcl['jobkeys']
        self.insert_PFW_row('PFW_JOB', row)
            

    def update_job_batchids (self, wcl, jobnum, submit_condor_id = None, target_batch_id = None, exechost=None):

        updatevals = {}
        if exechost is not None:
            updatevals['exechost'] = exechost

        # batchid 
        if submit_condor_id is not None:
            updatevals['condorid'] = float(submit_condor_id)

        if target_batch_id is not None:
            updatevals['batchid'] = target_batch_id

        wherevals = {}
        wherevals['reqnum'] = wcl[REQNUM]
        wherevals['unitname'] = wcl[UNITNAME]
        wherevals['attnum'] = wcl[ATTNUM]
        wherevals['jobnum'] = wcl[PF_JOBNUM]

        
        if len(updatevals) > 0:
            self.update_PFW_row ('PFW_JOB', wherevals, updatevals)


    def update_job_junktar (self, wcl, junktar=None):
        """ update row in pfw_job with junk tarball name """

        if junktar is not None: 
            fwdebug(3, 'PFWDB_DEBUG', "Saving junktar (%s) to pfw_job" % junktar)
            updatevals = {}
            updatevals['junktar'] = junktar

            wherevals = {}
            wherevals['reqnum'] = wcl[REQNUM]
            wherevals['unitname'] = wcl[UNITNAME]
            wherevals['attnum'] = wcl[ATTNUM]
            wherevals['jobnum'] = wcl[PF_JOBNUM]

            self.update_PFW_row ('PFW_JOB', wherevals, updatevals)


    def update_job_end (self, wcl, exitcode):
        """ update row in pfw_job with end of job info"""

        #self.end_timing(wcl, 'job', '%4d' % int(wcl[PF_JOBNUM]), exitcode, blknum=wcl[PF_BLKNUM], jobnum=wcl[PF_JOBNUM])

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['reqnum'] = wcl[REQNUM]
        wherevals['unitname'] = wcl[UNITNAME]
        wherevals['attnum'] = wcl[ATTNUM]
        wherevals['jobnum'] = wcl[PF_JOBNUM]

        self.update_PFW_row ('PFW_JOB', wherevals, updatevals)



    def update_job_info (self, wcl, jobnum, jobinfo):
        """ update row in pfw_job with information gathered post job from condor log """

        fwdebug(1, 'PFWDB_DEBUG', "Updating job information post job (%s)" % jobnum)
        fwdebug(3, 'PFWDB_DEBUG', "jobinfo=%s"%jobinfo)

        updatevals = {}
        if 'gsubmittime' in jobinfo:
            updatevals['glsubmit'] = jobinfo['gsubmittime']
        if 'csubmittime' in jobinfo:
            updatevals['submittime'] = jobinfo['csubmittime']
        if 'starttime' in jobinfo:
            updatevals['starttime'] = jobinfo['starttime']
        if 'endtime' in jobinfo:
            updatevals['endtime'] = jobinfo['endtime']
        if 'retval' in jobinfo:
            updatevals['status'] = jobinfo['retval']
        if 'clusterid' in jobinfo:
            updatevals['condorid'] = jobinfo['clusterid']
        
        fwdebug(3, 'PFWDB_DEBUG', "updatevals = %s" %(updatevals))

        wherevals = {}
        wherevals['reqnum'] = wcl[REQNUM]
        wherevals['unitname'] = wcl[UNITNAME]
        wherevals['attnum'] = wcl[ATTNUM]
        wherevals['jobnum'] = jobnum
        fwdebug(3, 'PFWDB_DEBUG', "wherevals = %s" %(wherevals))

        if len(updatevals) > 0:
            self.update_PFW_row ('PFW_JOB', wherevals, updatevals)
        else:
            fwdebug(3, 'PFWDB_DEBUG', "Found 0 values to update (%s)" % (wherevals))
            fwdebug(6, 'PFWDB_DEBUG', "\tjobnum = %s, jobinfo = %s" % (jobnum, jobinfo))


    
    ### JOB_TASK
    def insert_job_task (self, wcl, taskname):
        """ Insert an entry into the pfw_job_task table """

        row = {}
        row['reqnum'] = wcl[REQNUM]
        row['unitname'] = wcl[UNITNAME]
        row['attnum'] = wcl[ATTNUM]
        row['jobnum'] = wcl[PF_JOBNUM]
        row['taskname'] = taskname
        row['tasknum'] = next_tasknum(wcl, 'job')
        row['starttime'] = self.get_current_timestamp_str()

        self.insert_PFW_row('PFW_JOB_TASK', row)
        return row['tasknum']


    def update_job_task_end (self, wcl, tasknum, exitcode):
        """ update row in pfw_job_task with end of task info"""

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['reqnum'] = wcl[REQNUM]
        wherevals['unitname'] = wcl[UNITNAME]
        wherevals['attnum'] = wcl[ATTNUM]
        wherevals['jobnum'] = wcl[PF_JOBNUM]
        wherevals['tasknum'] = tasknum

        self.update_PFW_row ('PFW_JOB_TASK', wherevals, updatevals)


    ##### MSG #####
    def insert_message(self, wcl, parent, msglevel, msg, blknum=None, jobnum=None, wrapnum=None, tasknum=None):
        """ Insert an entry into the pfw_message table """

        row = {}
        row['reqnum'] = wcl[REQNUM]
        row['unitname'] = wcl[UNITNAME]
        row['attnum'] = wcl[ATTNUM]
        row['parent'] = parent
        if blknum is not None:
            row['blknum'] = blknum 
        elif 'attempt' not in parent:
            row['blknum'] = wcl['blknum']
            
        if jobnum is not None:
            row['jobnum'] = jobnum 
        elif 'job' in parent:
            row['jobnum'] = wcl['jobnum'] 

        if wrapnum is not None:
            row['wrapnum'] = wrapnum 
        elif 'wrap' in parent:
            row['wrapnum'] = wcl['wrapnum'] 

        if tasknum is not None:
            row['tasknum'] = tasknum 
        elif 'task' in parent:
            row['tasknum'] = wcl['tasknums'][parent[:-len('_task')]] 

        row['msgtime'] = self.get_current_timestamp_str()
        row['msglevel'] = msglevel 
        row['msg'] = msg 
        self.insert_PFW_row('PFW_MESSAGE', row)



    ##### WRAPPER #####
    def insert_wrapper (self, wcl, iwfilename):
        """ insert row into pfw_wrapper """

        wrapid = self.get_seq_next_value('pfw_wrapper_seq')
        
        #self.start_timing(wcl, 'wrapper', '%4d_%s' % (int(wcl[PF_WRAPNUM]), wcl['modname']), blknum=wcl[PF_BLKNUM], jobnum=wcl[PF_JOBNUM], wrapnum=wcl['wrapnum'])

        row = {}
        row['reqnum'] = wcl[REQNUM]
        row['unitname'] = wcl[UNITNAME]
        row['attnum'] = wcl[ATTNUM]
        row['wrapnum'] = wcl[PF_WRAPNUM]
        row['modname'] = wcl['modname']
        row['name'] = wcl['wrapper']['wrappername']
        row['id'] = wrapid
        row['blknum'] = wcl[PF_BLKNUM]
        row['jobnum'] = wcl[PF_JOBNUM]
        row['inputwcl'] = os.path.split(iwfilename)[-1]
        row['starttime'] = self.get_current_timestamp_str()

        self.insert_PFW_row('PFW_WRAPPER', row)
        return wrapid


    def update_wrapper_end (self, wcl, owclfile, logfile, exitcode):
        """ update row in pfw_wrapper with end of wrapper info """

        #self.end_timing(wcl, 'wrapper', '%4d_%s' % (int(wcl[PF_WRAPNUM]), wcl['modname']), exitcode, blknum=wcl[PF_BLKNUM], jobnum=wcl[PF_JOBNUM], wrapnum=wcl['wrapnum'])

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        if owclfile is not None:
            updatevals['outputwcl'] = os.path.split(owclfile)[-1]
        if logfile is not None:
            updatevals['log'] = os.path.split(logfile)[-1]
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['id'] = wcl['wrapperid']

        self.update_PFW_row ('PFW_WRAPPER', wherevals, updatevals)

        
    ##### WRAPPER #####
    def insert_job_wrapper_task (self, wcl, taskname):
        """ insert row into pfw_job_wrapper_task """

        if PF_WRAPNUM not in wcl:
            print wcl.keys()
            raise Exception("Error: Cannot find %s" % PF_WRAPNUM)
        
        row = {}
        row['reqnum'] = wcl[REQNUM]
        row['unitname'] = wcl[UNITNAME]
        row['attnum'] = wcl[ATTNUM]
        row['jobnum'] = wcl[PF_JOBNUM]
        row['wrapnum'] = wcl[PF_WRAPNUM]
        row['taskname'] = taskname
        row['tasknum'] = next_tasknum(wcl, 'job_wrapper')
        row['starttime'] = self.get_current_timestamp_str()

        self.insert_PFW_row('PFW_JOB_WRAPPER_TASK', row)
        return row['tasknum']


    def update_job_wrapper_task_end (self, wcl, tasknum, exitcode):
        """ update row in pfw_job_wrapper_task with end of task info """

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['reqnum'] = wcl[REQNUM]
        wherevals['unitname'] = wcl[UNITNAME]
        wherevals['attnum'] = wcl[ATTNUM]
        wherevals['jobnum'] = wcl[PF_JOBNUM]
        wherevals['wrapnum'] = wcl[PF_WRAPNUM]
        wherevals['tasknum'] = tasknum

        self.update_PFW_row ('PFW_JOB_WRAPPER_TASK', wherevals, updatevals)


    ##### PFW_EXEC
    def insert_exec (self, wcl, sect):
        """ insert row into pfw_exec """

        fwdebug(3, 'PFWDB_DEBUG', sect)
        fwdebug(3, 'PFWDB_DEBUG', wcl[sect])

        execid = self.get_seq_next_value('pfw_exec_seq')

        row = {}
        row['reqnum'] = wcl[REQNUM]
        row['unitname'] = wcl[UNITNAME]
        row['attnum'] = wcl[ATTNUM]
        row['wrapnum'] = wcl[PF_WRAPNUM]
        row['id'] = execid
        row['execnum'] = wcl[sect]['execnum']
        row['name'] = wcl[sect]['execname']
        if 'version' in wcl[sect] and wcl[sect]['version'] is not None:
            row['version'] = wcl[sect]['version']

        self.insert_PFW_row('PFW_EXEC', row)
        fwdebug(3, 'PFWDB_DEBUG', "end")
        return execid

    def update_exec_version (self, execid, version):
        """ update row in pfw_exec with exec version info """
        fwdebug(3, 'PFWDB_DEBUG', execid)

        updatevals = {}
        updatevals['version'] = version

        wherevals = {}
        wherevals['id'] = execid

        self.update_PFW_row ('PFW_EXEC', wherevals, updatevals)


    def update_exec_end (self, execwcl, execid, exitcode):
        """ update row in pfw_exec with end of exec info """
        fwdebug(3, 'PFWDB_DEBUG', execid)

        updatevals = {}
        updatevals['cmdargs'] = execwcl['cmdlineargs']
        updatevals['walltime'] = execwcl['walltime']
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['id'] = execid

        self.update_PFW_row ('PFW_EXEC', wherevals, updatevals)


    ##### PFW_JOB_EXEC_TASK
    def insert_job_exec_task (self, wcl, execnum, taskname):
        """ insert row into pfw_job_exec_task """

        row = {}
        row['reqnum'] = wcl[REQNUM]
        row['unitname'] = wcl[UNITNAME]
        row['attnum'] = wcl[ATTNUM]
        row['wrapnum'] = wcl[PF_WRAPNUM]
        row['execnum'] = execnum
        row['taskname'] = taskname
        row['tasknum'] = next_tasknum(wcl, 'job_exec')
        row['starttime'] = self.get_current_timestamp_str()

        self.insert_PFW_row('PFW_JOB_EXEC_TASK', row)
        fwdebug(3, 'PFWDB_DEBUG', "end")
        return row['tasknum']

    ##### 
    def insert_task (self, wcl, tasktype, taskname, **kwargs):
        """ call correct insert task function """
        fwdebug(3, 'PFWDB_DEBUG', "BEG (%s, %s)" % (tasktype, taskname))

        tasknum = -1
        if tasktype == 'job':
            tasknum = self.insert_job_task(wcl, taskname)
        elif tasktype == 'job_wrapper':
            tasknum = self.insert_job_wrapper_task(wcl, taskname)
        elif tasktype == 'job_exec': 
            tasknum = self.insert_job_exec_task(wcl, kwargs['execnum'], taskname)
        else:
            fwdie("Error: invalid tasktype (%s)" % (tasktype), PF_EXIT_FAILURE, 2)

        fwdebug(3, 'PFWDB_DEBUG', "end")
        return tasknum


    def update_job_exec_task_end (self, wcl, execnum, tasknum, exitcode):
        """ update row in pfw_job_exec_task with end of task info """

        if tasknum > 0:
            updatevals = {}
            updatevals['endtime'] = self.get_current_timestamp_str()
            updatevals['status'] = exitcode

            wherevals = {}
            wherevals['reqnum'] = wcl[REQNUM]
            wherevals['unitname'] = wcl[UNITNAME]
            wherevals['attnum'] = wcl[ATTNUM]
            wherevals['wrapnum'] = wcl[PF_WRAPNUM]
            wherevals['execnum'] = execnum
            wherevals['tasknum'] = tasknum

            self.update_PFW_row ('PFW_JOB_EXEC_TASK', wherevals, updatevals)


    def update_task_end(self, wcl, tasktype, tasknum, status, **kwargs):
        """ call correct update task function """

        if tasktype == 'job':
            self.update_job_task_end(wcl, tasknum, status)
        elif tasktype == 'job_wrapper':
            self.update_job_wrapper_task_end(wcl, tasknum, status)
        elif tasktype == 'job_exec': 
            self.update_job_exec_task_end(wcl, kwargs['execnum'], tasknum, status)
        fwdebug(3, 'PFWDB_DEBUG', "end")

    
    #####
    def insert_data_query (self, wcl, modname, datatype, dataname, execname, cmdargs, version):
        """ insert row into pfw_data_query table """
        fwdebug(3, 'PFWDB_DEBUG', "BEG")

        queryid = self.get_seq_next_value('pfw_wrapper_seq')

        row = {}
        row['reqnum'] = wcl[REQNUM] 
        row['unitname'] = wcl[UNITNAME] 
        row['attnum'] = wcl[ATTNUM]
        row['blknum'] = wcl[PF_BLKNUM] 
        row['modname'] =  modname
        row['datatype'] = datatype   # file, list
        row['dataname'] = dataname
        row['id'] = queryid
        row['execname'] = os.path.basename(execname)
        row['cmdargs'] = cmdargs
        row['version'] = version
        row['starttime'] = self.get_current_timestamp_str()
        self.insert_PFW_row('PFW_DATA_QUERY', row) 
        fwdebug(3, 'PFWDB_DEBUG', "END")
        return queryid


    def update_data_query_end (self, queryid, exitcode):
        """ update row in pfw_data_query_end with end of query info """

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['id'] = queryid

        self.update_PFW_row ('PFW_DATA_QUERY', wherevals, updatevals)



    ##########
    def insert_PFW_row (self, pfwtable, row):
        """ Insert a row into a PFW table and commit """
        ctstr = self.get_current_timestamp_str()
        cols = row.keys()
        namedbind = []
        params = {}
        for col in cols:
            if row[col] == ctstr:
                namedbind.append(row[col])
            else:
                namedbind.append(self.get_named_bind_string(col))
                params[col] = row[col]

        sql = "insert into %s (%s) values (%s)" % (pfwtable, 
                                                   ','.join(cols), 
                                                   ','.join(namedbind))
    
        fwdebug(3, 'PFWDB_DEBUG', sql)
        fwdebug(3, 'PFWDB_DEBUG', params)

        curs = self.cursor()
        try:
            curs.execute(sql, params)
        except:
            print "******************************" 
            (type, value, traceback) = sys.exc_info()
            print "Error:", type, value
            print "sql> %s\n" % (sql)
            print "params> %s\n" % (params)
            raise

        self.commit()
        fwdebug(3, 'PFWDB_DEBUG', "end")

            

    def update_PFW_row (self, pfwtable, wherevals, updatevals):
        """ Update a row in a PFW table and commit """

        ctstr = self.get_current_timestamp_str()

        params = {}
        whclause = []
        for c,v in wherevals.items():
            if v == ctstr:
                whclause.append("%s=%s" % (c, v))
            else:
                whclause.append("%s=%s" % (c, self.get_named_bind_string('w_'+c)))
                params['w_'+c] = v

        upclause = []
        for c,v in updatevals.items():
            if v == ctstr:
                upclause.append("%s=%s" % (c, v))
            else:
                upclause.append("%s=%s" % (c, self.get_named_bind_string('u_'+c)))
                params['u_'+c] = v

    
        sql = "update %s set %s where %s" % (pfwtable, 
                                             ','.join(upclause),
                                             ' and '.join(whclause))

        fwdebug(3, 'PFWDB_DEBUG', sql)
        fwdebug(3, 'PFWDB_DEBUG', params)
        curs = self.cursor()
        try:
            curs.execute(sql, params)
        except:
            print "******************************"
            (type, value, traceback) = sys.exc_info()
            print "Error:", type, value
            print "sql> %s\n" % (sql)
            print "params = %s\n" % params
            raise
    
        if curs.rowcount == 0:
            print "******************************"
            print "sql> %s\n" % sql
            raise Exception("Error: 0 rows updated in table %s" % pfwtable) 

        curs.close()
        self.commit()


    def getFilenameIdMap(self, prov):
        DELIM = ","
        USED  = "used"
        WGB   = "was_generated_by"
        WDF   = "was_derived_from"

        allfiles = set()
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

        result = []
        if len(allfiles) > 0:
            gtt_name = self.load_filename_gtt(allfiles)
            sqlstr = "SELECT f.filename, a.ID FROM OPM_ARTIFACT a, %s f WHERE a.name=f.filename" % (gtt_name)
            cursor = self.cursor()
            cursor.execute(sqlstr)
            result = cursor.fetchall()
            cursor.close()
            self.empty_gtt(gtt_name)

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


    def get_job_info(self, wherevals):
        whclause = []
        for c,v in wherevals.items():
            whclause.append("%s=%s" % (c, self.get_named_bind_string(c)))

        sql = "select * from pfw_job where %s" % (' and '.join(whclause))

        fwdebug(3, 'PFWDB_DEBUG', "sql> %s" % sql)
        fwdebug(3, 'PFWDB_DEBUG', "params> %s" % wherevals)
        
        curs = self.cursor()
        curs.execute(sql, wherevals)
        desc = [d[0].lower() for d in curs.description]

        jobinfo = {}
        for line in curs:
            d = dict(zip(desc, line))
            jobinfo[d['jobnum']] = d
        return jobinfo


    def get_attempt_info(self, reqnum, unitname, attnum):
        """ """
        # get the run info
        sql = "select * from pfw_attempt where reqnum=%s and attnum=%s and unitname='%s'"  % (reqnum, attnum, unitname)
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]
        attinfo = None
        row = curs.fetchone()    # should only be 1 row
        if row is not None: 
            attinfo = dict(zip(desc, row))
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


    def get_run_filelist(self, reqnum, unitname, attnum, 
                         blknum = None, archive=None):

        # store filenames in dictionary just to ensure don't get filename multiple times
        filedict = {} 

        # setup up common where clauses and params
        wherevals = {'reqnum': reqnum, 'unitname':unitname, 'attnum': attnum}
        if blknum is not None:
            wherevals['blknum'] = blknum

        whclause = []
        for k in wherevals.keys():
            whclause.append("%s=%s" % (k, self.get_named_bind_string(k)))


        # search for output files
        sql = "select wgb.filename from wgb where %s" % (' and '.join(whclause))

        fwdebug(3, 'PFWDB_DEBUG', "sql> %s" % sql)
        fwdebug(3, 'PFWDB_DEBUG', "params> %s" % wherevals)
        
        curs = self.cursor()
        curs.execute(sql, wherevals)

        for row in curs:
            filedict[row[0]] = True


        # search for logs 
        # (not all logs show up in wgb, example ingestions which don't have output file)
        sql = "select log from pfw_wrapper where log is not NULL and %s" % (' and '.join(whclause))

        fwdebug(3, 'PFWDB_DEBUG', "sql> %s" % sql)
        fwdebug(3, 'PFWDB_DEBUG', "params> %s" % wherevals)
        
        curs = self.cursor()
        curs.execute(sql, wherevals)

        for row in curs:
            filedict[row[0]] = True

        # search for junk tarball
        sql = "select junktar from pfw_job where junktar is not NULL and %s" % (' and '.join(whclause))

        fwdebug(3, 'PFWDB_DEBUG', "sql> %s" % sql)
        fwdebug(3, 'PFWDB_DEBUG', "params> %s" % wherevals)
        
        curs = self.cursor()
        curs.execute(sql, wherevals)

        for row in curs:
            filedict[row[0]] = True

        
        # convert dictionary to list
        filelist = filedict.keys()
        fwdebug(3, 'PFWDB_DEBUG', "filelist = %s" % filelist)

        if archive is not None:   # limit to files on a specified archive
            gtt_name = self.load_filename_gtt(filelist)
            sqlstr = "SELECT f.filename FROM file_archive_info a, %s f WHERE a.filename=f.filename and a.archive_name=%s" % (gtt_name, self.get_named_bind_string('archive'))
            cursor = self.cursor()
            cursor.execute(sqlstr, {'archive':archive})
            results = cursor.fetchall()
            cursor.close()
            self.empty_gtt(gtt_name)
            filelist = [x[0] for x in results]

        return filelist
