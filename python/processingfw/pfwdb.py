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

import coreutils.desdbi as desdbi
import processingfw.pfwdefs as pfwdefs
import coreutils.miscutils as coremisc
import processingfw.pfwutils as pfwutils

#from errors import DuplicateDBFiletypeError
#from errors import DuplicateDBHeaderError
#from errors import IdMetadataHeaderError
#from errors import FileMetadataIngestError
#from errors import RequiredMetadataMissingError
#from errors import DBMetadataNotFoundError

PFW_MSG_ERROR = 3
PFW_MSG_WARN = 2
PFW_MSG_INFO = 1

class PFWDB (desdbi.DesDbi):
    """
        Extend coreutils.DesDbi to add database access methods

        Add methods to retrieve the metadata headers required for one or more
        filetypes and to ingest metadata associated with those headers.
    """

    def __init__ (self, *args, **kwargs):
        coremisc.fwdebug(3, 'PFWDB_DEBUG', args)
        try:
            desdbi.DesDbi.__init__ (self, *args, **kwargs)
        except Exception as err:
            coremisc.fwdie("Error: problem connecting to database: %s\n\tCheck desservices file and environment variables" % err, pfwdefs.PF_EXIT_FAILURE)
            

    def get_database_defaults(self):
        """ Grab default configuration information stored in database """

        result = OrderedDict()
        
        result['archive'] = self.get_archive_info()
        result['archive_transfer'] = self.get_archive_transfer_info()
        result['job_file_mvmt'] = self.get_job_file_mvmt_info()

        result[pfwdefs.DIRPATSECT] = self.get_database_table('OPS_DIRECTORY_PATTERN', 'NAME')
        result[pfwdefs.SW_FILEPATSECT] = self.get_filename_pattern()

        result['site'] = self.get_site_info()
        result[pfwdefs.SW_EXEC_DEF] = self.get_database_table('OPS_EXEC_DEF', 'NAME')

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
        allparams['reqnum'] = config[pfwdefs.REQNUM]
        allparams['unitname'] =  config.search(pfwdefs.UNITNAME, {'interpolate': True})[1]
        allparams['project'] = config.search('project', {'interpolate': True})[1]
        allparams['jiraid'] = config.search('jira_id', {'interpolate': True})[1]
        allparams['pipeline'] = config.search('pipeline', {'interpolate': True})[1]
        allparams['operator'] =  config.search('operator', {'interpolate': True})[1]
        allparams['numexpblk'] = len(coremisc.fwsplit(config[pfwdefs.SW_BLOCKLIST]))
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
                coremisc.fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_request table\n")
                sql =  "insert into pfw_request (reqnum, project, jira_id, pipeline) " 
                sql += "select %s, %s, %s, %s %s where not exists (select null from pfw_request where reqnum=%s)" % \
                       (namebinds['reqnum'], namebinds['project'], namebinds['jiraid'], namebinds['pipeline'], 
                       from_dual, namebinds['reqnum'])
                coremisc.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)

                params = {}
                for k in ['reqnum', 'project', 'jiraid', 'pipeline']:
                    params[k]=allparams[k]
                coremisc.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % params)
                curs.execute(sql, params)

                # pfw_unit
                coremisc.fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_unit table\n")
                curs = self.cursor()
                sql = "insert into pfw_unit (reqnum, unitname) select %s, %s %s where not exists (select null from pfw_unit where reqnum=%s and unitname=%s)" % (namebinds['reqnum'], namebinds['unitname'], from_dual, namebinds['reqnum'], namebinds['unitname'])
                coremisc.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)
                params = {}
                for k in ['reqnum', 'unitname']:
                    params[k]=allparams[k]
                coremisc.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % params)
                curs.execute(sql, params)

                # pfw_attempt
                coremisc.fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_attempt table\n")
                ## get current max attnum and try next value
                sql = "select max(attnum) from pfw_attempt where reqnum=%s and unitname=%s" % (namebinds['reqnum'], namebinds['unitname'])
                coremisc.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)
                params = {}
                for k in ['reqnum', 'unitname']:
                    params[k]=allparams[k]
                coremisc.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % params)
                curs.execute(sql, params)
                maxarr = curs.fetchall()
                if len(maxarr) == 0:
                    maxatt = 0
                elif maxarr[0][0] == None:
                    maxatt = 0
                else:
                    maxatt = int(maxarr[0][0])

                coremisc.fwdebug(3, 'PFWDB_DEBUG', "maxatt = %s" % maxatt)
                allparams['attnum'] = maxatt + 1
                namebinds['attnum'] = self.get_named_bind_string('attnum')

                # execute will fail if extra params
                params = {}
                for k in ['reqnum', 'unitname', 'attnum', 'operator', 'numexpblk', 'basket', 'group_submit_id']:
                    params[k]=allparams[k]
                coremisc.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % params)

                sql = "insert into pfw_attempt (reqnum, unitname, attnum, operator, submittime, numexpblk, basket, group_submit_id) select %s, %s, %s, %s, %s, %s, %s, %s %s where not exists (select null from pfw_attempt where reqnum=%s and unitname=%s and attnum=%s)" % (namebinds['reqnum'], namebinds['unitname'], namebinds['attnum'], namebinds['operator'], self.get_current_timestamp_str(), namebinds['numexpblk'], namebinds['basket'], namebinds['group_submit_id'], from_dual, namebinds['reqnum'], namebinds['unitname'], namebinds['attnum'])
                coremisc.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)

                curs.execute(sql, params)

                config[pfwdefs.ATTNUM] = allparams['attnum']
                done = True
            except Exception:
                print "\n\n"
                print "sql> ", sql
                print "params> ", params
                print "namebinds> ", namebinds
                (type, value, traceback) = sys.exc_info()
                if loopcnt < maxtries:
                    coremisc.fwdebug(0, 'PFWDB_DEBUG', "Warning: %s" % value)
                    coremisc.fwdebug(0, 'PFWDB_DEBUG', "Retrying inserting run into database\n\n")
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
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "Inserting into pfw_attempt_label table\n")

        row = {}
        row['reqnum'] = config[pfwdefs.REQNUM]
        row['unitname'] =  config.search(pfwdefs.UNITNAME, {'interpolate': True})[1]
        row['attnum'] = config[pfwdefs.ATTNUM]

        if pfwdefs.SW_LABEL in config:
            labels = config.search(pfwdefs.SW_LABEL, {'interpolate': True})[1]
            labels = coremisc.fwsplit(labels,',')
            for label in labels:
                row['label'] = label
                self.insert_PFW_row('PFW_ATTEMPT_LABEL', row)


    def update_attempt_cid (self, config, condorid):
        """ update row in pfw_attempt with condorid """

        updatevals = {}
        updatevals['condorid'] = condorid

        wherevals = {}
        wherevals['reqnum'] = config[pfwdefs.REQNUM]
        wherevals['unitname'] = config[pfwdefs.UNITNAME]
        wherevals['attnum'] = config[pfwdefs.ATTNUM]

        self.update_PFW_row ('PFW_ATTEMPT', updatevals, wherevals)


    def update_attempt_beg (self, config):
        """ update row in pfw_attempt with beg of attempt info """


        updatevals = {}
        updatevals['starttime'] = self.get_current_timestamp_str()

        wherevals = {}
        wherevals['reqnum'] = config[pfwdefs.REQNUM]
        wherevals['unitname'] = config[pfwdefs.UNITNAME]
        wherevals['attnum'] = config[pfwdefs.ATTNUM]

        self.update_PFW_row ('PFW_ATTEMPT', updatevals, wherevals)


    def update_attempt_end (self, config, exitcode):
        """ update row in pfw_attempt with end of attempt info """

        self.update_attempt_end_vals(config[pfwdefs.REQNUM], config[pfwdefs.UNITNAME], config[pfwdefs.ATTNUM], exitcode)
        

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
        
        self.update_PFW_row ('PFW_ATTEMPT', updatevals, wherevals)

    #### ATTEMPT_TASK #####
    def insert_attempt_task(self, config, taskname):
        """ Insert an entry into the pfw_attempt_task table """
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_attempt_task table\n")

        row = {}
        row['reqnum'] = config[pfwdefs.REQNUM]
        row['unitname'] = config[pfwdefs.UNITNAME]
        row['attnum'] = config[pfwdefs.ATTNUM]
        row['tasknum'] = pfwutils.next_tasknum(config, 'attempt')
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
        wherevals['reqnum'] = config[pfwdefs.REQNUM]
        wherevals['unitname'] = config[pfwdefs.UNITNAME]
        wherevals['attnum'] = config[pfwdefs.ATTNUM]
        wherevals['tasknum'] = tasknum

        self.update_PFW_row ('PFW_ATTEMPT_TASK', updatevals, wherevals)


    ##### BLOCK #####
    def insert_block (self, config):
        """ Insert an entry into the pfw_block table """
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_block table\n")

        blknum = config.search(pfwdefs.PF_BLKNUM, {'interpolate': False})[1]
        if blknum == '1':  # attempt is starting
            self.update_attempt_beg(config)

        blkname = config.search('blockname', {'interpolate': True})[1]
        #self.start_timing(config, 'block', blkname, blknum=blknum)
            
        row = {}
        row['reqnum'] = config[pfwdefs.REQNUM]
        row['unitname'] = config[pfwdefs.UNITNAME]
        row['attnum'] = config[pfwdefs.ATTNUM]
        row['blknum'] = blknum
        row['name'] = config.search('blockname', {'interpolate': True})[1]
        row['target_site'] = config.search('target_site', {'interpolate': True})[1]
        row['modulelist'] = config.search(pfwdefs.SW_MODULELIST, {'interpolate': True})[1]
        row['starttime'] = self.get_current_timestamp_str()
        self.insert_PFW_row('PFW_BLOCK', row)

    def update_block_numexpjobs (self, config, numexpjobs):
        """ update numexpjobs in pfw_block """
        updatevals = {}
        updatevals['numexpjobs'] = numexpjobs

        wherevals = {}
        wherevals['reqnum'] = config[pfwdefs.REQNUM]
        wherevals['unitname'] = config[pfwdefs.UNITNAME]
        wherevals['attnum'] = config[pfwdefs.ATTNUM]
        wherevals['blknum'] = config.search(pfwdefs.PF_BLKNUM, {'interpolate': False})[1]

        self.update_PFW_row ('PFW_BLOCK', updatevals, wherevals)

        
    def update_block_end (self, config, exitcode):
        """ update row in pfw_block with end of block info"""
    
        #self.end_timing(config, 'block', blkname, exitcode, blknum=blknum)

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['reqnum'] = config[pfwdefs.REQNUM]
        wherevals['unitname'] = config[pfwdefs.UNITNAME]
        wherevals['attnum'] = config[pfwdefs.ATTNUM]
        wherevals['blknum'] = config.search(pfwdefs.PF_BLKNUM, {'interpolate': False})[1]

        self.update_PFW_row ('PFW_BLOCK', updatevals, wherevals)

    #### BLOCK_TASK #####
    def insert_block_task(self, config, taskname):
        """ Insert an entry into the pfw_blktask table """

        row = {}
        row['reqnum'] = config[pfwdefs.REQNUM]
        row['unitname'] = config[pfwdefs.UNITNAME]
        row['attnum'] = config[pfwdefs.ATTNUM]
        row['blknum'] = config.search(pfwdefs.PF_BLKNUM, {'interpolate': False})[1]
        row['tasknum'] = pfwutils.next_tasknum(config, 'block')
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
        wherevals['reqnum'] = config[pfwdefs.REQNUM]
        wherevals['unitname'] = config[pfwdefs.UNITNAME]
        wherevals['attnum'] = config[pfwdefs.ATTNUM]
        wherevals['blknum'] = config[pfwdefs.PF_BLKNUM]
        wherevals['tasknum'] = tasknum

        self.update_PFW_row ('PFW_BLOCK_TASK', updatevals, wherevals)



    ##### JOB #####
    def insert_job (self, wcl, jobnum):
        """ Insert an entry into the pfw_job table """
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_job table\n")

        row = {}
        row['reqnum'] = wcl[pfwdefs.REQNUM]
        row['unitname'] = wcl[pfwdefs.UNITNAME]
        row['attnum'] = wcl[pfwdefs.ATTNUM]
        row['blknum'] = wcl[pfwdefs.PF_BLKNUM]
        row['jobnum'] = jobnum
        row['expect_num_wrap'] = wcl['numexpwrap']
        row['pipeprod'] = wcl['pipeprod']
        row['pipever'] = wcl['pipever']
        row['runjob_task_id'] = self.create_task('runjob', 'pfw_job')

        if 'jobkeys' in wcl:
            row['jobkeys'] = wcl['jobkeys']
        self.insert_PFW_row('PFW_JOB', row)
        return row['runjob_task_id']
            

    def update_runjob_target_info (self, wcl, jobnum, submit_condor_id = None, target_batch_id = None, exechost=None):
        """ Save information about target job from pfwrunjob """

        updatevals = {}
        if exechost is not None:
            updatevals['target_exec_host'] = exechost

        if submit_condor_id is not None:
            updatevals['condor_job_id'] = float(submit_condor_id)

        if target_batch_id is not None:
            updatevals['target_job_id'] = target_batch_id

        wherevals = {}
        wherevals['reqnum'] = wcl[pfwdefs.REQNUM]
        wherevals['unitname'] = wcl[pfwdefs.UNITNAME]
        wherevals['attnum'] = wcl[pfwdefs.ATTNUM]
        wherevals['jobnum'] = wcl[pfwdefs.PF_JOBNUM]

        
        if len(updatevals) > 0:
            self.update_PFW_row ('PFW_JOB', updatevals, wherevals)



    def update_job_junktar (self, wcl, junktar=None):
        """ update row in pfw_job with junk tarball name """

        if junktar is not None: 
            coremisc.fwdebug(3, 'PFWDB_DEBUG', "Saving junktar (%s) to pfw_job" % junktar)
            updatevals = {}
            updatevals['junktar'] = junktar

            wherevals = {}
            wherevals['reqnum'] = wcl[pfwdefs.REQNUM]
            wherevals['unitname'] = wcl[pfwdefs.UNITNAME]
            wherevals['attnum'] = wcl[pfwdefs.ATTNUM]
            wherevals['jobnum'] = wcl[pfwdefs.PF_JOBNUM]

            self.update_PFW_row ('PFW_JOB', updatevals, wherevals)


    def update_job_end (self, wcl, exitcode):
        """ update row in pfw_job with end of job info"""

        #self.end_timing(wcl, 'job', '%4d' % int(wcl[pfwdefs.PF_JOBNUM]), exitcode, blknum=wcl[pfwdefs.PF_BLKNUM], jobnum=wcl[pfwdefs.PF_JOBNUM])

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['reqnum'] = wcl[pfwdefs.REQNUM]
        wherevals['unitname'] = wcl[pfwdefs.UNITNAME]
        wherevals['attnum'] = wcl[pfwdefs.ATTNUM]
        wherevals['jobnum'] = wcl[pfwdefs.PF_JOBNUM]

        self.update_PFW_row ('PFW_JOB', updatevals, wherevals)



    def update_job_info (self, wcl, jobnum, jobinfo):
        """ update row in pfw_job with information gathered post job from condor log """

        coremisc.fwdebug(1, 'PFWDB_DEBUG', "Updating job information post job (%s)" % jobnum)
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "jobinfo=%s"%jobinfo)

        wherevals = {}
        wherevals['reqnum'] = wcl[pfwdefs.REQNUM]
        wherevals['unitname'] = wcl[pfwdefs.UNITNAME]
        wherevals['attnum'] = wcl[pfwdefs.ATTNUM]
        wherevals['jobnum'] = jobnum
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "wherevals = %s" %(wherevals))

        if len(jobinfo) > 0:
            self.update_PFW_row ('PFW_JOB', jobinfo, wherevals)
        else:
            coremisc.fwdebug(3, 'PFWDB_DEBUG', "Found 0 values to update (%s)" % (wherevals))
            coremisc.fwdebug(6, 'PFWDB_DEBUG', "\tjobnum = %s, jobinfo = %s" % (jobnum, jobinfo))


    
    ### JOB_TASK
    def insert_job_task (self, wcl, taskname):
        """ Insert an entry into the pfw_job_task table """

        row = {}
        row['reqnum'] = wcl[pfwdefs.REQNUM]
        row['unitname'] = wcl[pfwdefs.UNITNAME]
        row['attnum'] = wcl[pfwdefs.ATTNUM]
        row['jobnum'] = wcl[pfwdefs.PF_JOBNUM]
        row['taskname'] = taskname
        row['tasknum'] = pfwutils.next_tasknum(wcl, 'job')
        row['starttime'] = self.get_current_timestamp_str()
        row['task_id'] = self.create_task(taskname, 'pfw_job_task')

        self.insert_PFW_row('PFW_JOB_TASK', row)
        self.begin_task(row['task_id'])
        #return row['tasknum']
        return row['task_id']


    def update_job_task_end (self, wcl, task_id, exitcode):
        """ update row in pfw_job_task with end of task info"""

        self.end_task(task_id, exitcode)

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = exitcode

        wherevals = {}
        #wherevals['reqnum'] = wcl[pfwdefs.REQNUM]
        #wherevals['unitname'] = wcl[pfwdefs.UNITNAME]
        #wherevals['attnum'] = wcl[pfwdefs.ATTNUM]
        #wherevals['jobnum'] = wcl[pfwdefs.PF_JOBNUM]
        #wherevals['tasknum'] = tasknum
        wherevals['task_id'] = task_id

        self.update_PFW_row ('PFW_JOB_TASK', updatevals, wherevals)


    ##### MSG #####
    def insert_message(self, wcl, parent, msglevel, msg, blknum=None, jobnum=None, wrapnum=None, tasknum=None):
        """ Insert an entry into the pfw_message table """

        row = {}
        row['reqnum'] = wcl[pfwdefs.REQNUM]
        row['unitname'] = wcl[pfwdefs.UNITNAME]
        row['attnum'] = wcl[pfwdefs.ATTNUM]
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
        
        #self.start_timing(wcl, 'wrapper', '%4d_%s' % (int(wcl[pfwdefs.PF_WRAPNUM]), wcl['modname']), blknum=wcl[pfwdefs.PF_BLKNUM], jobnum=wcl[pfwdefs.PF_JOBNUM], wrapnum=wcl['wrapnum'])

        row = {}
        row['reqnum'] = wcl[pfwdefs.REQNUM]
        row['unitname'] = wcl[pfwdefs.UNITNAME]
        row['attnum'] = wcl[pfwdefs.ATTNUM]
        row['wrapnum'] = wcl[pfwdefs.PF_WRAPNUM]
        row['modname'] = wcl['modname']
        row['name'] = wcl['wrapper']['wrappername']
        row['id'] = wrapid
        row['blknum'] = wcl[pfwdefs.PF_BLKNUM]
        row['jobnum'] = wcl[pfwdefs.PF_JOBNUM]
        row['inputwcl'] = os.path.split(iwfilename)[-1]
        row['starttime'] = self.get_current_timestamp_str()

        self.insert_PFW_row('PFW_WRAPPER', row)
        return wrapid


    def update_wrapper_end (self, wcl, owclfile, logfile, exitcode):
        """ update row in pfw_wrapper with end of wrapper info """

        #self.end_timing(wcl, 'wrapper', '%4d_%s' % (int(wcl[pfwdefs.PF_WRAPNUM]), wcl['modname']), exitcode, blknum=wcl[pfwdefs.PF_BLKNUM], jobnum=wcl[pfwdefs.PF_JOBNUM], wrapnum=wcl['wrapnum'])

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        if owclfile is not None:
            updatevals['outputwcl'] = os.path.split(owclfile)[-1]
        if logfile is not None:
            updatevals['log'] = os.path.split(logfile)[-1]
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['id'] = wcl['wrapperid']

        self.update_PFW_row ('PFW_WRAPPER', updatevals, wherevals)

        
    ##### WRAPPER #####
    def insert_job_wrapper_task (self, wcl, taskname):
        """ insert row into pfw_job_wrapper_task """

        if pfwdefs.PF_WRAPNUM not in wcl:
            print wcl.keys()
            raise Exception("Error: Cannot find %s" % pfwdefs.PF_WRAPNUM)
        
        row = {}
        row['reqnum'] = wcl[pfwdefs.REQNUM]
        row['unitname'] = wcl[pfwdefs.UNITNAME]
        row['attnum'] = wcl[pfwdefs.ATTNUM]
        row['jobnum'] = wcl[pfwdefs.PF_JOBNUM]
        row['wrapnum'] = wcl[pfwdefs.PF_WRAPNUM]
        row['taskname'] = taskname
        row['tasknum'] = pfwutils.next_tasknum(wcl, 'job_wrapper')
        row['starttime'] = self.get_current_timestamp_str()
        row['task_id'] =  self.create_task(taskname, 'pfw_job_wrapper_task')

        self.begin_task(row['task_id'])
        self.insert_PFW_row('PFW_JOB_WRAPPER_TASK', row)

        return row['task_id']


    def update_job_wrapper_task_end (self, wcl, task_id, exitcode):
        """ update row in pfw_job_wrapper_task with end of task info """

        self.end_task(task_id, exitcode)

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = exitcode

        wherevals = {}
        #wherevals['reqnum'] = wcl[pfwdefs.REQNUM]
        #wherevals['unitname'] = wcl[pfwdefs.UNITNAME]
        #wherevals['attnum'] = wcl[pfwdefs.ATTNUM]
        #wherevals['jobnum'] = wcl[pfwdefs.PF_JOBNUM]
        #wherevals['wrapnum'] = wcl[pfwdefs.PF_WRAPNUM]
        #wherevals['tasknum'] = tasknum
        wherevals['task_id'] = task_id

        self.update_PFW_row ('PFW_JOB_WRAPPER_TASK', updatevals, wherevals)


    ##### PFW_EXEC
    def insert_exec (self, wcl, sect):
        """ insert row into pfw_exec """

        coremisc.fwdebug(3, 'PFWDB_DEBUG', sect)
        coremisc.fwdebug(3, 'PFWDB_DEBUG', wcl[sect])

        execid = self.get_seq_next_value('pfw_exec_seq')

        row = {}
        row['reqnum'] = wcl[pfwdefs.REQNUM]
        row['unitname'] = wcl[pfwdefs.UNITNAME]
        row['attnum'] = wcl[pfwdefs.ATTNUM]
        row['wrapnum'] = wcl[pfwdefs.PF_WRAPNUM]
        row['id'] = execid
        row['execnum'] = wcl[sect]['execnum']
        row['name'] = wcl[sect]['execname']
        if 'version' in wcl[sect] and wcl[sect]['version'] is not None:
            row['version'] = wcl[sect]['version']

        self.insert_PFW_row('PFW_EXEC', row)
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "end")
        return execid

    def update_exec_version (self, execid, version):
        """ update row in pfw_exec with exec version info """
        coremisc.fwdebug(3, 'PFWDB_DEBUG', execid)

        updatevals = {}
        updatevals['version'] = version

        wherevals = {}
        wherevals['id'] = execid

        self.update_PFW_row ('PFW_EXEC', updatevals, wherevals)


    def update_exec_end (self, execwcl, execid, exitcode):
        """ update row in pfw_exec with end of exec info """
        coremisc.fwdebug(3, 'PFWDB_DEBUG', execid)

        updatevals = {}
        updatevals['cmdargs'] = execwcl['cmdlineargs']
        updatevals['walltime'] = execwcl['walltime']
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['id'] = execid

        self.update_PFW_row ('PFW_EXEC', updatevals, wherevals)


    ##### PFW_JOB_EXEC_TASK
    def insert_job_exec_task (self, wcl, execnum, taskname):
        """ insert row into pfw_job_exec_task """

        row = {}
        row['reqnum'] = wcl[pfwdefs.REQNUM]
        row['unitname'] = wcl[pfwdefs.UNITNAME]
        row['attnum'] = wcl[pfwdefs.ATTNUM]
        row['wrapnum'] = wcl[pfwdefs.PF_WRAPNUM]
        row['execnum'] = execnum
        row['taskname'] = taskname
        row['tasknum'] = pfwutils.next_tasknum(wcl, 'job_exec')
        row['starttime'] = self.get_current_timestamp_str()

        self.insert_PFW_row('PFW_JOB_EXEC_TASK', row)
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "end")
        return row['tasknum']

    ##### 
    def insert_task (self, wcl, tasktype, taskname, **kwargs):
        """ call correct insert task function """
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "BEG (%s, %s)" % (tasktype, taskname))

        tasknum = -1
        if tasktype == 'job':
            tasknum = self.insert_job_task(wcl, taskname)
        elif tasktype == 'job_wrapper':
            tasknum = self.insert_job_wrapper_task(wcl, taskname)
        elif tasktype == 'job_exec': 
            tasknum = self.insert_job_exec_task(wcl, kwargs['execnum'], taskname)
        else:
            coremisc.fwdie("Error: invalid tasktype (%s)" % (tasktype), pfwdefs.PF_EXIT_FAILURE, 2)

        coremisc.fwdebug(3, 'PFWDB_DEBUG', "end")
        return tasknum


    def update_job_exec_task_end (self, wcl, execnum, tasknum, exitcode):
        """ update row in pfw_job_exec_task with end of task info """

        if tasknum > 0:
            updatevals = {}
            updatevals['endtime'] = self.get_current_timestamp_str()
            updatevals['status'] = exitcode

            wherevals = {}
            wherevals['reqnum'] = wcl[pfwdefs.REQNUM]
            wherevals['unitname'] = wcl[pfwdefs.UNITNAME]
            wherevals['attnum'] = wcl[pfwdefs.ATTNUM]
            wherevals['wrapnum'] = wcl[pfwdefs.PF_WRAPNUM]
            wherevals['execnum'] = execnum
            wherevals['tasknum'] = tasknum

            self.update_PFW_row ('PFW_JOB_EXEC_TASK', updatevals, wherevals)


    def update_task_end(self, wcl, tasktype, tasknum, status, **kwargs):
        """ call correct update task function """

        if tasktype == 'job':
            self.update_job_task_end(wcl, tasknum, status)
        elif tasktype == 'job_wrapper':
            self.update_job_wrapper_task_end(wcl, tasknum, status)
        elif tasktype == 'job_exec': 
            self.update_job_exec_task_end(wcl, kwargs['execnum'], tasknum, status)
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "end")

    
    #####
    def insert_data_query (self, wcl, modname, datatype, dataname, execname, cmdargs, version):
        """ insert row into pfw_data_query table """
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "BEG")

        queryid = self.get_seq_next_value('pfw_wrapper_seq')

        row = {}
        row['reqnum'] = wcl[pfwdefs.REQNUM] 
        row['unitname'] = wcl[pfwdefs.UNITNAME] 
        row['attnum'] = wcl[pfwdefs.ATTNUM]
        row['blknum'] = wcl[pfwdefs.PF_BLKNUM] 
        row['modname'] =  modname
        row['datatype'] = datatype   # file, list
        row['dataname'] = dataname
        row['id'] = queryid
        row['execname'] = os.path.basename(execname)
        row['cmdargs'] = cmdargs
        row['version'] = version
        row['starttime'] = self.get_current_timestamp_str()
        self.insert_PFW_row('PFW_DATA_QUERY', row) 
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "END")
        return queryid


    def update_data_query_end (self, queryid, exitcode):
        """ update row in pfw_data_query_end with end of query info """

        updatevals = {}
        updatevals['endtime'] = self.get_current_timestamp_str()
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['id'] = queryid

        self.update_PFW_row ('PFW_DATA_QUERY', updatevals, wherevals)



    ##########
    def insert_PFW_row (self, pfwtable, row):
        """ Insert a row into a PFW table and commit """

        self.basic_insert_row(pfwtable, row)
        self.commit()
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "end")

            

    ##########
    def update_PFW_row (self, pfwtable, updatevals, wherevals):
        """ Update a row in a PFW table and commit """

        self.basic_update_row(pfwtable, updatevals, wherevals)
        self.commit()


    ##########
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
        TASK_ID = "TASK_ID"
        OPM_ARTIFACT_ID = "OPM_ARTIFACT_ID"
        PARENT_OPM_ARTIFACT_ID = "PARENT_OPM_ARTIFACT_ID"
        CHILD_OPM_ARTIFACT_ID  = "CHILD_OPM_ARTIFACT_ID"
        USED_TABLE = "OPM_USED"
        WGB_TABLE  = "OPM_WAS_GENERATED_BY"
        WDF_TABLE  = "OPM_WAS_DERIVED_FROM"
        COLMAP_USED_WGB = [TASK_ID,OPM_ARTIFACT_ID]
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
            execSQL = insertSQL % (USED_TABLE, TASK_ID + "," + OPM_ARTIFACT_ID, 
                bindStr, bindStr,self.from_dual(), USED_TABLE, TASK_ID, bindStr, 
                OPM_ARTIFACT_ID, bindStr)
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
            execSQL = insertSQL % (WGB_TABLE, TASK_ID + "," + OPM_ARTIFACT_ID, 
                bindStr, bindStr, self.from_dual(), WGB_TABLE, TASK_ID, bindStr,
                OPM_ARTIFACT_ID, bindStr)
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
            execSQL = insertSQL % (WDF_TABLE, PARENT_OPM_ARTIFACT_ID + "," + 
                CHILD_OPM_ARTIFACT_ID, bindStr, bindStr, self.from_dual(), 
                WDF_TABLE, PARENT_OPM_ARTIFACT_ID, bindStr, CHILD_OPM_ARTIFACT_ID,
                bindStr)
            cursor.executemany(execSQL, data)
            self.commit()
    #end_ingest_provenance


    def get_job_info(self, wherevals):
        whclause = []
        for c,v in wherevals.items():
            whclause.append("%s=%s" % (c, self.get_named_bind_string(c)))

        sql = "select * from pfw_job where %s" % (' and '.join(whclause))

        coremisc.fwdebug(3, 'PFWDB_DEBUG', "sql> %s" % sql)
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "params> %s" % wherevals)
        
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

        coremisc.fwdebug(3, 'PFWDB_DEBUG', "sql> %s" % sql)
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "params> %s" % wherevals)
        
        curs = self.cursor()
        curs.execute(sql, wherevals)

        for row in curs:
            filedict[row[0]] = True


        # search for logs 
        # (not all logs show up in wgb, example ingestions which don't have output file)
        sql = "select log from pfw_wrapper where log is not NULL and %s" % (' and '.join(whclause))

        coremisc.fwdebug(3, 'PFWDB_DEBUG', "sql> %s" % sql)
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "params> %s" % wherevals)
        
        curs = self.cursor()
        curs.execute(sql, wherevals)

        for row in curs:
            filedict[row[0]] = True

        # search for junk tarball
        sql = "select junktar from pfw_job where junktar is not NULL and %s" % (' and '.join(whclause))

        coremisc.fwdebug(3, 'PFWDB_DEBUG', "sql> %s" % sql)
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "params> %s" % wherevals)
        
        curs = self.cursor()
        curs.execute(sql, wherevals)

        for row in curs:
            filedict[row[0]] = True

        
        # convert dictionary to list
        filelist = filedict.keys()
        coremisc.fwdebug(3, 'PFWDB_DEBUG', "filelist = %s" % filelist)

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
