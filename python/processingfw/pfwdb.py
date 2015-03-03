# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
    Define a database utility class extending despydmdb.desdmdbi

    Developed at: 
    The National Center for Supercomputing Applications (NCSA).
  
    Copyright (C) 2012 Board of Trustees of the University of Illinois. 
    All rights reserved.
"""

__version__ = "$Rev$"

import os
import sys
import traceback
import datetime
from collections import OrderedDict

import despydmdb.desdmdbi as desdmdbi
import processingfw.pfwdefs as pfwdefs
import despymisc.miscutils as miscutils
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

class PFWDB (desdmdbi.DesDmDbi):
    """
        Extend despydmdb.desdmdbi to add database access methods

        Add methods to retrieve the metadata headers required for one or more
        filetypes and to ingest metadata associated with those headers.
    """

    def __init__ (self, *args, **kwargs):
        miscutils.fwdebug(3, 'PFWDB_DEBUG', args)
        try:
            desdmdbi.DesDmDbi.__init__ (self, *args, **kwargs)
        except Exception as err:
            miscutils.fwdie("Error: problem connecting to database: %s\n\tCheck desservices file and environment variables" % err, pfwdefs.PF_EXIT_FAILURE)
            

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
        """ Insert entries into the pfw_request, pfw_unit, pfw_attempt tables for a single run submission
            Saves task id in config """
        maxtries = 1
        from_dual = self.from_dual()


        allparams = {}
        allparams['task_id'] =  self.create_task(name = 'attempt', 
                                                 info_table = 'pfw_attempt', 
                                                 parent_task_id = None, 
                                                 root_task_id = None, 
                                                 label = None, 
                                                 i_am_root = True,
                                                 do_commit = False)
        allparams['reqnum'] = config[pfwdefs.REQNUM]
        allparams['unitname'] =  config.search(pfwdefs.UNITNAME, {'interpolate': True})[1]
        allparams['project'] = config.search('project', {'interpolate': True})[1]
        allparams['jiraid'] = config.search('jira_id', {'interpolate': True})[1]
        allparams['pipeline'] = config.search('pipeline', {'interpolate': True})[1]
        allparams['operator'] =  config.search('operator', {'interpolate': True})[1]
        allparams['numexpblk'] = len(miscutils.fwsplit(config[pfwdefs.SW_BLOCKLIST]))

        if 'DESDM_PIPEPROD' in os.environ:
            allparams['subpipeprod'] = os.environ['DESDM_PIPEPROD']
        else:
            allparams['subpipeprod'] = None

        if 'DESDM_PIPEVER' in os.environ:
            allparams['subpipever'] = os.environ['DESDM_PIPEVER']
        else:
            allparams['subpipever'] = None

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

        (exists, value) = config.search('campaign', {'interpolate': True})
        if exists:
            allparams['campaign'] = value
        else:
            allparams['campaign'] = None

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
                miscutils.fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_request table\n")
                sql =  "insert into pfw_request (reqnum, project, campaign, jira_id, pipeline) " 
                sql += "select %s, %s, %s, %s, %s %s where not exists (select null from pfw_request where reqnum=%s)" % \
                       (namebinds['reqnum'], namebinds['project'], namebinds['campaign'], namebinds['jiraid'], namebinds['pipeline'], 
                       from_dual, namebinds['reqnum'])
                miscutils.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)

                params = {}
                for k in ['reqnum', 'project', 'jiraid', 'pipeline', 'campaign']:
                    params[k]=allparams[k]
                miscutils.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % params)
                curs.execute(sql, params)

                # pfw_unit
                miscutils.fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_unit table\n")
                curs = self.cursor()
                sql = "insert into pfw_unit (reqnum, unitname) select %s, %s %s where not exists (select null from pfw_unit where reqnum=%s and unitname=%s)" % (namebinds['reqnum'], namebinds['unitname'], from_dual, namebinds['reqnum'], namebinds['unitname'])
                miscutils.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)
                params = {}
                for k in ['reqnum', 'unitname']:
                    params[k]=allparams[k]
                miscutils.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % params)
                curs.execute(sql, params)

                # pfw_attempt
                miscutils.fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_attempt table\n")
                ## get current max attnum and try next value
                sql = "select max(attnum) from pfw_attempt where reqnum=%s and unitname=%s" % (namebinds['reqnum'], namebinds['unitname'])
                miscutils.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)
                params = {}
                for k in ['reqnum', 'unitname']:
                    params[k]=allparams[k]
                miscutils.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % params)
                curs.execute(sql, params)
                maxarr = curs.fetchall()
                if len(maxarr) == 0:
                    maxatt = 0
                elif maxarr[0][0] == None:
                    maxatt = 0
                else:
                    maxatt = int(maxarr[0][0])

                miscutils.fwdebug(3, 'PFWDB_DEBUG', "maxatt = %s" % maxatt)
                allparams['attnum'] = maxatt + 1
                namebinds['attnum'] = self.get_named_bind_string('attnum')

                # execute will fail if extra params
                params = {}
                needed_vals = ['reqnum', 'unitname', 'attnum', 'operator', 
                               'numexpblk', 'basket', 'group_submit_id', 
                               'task_id', 'subpipeprod', 'subpipever']
                for k in needed_vals:
                    params[k]=allparams[k]
                miscutils.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % params)

                #sql = "insert into pfw_attempt (reqnum, unitname, attnum, operator, submittime, numexpblk, basket, group_submit_id, task_id, subpipeprod, subpipever) select %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s %s where not exists (select null from pfw_attempt where reqnum=%s and unitname=%s and attnum=%s)" % (namebinds['reqnum'], namebinds['unitname'], namebinds['attnum'], namebinds['operator'], self.get_current_timestamp_str(), namebinds['numexpblk'], namebinds['basket'], namebinds['group_submit_id'], namebinds['task_id'], namebinds['subpipeprod'], namebinds['subpipever'], from_dual, namebinds['reqnum'], namebinds['unitname'], namebinds['attnum'])
                subsql = "select null from pfw_attempt where reqnum=%s and unitname=%s and attnum=%s" % \
                          (namebinds['reqnum'], namebinds['unitname'], namebinds['attnum'])
                sql = "insert into pfw_attempt (%s, submittime) select %s, %s %s where not exists (%s)" % \
                       (','.join(needed_vals),
                        ','.join(namebinds[x] for x in needed_vals),
                        self.get_current_timestamp_str(), 
                        from_dual,
                        subsql)
                miscutils.fwdebug(3, 'PFWDB_DEBUG', "\t%s\n" % sql)

                curs.execute(sql, params)

                config[pfwdefs.ATTNUM] = allparams['attnum']
                config['task_id'] = {'attempt': allparams['task_id'], 'block': {}, 'job': {}}
                done = True
            except Exception:
                print "\n\n"
                print "sql> ", sql
                print "params> ", params
                print "namebinds> ", namebinds
                (type, value, traceback) = sys.exc_info()
                if loopcnt < maxtries:
                    miscutils.fwdebug(0, 'PFWDB_DEBUG', "Warning: %s" % value)
                    miscutils.fwdebug(0, 'PFWDB_DEBUG', "Retrying inserting run into database\n\n")
                    loopcnt = loopcnt + 1
                    self.rollback()
                    continue
                else:
                    raise

        if not done:
            raise Exception("Exceeded max tries for inserting into pfw_attempt table")


        curs.close()
        self.commit()  


    def insert_attempt_label(self, config):
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "Inserting into pfw_attempt_label table\n")

        row = {}
        row['reqnum'] = config[pfwdefs.REQNUM]
        row['unitname'] =  config.search(pfwdefs.UNITNAME, {'interpolate': True})[1]
        row['attnum'] = config[pfwdefs.ATTNUM]

        if pfwdefs.SW_LABEL in config:
            labels = config.search(pfwdefs.SW_LABEL, {'interpolate': True})[1]
            labels = miscutils.fwsplit(labels,',')
            for label in labels:
                row['label'] = label
                self.insert_PFW_row('PFW_ATTEMPT_LABEL', row)


    def insert_attempt_val(self, config):
        """ Insert key/val pairs of information about an attempt into the pfw_attempt_val table """
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "Inserting into pfw_attempt_val table\n")

        row = {}
        row['reqnum'] = config[pfwdefs.REQNUM]
        row['unitname'] =  config.search(pfwdefs.UNITNAME, {'interpolate': True})[1]
        row['attnum'] = config[pfwdefs.ATTNUM]

        if pfwdefs.SW_SAVE_RUN_VALS in config:
            keys2save = config.search(pfwdefs.SW_SAVE_RUN_VALS, {'interpolate': True})[1]
            keys = miscutils.fwsplit(keys2save,',')
            for key in keys:
                row['key'] = key
                val = config.search(key, {'interpolate': True, 'expand': True})[1]
                if isinstance(val, list):
                    for v in val:
                        row['val'] = v
                        self.insert_PFW_row('PFW_ATTEMPT_VAL', row)
                else:
                    row['val'] = val
                    self.insert_PFW_row('PFW_ATTEMPT_VAL', row)


    def update_attempt_archive_path(self, config):
        """ update row in pfw_attempt with relative path in archive """

        updatevals = {}
        updatevals['archive_path'] = config.search(pfwdefs.ATTEMPT_ARCHIVE_PATH, {'interpolate': True})[1]

        wherevals = {}
        wherevals['task_id'] = config['task_id']['attempt']
        
        self.update_PFW_row ('PFW_ATTEMPT', updatevals, wherevals)



    def update_attempt_cid (self, config, condorid):
        """ update row in pfw_attempt with condorid """

        updatevals = {}
        updatevals['condorid'] = condorid

        wherevals = {}
        wherevals['reqnum'] = config[pfwdefs.REQNUM]
        wherevals['unitname'] = config[pfwdefs.UNITNAME]
        wherevals['attnum'] = config[pfwdefs.ATTNUM]

        self.update_PFW_row ('PFW_ATTEMPT', updatevals, wherevals)


    #def update_attempt_beg (self, config):
    #    """ update row in pfw_attempt with beg of attempt info """
    #
    #
    #    updatevals = {}
    #    updatevals['starttime'] = self.get_current_timestamp_str()
    #
    #    wherevals = {}
    #    wherevals['reqnum'] = config[pfwdefs.REQNUM]
    #    wherevals['unitname'] = config[pfwdefs.UNITNAME]
    #    wherevals['attnum'] = config[pfwdefs.ATTNUM]
    #
    #    self.update_PFW_row ('PFW_ATTEMPT', updatevals, wherevals)


    #def update_attempt_end (self, config, exitcode):
    #    """ update row in pfw_attempt with end of attempt info """
    #
    #    self.update_attempt_end_vals(config[pfwdefs.REQNUM], config[pfwdefs.UNITNAME], 
    #                                 config[pfwdefs.ATTNUM], exitcode)
        

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


    ##### BLOCK #####
    def insert_block (self, config):
        """ Insert an entry into the pfw_block table """
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_block table\n")

        blknum = config.search(pfwdefs.PF_BLKNUM, {'interpolate': False})[1]
        if blknum == '1':  # attempt is starting
            #self.update_attempt_beg(config)
            self.begin_task(config['task_id']['attempt'], True)

        blkname = config.search('blockname', {'interpolate': True})[1]
            
        row = {}
        row['reqnum'] = config[pfwdefs.REQNUM]
        row['unitname'] = config[pfwdefs.UNITNAME]
        row['attnum'] = config[pfwdefs.ATTNUM]
        row['blknum'] = blknum
        row['name'] = config.search('blockname', {'interpolate': True})[1]
        row['target_site'] = config.search('target_site', {'interpolate': True})[1]
        row['modulelist'] = config.search(pfwdefs.SW_MODULELIST, {'interpolate': True})[1]
        row['task_id'] =  self.create_task(name = 'block', 
                                           info_table = 'pfw_block', 
                                           parent_task_id = int(config['task_id']['attempt']), 
                                           root_task_id = int(config['task_id']['attempt']), 
                                           label = None, 
                                           do_commit = False)
        self.begin_task(row['task_id'])
        self.insert_PFW_row('PFW_BLOCK', row)

        config['task_id']['block'][str(row['blknum'])] = row['task_id']
        print config['task_id']


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

        
    ##### JOB #####
    def insert_job (self, wcl, jobdict):
        """ Insert an entry into the pfw_job table """
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "Inserting to pfw_job table\n")

        row = {}
        row['reqnum'] = wcl[pfwdefs.REQNUM]
        row['unitname'] = wcl[pfwdefs.UNITNAME]
        row['attnum'] = wcl[pfwdefs.ATTNUM]
        row['blknum'] = wcl[pfwdefs.PF_BLKNUM]
        row['jobnum'] = int(jobdict['jobnum'])
        row['expect_num_wrap'] = jobdict['numexpwrap']
        row['pipeprod'] = wcl['pipeprod']
        row['pipever'] = wcl['pipever']
        row['task_id'] =  self.create_task(name = 'job', 
                                           info_table = 'pfw_job', 
                                           parent_task_id = wcl['task_id']['block'][row['blknum']], 
                                           root_task_id = int(wcl['task_id']['attempt']), 
                                           label = None, 
                                           do_commit = False)
        wcl['task_id']['job'][jobdict['jobnum']] = row['task_id']

        if 'jobkeys' in jobdict:
            row['jobkeys'] = jobdict['jobkeys']
        self.insert_PFW_row('PFW_JOB', row)
            

    def update_job_target_info (self, wcl, submit_condor_id = None, 
                                target_batch_id = None, exechost=None):
        """ Save information about target job from pfwrunjob """

        jobnum = wcl[pfwdefs.PF_JOBNUM]

        params = {}
        setvals = []
        if submit_condor_id is not None:
            setvals.append('condor_job_id=%s' % self.get_named_bind_string('condor_job_id'))
            params['condor_job_id'] = float(submit_condor_id)

        if target_batch_id is not None:
            setvals.append('target_job_id=%s' % self.get_named_bind_string('target_job_id'))
            params['target_job_id'] = target_batch_id

        if len(setvals) > 0:
            params['task_id'] = wcl['task_id']['job'][jobnum] 

            sql = "update pfw_job set %s where task_id=%s and condor_job_id is NULL" % (','.join(setvals), self.get_named_bind_string('task_id'))

            miscutils.fwdebug(3, 'PFWDB_DEBUG', "sql> %s" % sql)
            miscutils.fwdebug(3, 'PFWDB_DEBUG', "params> %s" % params)
            curs = self.cursor()
            try:
                curs.execute(sql, params)
            except:
                (type, value, traceback) = sys.exc_info()
                print "******************************"
                print "Error:", type, value
                print "sql> %s\n" % (sql)
                print "params> %s\n" % params
                raise

            if curs.rowcount == 0:
                self.insert_message(wcl['task_id']['job'][jobnum], PFW_MSG_ERROR, 
                                    "Job attempted to run more than once")

                print "******************************"
                print "Error:  This job has already been run before."
                print "reqnum = ", wcl[pfwdefs.REQNUM]
                print "unitname = ", wcl[pfwdefs.UNITNAME]
                print "attnum = ", wcl[pfwdefs.ATTNUM]
                print "blknum = ", wcl[pfwdefs.PF_BLKNUM]
                print "jobnum = ", jobnum
                print "job task_id = ", wcl['task_id']['job'][jobnum]

                print "\nThe 1st job information:"
                curs2 = self.cursor()
                sql = "select * from pfw_job, task where pfw_job.task_id=task.id and reqnum=%s and unitname=%s and attnum=%s and jobnum=%s" % (self.get_named_bind_string('reqnum'), self.get_named_bind_string('unitname'), self.get_named_bind_string('attnum'), self.get_named_bind_string('jobnum'))
                curs2.execute(sql, {'reqnum': wcl[pfwdefs.REQNUM],
                                    'unitname': wcl[pfwdefs.UNITNAME],
                                    'attnum': wcl[pfwdefs.ATTNUM],
                                    'jobnum': jobnum})
                desc = [d[0].lower() for d in curs2.description]
                for row in curs2:
                    d = dict(zip(desc, row))
                    for k,v in d.items():
                        print k,v
                    print "\n"


    
                print "\nThe 2nd job information:"
                print "submit_condor_id = ", submit_condor_id
                print "target_batch_id = ", target_batch_id
                print "exechost = ", exechost
                print "current time = ", str(datetime.datetime.now())

                print "\nupdate statement information"
                print "sql> %s\n" % sql
                print "params> %s\n" % params


                raise Exception("Error: job attempted to run more than once")

        if exechost is not None:
            wherevals = {}
            wherevals['id'] = wcl['task_id']['job'][jobnum] 
            updatevals = {}
            updatevals['exec_host'] = exechost
            self.update_PFW_row ('TASK', updatevals, wherevals)


    def update_job_junktar (self, wcl, junktar=None):
        """ update row in pfw_job with junk tarball name """

        jobnum = wcl[pfwdefs.PF_JOBNUM]

        if junktar is not None: 
            miscutils.fwdebug(3, 'PFWDB_DEBUG', "Saving junktar (%s) to pfw_job" % junktar)
            updatevals = {}
            updatevals['junktar'] = junktar

            wherevals = {}
            wherevals['task_id'] = wcl['task_id']['job'][jobnum] 

            self.update_PFW_row ('PFW_JOB', updatevals, wherevals)


    def update_job_info (self, wcl, jobnum, jobinfo):
        """ update row in pfw_job with information gathered post job from condor log """

        miscutils.fwdebug(1, 'PFWDB_DEBUG', "Updating job information post job (%s)" % jobnum)
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "jobinfo=%s"%jobinfo)

        wherevals = {}
        wherevals['task_id'] = wcl['task_id']['job'][jobnum] 
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "wherevals = %s" %(wherevals))

        if len(jobinfo) > 0:
            self.update_PFW_row ('PFW_JOB', jobinfo, wherevals)
        else:
            miscutils.fwdebug(3, 'PFWDB_DEBUG', "Found 0 values to update (%s)" % (wherevals))
            miscutils.fwdebug(6, 'PFWDB_DEBUG', "\tjobnum = %s, jobinfo = %s" % (jobnum, jobinfo))


    #def update_tjob_info(self, wcl, jobnum, jobinfo, taskinfo):
    def update_tjob_info(self, wcl, jobnum, jobinfo):
        """ update a row in the task table because couldn't do so at run time """

        #wherevals = {}
        #wherevals['id'] = wcl['task_id']['job'][jobnum]
        #self.basic_update_row ('task', taskinfo, wherevals)

        wherevals = {}
        wherevals['task_id'] = wcl['task_id']['job'][jobnum]
        self.basic_update_row ('pfw_job', jobinfo, wherevals)
        self.commit()

    
    ##### MSG #####
    def insert_message(self, task_id, msglevel, msg):
        """ Insert an entry into the pfw_message table """

        row = {}
        row['task_id'] = task_id
        row['msgtime'] = self.get_current_timestamp_str()
        row['msglevel'] = msglevel 
        row['msg'] = msg 
        self.insert_PFW_row('PFW_MESSAGE', row)



    ##### WRAPPER #####
    def insert_wrapper (self, wcl, iwfilename, parent_tid):
        """ insert row into pfw_wrapper """

        row = {}
        row['reqnum'] = wcl[pfwdefs.REQNUM]
        row['unitname'] = wcl[pfwdefs.UNITNAME]
        row['attnum'] = wcl[pfwdefs.ATTNUM]
        row['wrapnum'] = wcl[pfwdefs.PF_WRAPNUM]
        row['modname'] = wcl['modname']
        row['name'] = wcl['wrapper']['wrappername']
        row['task_id'] = self.create_task(name = 'wrapper', 
                                          info_table = 'pfw_wrapper', 
                                          parent_task_id = parent_tid,
                                          root_task_id = int(wcl['task_id']['attempt']), 
                                          label = None, 
                                          do_commit = True)
        row['blknum'] = wcl[pfwdefs.PF_BLKNUM]
        row['jobnum'] = wcl[pfwdefs.PF_JOBNUM]
        row['inputwcl'] = os.path.split(iwfilename)[-1]

        self.insert_PFW_row('PFW_WRAPPER', row)
        return row['task_id']


    def update_wrapper_end (self, wcl, owclfile, logfile, exitcode):
        """ update row in pfw_wrapper with end of wrapper info """

        self.end_task(wcl['task_id']['wrapper'], exitcode, True)

        updatevals = {}
        if owclfile is not None:
            updatevals['outputwcl'] = os.path.split(owclfile)[-1]
        if logfile is not None:
            updatevals['log'] = os.path.split(logfile)[-1]

        wherevals = {}
        wherevals['task_id'] = wcl['task_id']['wrapper']

        self.update_PFW_row ('PFW_WRAPPER', updatevals, wherevals)

    ##### PFW_EXEC
    def insert_exec (self, wcl, sect):
        """ insert row into pfw_exec """

        miscutils.fwdebug(3, 'PFWDB_DEBUG', sect)
        miscutils.fwdebug(3, 'PFWDB_DEBUG', wcl[sect])

        row = {}
        row['reqnum'] = wcl[pfwdefs.REQNUM]
        row['unitname'] = wcl[pfwdefs.UNITNAME]
        row['attnum'] = wcl[pfwdefs.ATTNUM]
        row['wrapnum'] = wcl[pfwdefs.PF_WRAPNUM]
        row['execnum'] = wcl[sect]['execnum']
        row['name'] = wcl[sect]['execname']
        row['task_id'] =  self.create_task(name = sect, 
                                           info_table ='pfw_exec', 
                                           parent_task_id = wcl['task_id']['wrapper'], 
                                           root_task_id = int(wcl['task_id']['attempt']), 
                                           label=None, 
                                           do_commit=True)
        if 'version' in wcl[sect] and wcl[sect]['version'] is not None:
            row['version'] = wcl[sect]['version']

        self.insert_PFW_row('PFW_EXEC', row)
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "end")
        return row['task_id']

    def update_exec_version (self, taskid, version):
        """ update row in pfw_exec with exec version info """
        miscutils.fwdebug(3, 'PFWDB_DEBUG', taskid)

        updatevals = {}
        updatevals['version'] = version

        wherevals = {}
        wherevals['task_id'] = taskid

        self.update_PFW_row ('PFW_EXEC', updatevals, wherevals)


    def update_exec_end (self, execwcl, taskid, exitcode):
        """ update row in pfw_exec with end of exec info """
        miscutils.fwdebug(3, 'PFWDB_DEBUG', taskid)

        # update pfw_exec table
        updatevals = {}
        updatevals['cmdargs'] = execwcl['cmdlineargs']
        updatevals['walltime'] = execwcl['walltime']
        updatevals['status'] = exitcode

        wherevals = {}
        wherevals['task_id'] = taskid

        self.update_PFW_row ('PFW_EXEC', updatevals, wherevals)

        # update task table
        updatevals = {}
        updatevals['status'] = exitcode
        wherevals = {}
        wherevals['id'] = taskid
        self.basic_update_row ('TASK', updatevals, wherevals)
        self.commit()

    #####
    def insert_data_query (self, wcl, modname, datatype, dataname, execname, cmdargs, version):
        """ insert row into pfw_data_query table """
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "BEG")

        parent_tid = wcl['task_id']['begblock']

        row = {}
        row['reqnum'] = wcl[pfwdefs.REQNUM] 
        row['unitname'] = wcl[pfwdefs.UNITNAME] 
        row['attnum'] = wcl[pfwdefs.ATTNUM]
        row['blknum'] = wcl[pfwdefs.PF_BLKNUM] 
        row['modname'] =  modname
        row['datatype'] = datatype   # file, list
        row['dataname'] = dataname
        row['task_id'] = self.create_task(name = 'dataquery', 
                                          info_table = 'PFW_DATA_QUERY', 
                                          parent_task_id = parent_tid,
                                          root_task_id = int(wcl['task_id']['attempt']), 
                                          label = None, 
                                          do_begin = True,
                                          do_commit = True)
        row['execname'] = os.path.basename(execname)
        row['cmdargs'] = cmdargs
        row['version'] = version
        self.insert_PFW_row('PFW_DATA_QUERY', row) 
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "END")
        return row['task_id']


    ##########
    def insert_PFW_row (self, pfwtable, row):
        """ Insert a row into a PFW table and commit """

        self.basic_insert_row(pfwtable, row)
        self.commit()
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "end")

            
    ##########
    def update_PFW_row (self, pfwtable, updatevals, wherevals):
        """ Update a row in a PFW table and commit """

        self.basic_update_row(pfwtable, updatevals, wherevals)
        self.commit()


    def get_job_info(self, wherevals):
        whclause = []
        for c,v in wherevals.items():
            whclause.append("%s=%s" % (c, self.get_named_bind_string(c)))

        sql = "select j.*,t.* from pfw_job j, task t where t.id=j.task_id and %s" % (' and '.join(whclause))

        miscutils.fwdebug(3, 'PFWDB_DEBUG', "sql> %s" % sql)
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "params> %s" % wherevals)
        
        curs = self.cursor()
        curs.execute(sql, wherevals)
        desc = [d[0].lower() for d in curs.description]

        sql2 = "select * from pfw_message where task_id=%s" % self.get_named_bind_string('task_id')
        curs2 = self.cursor()
        curs2.prepare(sql2)
            
        jobinfo = {}
        for line in curs:
            d = dict(zip(desc, line))
            curs2.execute(None, {'task_id': d['task_id']})
            desc2 = [x[0].lower() for x in curs2.description]
            msglist = []
            for r in curs2:
                mdict = dict(zip(desc2,r))
                msglist.append(mdict)
            d['message'] = msglist
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

    def get_block_task_info(self, blktid):
        """ Return task information for tasks for given block """

        sql = "select * from task where parent_task_id=%s and (info_table is Null or info_table != %s)" % (self.get_named_bind_string('parent_task_id'), self.get_named_bind_string('info_table'))
        curs = self.cursor()
        curs.execute(sql, {'parent_task_id': blktid,
                           'info_table': 'pfw_job'})
        desc = [d[0].lower() for d in curs.description]
        info = {}
        for line in curs:
            d = dict(zip(desc, line))
            info[d['name']] = d
        return info


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

        miscutils.fwdebug(3, 'PFWDB_DEBUG', "sql> %s" % sql)
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "params> %s" % wherevals)
        
        curs = self.cursor()
        curs.execute(sql, wherevals)

        for row in curs:
            filedict[row[0]] = True


        # search for logs 
        # (not all logs show up in wgb, example ingestions which don't have output file)
        sql = "select log from pfw_wrapper where log is not NULL and %s" % (' and '.join(whclause))

        miscutils.fwdebug(3, 'PFWDB_DEBUG', "sql> %s" % sql)
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "params> %s" % wherevals)
        
        curs = self.cursor()
        curs.execute(sql, wherevals)

        for row in curs:
            filedict[row[0]] = True

        # search for junk tarball
        sql = "select junktar from pfw_job where junktar is not NULL and %s" % (' and '.join(whclause))

        miscutils.fwdebug(3, 'PFWDB_DEBUG', "sql> %s" % sql)
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "params> %s" % wherevals)
        
        curs = self.cursor()
        curs.execute(sql, wherevals)

        for row in curs:
            filedict[row[0]] = True

        
        # convert dictionary to list
        filelist = filedict.keys()
        miscutils.fwdebug(3, 'PFWDB_DEBUG', "filelist = %s" % filelist)

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
