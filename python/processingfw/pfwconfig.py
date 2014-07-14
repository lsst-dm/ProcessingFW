#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

""" Contains class definition that stores configuration and state information for PFW """

from collections import OrderedDict
import getpass
import sys
import copy
import re
import os
import time
import random

from processingfw.pfwdefs import *
from processingfw.pfwutils import *
from coreutils.miscutils import *
import intgutils.wclutils as wclutils
import processingfw.pfwdb as pfwdb


class PfwConfig:
    """ Contains configuration and state information for PFW """

    # order in which to search for values
    DEFORDER = [SW_FILESECT, SW_LISTSECT, 'exec', 'job', SW_MODULESECT, SW_BLOCKSECT, 'archive', 'site']

    ###########################################################################
    def __init__(self, args):
        """ Initialize configuration object, typically reading from wclfile """

        # data which needs to be kept across programs must go in self.config
        # data which needs to be searched also must go in self.config
        self.config = OrderedDict()

        wcldict = OrderedDict()
        if 'wclfile' in args:
            fwdebug(3, 'PFWCONFIG_DEBUG', "Reading wclfile: %s" % (args['wclfile']))
            try:
                starttime = time.time()
                print "\tReading submit wcl...",
                with open(args['wclfile'], "r") as fh:
                    wcldict = wclutils.read_wcl(fh, filename=args['wclfile'])
                print "DONE (%0.2f secs)" % (time.time()-starttime)
                wcldict['wclfile'] = args['wclfile']
            except Exception as err:
                fwdie("Error: Problem reading wcl file '%s' : %s" % (args['wclfile'], err), PF_EXIT_FAILURE)

        if 'submit_des_services' in args and args['submit_des_services'] is not None:
            wcldict['submit_des_services'] = args['submit_des_services']
        elif 'submit_des_services' not in wcldict:
            if 'DES_SERVICES' in os.environ:
                wcldict['submit_des_services'] = os.environ['DES_SERVICES']
            else:
                # let it default to $HOME/.desservices.init    
                wcldict['submit_des_services'] = None

        if 'submit_des_db_section' in args and args['submit_des_db_section'] is not None:
            wcldict['submit_des_db_section'] = args['submit_des_db_section']
        elif 'submit_des_db_section' not in wcldict:
            if 'DES_DB_SECTION' in os.environ:
                wcldict['submit_des_db_section'] = os.environ['DES_DB_SECTION']
            else:
                # let DB connection code print error message
                wcldict['submit_des_db_section'] = None

        # for values passed in on command line, set top-level config 
        for var in (PF_DRYRUN, PF_USE_DB_IN, PF_USE_DB_OUT, PF_USE_QCF):
            if var in args and args[var] is not None:
                wcldict[var] = args[var]

        if 'usePFWconfig' in args:
            pfwconfig = os.environ['PROCESSINGFW_DIR'] + '/etc/pfwconfig.des' 
            fwdebug(3, 'PFWCONFIG_DEBUG', "Reading pfwconfig: %s" % (pfwconfig))
            starttime = time.time()
            print "\tReading config from software install...",
            fh = open(pfwconfig, "r")
            wclutils.updateDict(self.config, wclutils.read_wcl(fh, filename=pfwconfig))
            fh.close()
            print "DONE (%0.2f secs)" % (time.time()-starttime)

        if (PF_USE_DB_IN in wcldict and 
            convertBool(wcldict[PF_USE_DB_IN]) and 
            'get_db_config' in args and args['get_db_config']):
            print "\tGetting defaults from DB...",
            sys.stdout.flush()
            starttime = time.time()
            dbh = pfwdb.PFWDB(wcldict['submit_des_services'], wcldict['submit_des_db_section'])
            print "DONE (%0.2f secs)" % (time.time()-starttime)
            wclutils.updateDict(self.config, dbh.get_database_defaults())

        # wclfile overrides all, so must be added last
        if 'wclfile' in args:
            fwdebug(3, 'PFWCONFIG_DEBUG', "Reading wclfile: %s" % (args['wclfile']))
            wclutils.updateDict(self.config, wcldict)


        self.set_names()

        # store the file name of the top-level submitwcl in dict:
        if 'submitwcl' not in self.config and \
           'wclfile' in args:
            self.config['submitwcl'] = args['wclfile']

        if 'processingfw_dir' not in self.config and \
           'PROCESSINGFW_DIR' in os.environ:
            self.config['processingfw_dir'] = os.environ['PROCESSINGFW_DIR']

        if 'current' not in self.config:
            self.config['current'] = OrderedDict({'curr_block': '', 
                                                  'curr_archive': '', 
                                                  'curr_software': '', 
                                                  'curr_site' : ''} )
            self.config[PF_WRAPNUM] = '0'
            self.config[PF_BLKNUM] = '1'
            self.config[PF_TASKNUM] = '0'
            self.config[PF_JOBNUM] = '0'


        if SW_BLOCKLIST in self.config:
            self.block_array = fwsplit(self.config[SW_BLOCKLIST])
            self.config['num_blocks'] = len(self.block_array)
            if self.config[PF_BLKNUM] <= self.config['num_blocks']:
                self.set_block_info()


    ###########################################################################
    def save_file(self, filename):
        """Saves configuration in WCL format"""
        fh = open(filename, "w")
        if 'submit_des_services' in self.config and self.config['submit_des_services'] == None:
            del self.config['submit_des_services']
        wclutils.write_wcl(self.config, fh, True, 4)  # save it sorted
        fh.close()


    ###########################################################################
    def __contains__(self, key, opts=None):
        """ D.__contains__(k) -> True if D has a key k, else False """
        (found, value) = self.search(key, opts)
        return found

    ###########################################################################
    def __getitem__(self, key, opts=None):
        """ x.__getitem__(y) <==> x[y] """
        (found, value) = self.search(key, opts)
        return value

    ###########################################################################
    def __setitem__(self, key, val):
        """ x.__setitem__(i, y) <==> x[i]=y """
        self.config[key] = val

    ###########################################################################
    def get(self, key, default = None, opt = None):
        (found, val) = self.search(key, opt)
        if not found:
            val = default
        return val


    ###########################################################################
    def set(self, key, val):
        """ store a value in wcl """
        subkeys = key.split('.')
        valkey = subkeys.pop()
        wcldict = self.config
        for k in subkeys:
            wcldict = wcldict[k]

        wcldict[valkey] = val


    ###########################################################################
    def search(self, key, opt=None):
        """ Searches for key using given opt following hierarchy rules """ 
        fwdebug(8, 'PFWCONFIG_DEBUG', "\tBEG")
        fwdebug(8, 'PFWCONFIG_DEBUG',
                 "\tinitial key = '%s'" % key)
        fwdebug(8, 'PFWCONFIG_DEBUG',
                 "\tinitial opts = '%s'" % opt)

        found = False
        value = ''
        if hasattr(key, 'lower'):
            key = key.lower()
        else:
            print "key = %s" % key

        # if key contains period, use it exactly instead of scoping rules
        if isinstance(key, str) and '.' in key:
            val = self.config
            found = True
            for k in key.split('.'):
                #print "get_wcl_value: k=", k
                if k in val:
                    val = val[k]
                else:
                    found = False
                    break
        else:
            # start with stored current values
            curvals = copy.deepcopy(self.config['current'])

            # override with current values passed into function if given
            if opt is not None and PF_CURRVALS in opt:
                for k,v in opt[PF_CURRVALS].items():
                    #print "using specified curval %s = %s" % (k,v)
                    curvals[k] = v
    
            #print "curvals = ", curvals
            if key in curvals:
                #print "found %s in curvals" % (key)
                found = True
                value = curvals[key]
            elif opt and 'searchobj' in opt and key in opt['searchobj']:
                found = True
                value = opt['searchobj'][key]
            else:
                for sect in self.DEFORDER:
                    #print "Searching section %s for key %s" % (sect, key)
                    if "curr_" + sect in curvals:
                        currkey = curvals['curr_'+sect]
                        #print "\tcurrkey for section %s = %s" % (sect, currkey)
                        if sect in self.config:
                            if currkey in self.config[sect]:
                                if key in self.config[sect][currkey]:
                                    found = True
                                    value = self.config[sect][currkey][key]
                                    break
    
            # lastly check global values
            if not found:
                #print "\t%s not found, checking global values" % (key)
                if key in self.config:
                    found = True
                    value = self.config[key]


        if not found and opt and 'required' in opt and opt['required']:
            print "\n\nError: search for %s failed" % (key)
            print "\tcurrent = ", self.config['current']
            print "\topt = ", opt
            print "\tcurvals = ", curvals
            print "\n\n"
            fwdie("Error: Search failed (%s)" % key, PF_EXIT_FAILURE, 2)
    
        if found and opt and 'interpolate' in opt and opt['interpolate']:
            opt['interpolate'] = False
            value = self.interpolate(value, opt) 

        fwdebug(8, 'PFWCONFIG_DEBUG', "\tEND")
        return (found, value)
    
    
    
    ###########################################################################
    # assumes already run through chk
    def set_submit_info(self):
        """ Initialize submit time values """
        self.config['des_home'] = os.path.abspath(os.path.dirname(__file__)) + "/.."
        self.config['submit_dir'] = os.getcwd()
        self.config['submit_host'] = os.uname()[1] 
    
        if 'submit_time' in self.config:   # operator providing submit_time
            submit_time = self.config['submit_time']
        else:
            submit_epoch = time.time()
            submit_time = time.strftime("%Y%m%d%H%M%S", time.localtime(submit_epoch)) 
        self.config['submit_time'] = submit_time
    
        self.config['submit_epoch'] = submit_epoch
        self.config[PF_JOBNUM] = '0'
        self.config[PF_BLKNUM] = '1'
        self.config[PF_TASKNUM] = '0'
        self.config[PF_WRAPNUM] = '0'
        self.config[UNITNAME] = self.interpolate(self.config[UNITNAME])  
        self.set_block_info()
    
        self.config['submit_run'] = self.interpolate("${unitname}_r${reqnum}p${attnum:2}")
        self.config['run'] = self.config['submit_run']
    

        work_dir = ''
        if SUBMIT_RUN_DIR in self.config:
            work_dir = self.interpolate(self.config[SUBMIT_RUN_DIR])
            if work_dir[0] != '/':    # submit_run_dir was relative path
                work_dir = self.config['submit_dir'] + '/' + work_dir
                
        else:  # make a timestamp-based directory in cwd
            work_dir = self.config['submit_dir'] + '/' + os.path.splitext(self.config['submitwcl'])[0] + '_' + submit_time

        self.config['work_dir'] = work_dir
        self.config['uberctrl_dir'] = work_dir + "/uberctrl"

        if MASTER_SAVE_FILE in self.config:
            if self.config[MASTER_SAVE_FILE] not in VALID_MASTER_SAVE_FILE:
                m = re.match('rand_(\d\d)', self.config[MASTER_SAVE_FILE].lower())
                if m:
                    if random.randrange(100) <= int(m.group(1)):
                        fwdebug(2, 'PFWCONFIG_DEBUG', 'Changing %s to %s' % (MASTER_SAVE_FILE, 'always'))
                        self.config[MASTER_SAVE_FILE] = 'always' 
                    else:
                        fwdebug(2, 'PFWCONFIG_DEBUG', 'Changing %s to %s' % (MASTER_SAVE_FILE, 'file'))
                        self.config[MASTER_SAVE_FILE] = 'file' 
                else:
                    fwdie("Error:  Invalid value for %s (%s)" % (MASTER_SAVE_FILE, self.config[MASTER_SAVE_FILE]), PF_EXIT_FAILURE)
        else:
            self.config[MASTER_SAVE_FILE] = MASTER_SAVE_FILE_DEFAULT


    
    
    ###########################################################################
    def set_block_info(self):
        """ Set current vals to match current block number """
        fwdebug(1, 'PFWCONFIG_DEBUG', "BEG")

        curdict = self.config['current']
        fwdebug(4, 'PFWCONFIG_DEBUG', "\tcurdict = %s" % (curdict))

        # current block number
        blknum = self.config[PF_BLKNUM]

        # update current block name for accessing block information 
        blockname = self.get_block_name(blknum) 
        if not blockname:
            fwdie("Error: Cannot determine block name value for blknum=%s" % blknum, PF_EXIT_FAILURE)
        curdict['curr_block'] = blockname

        self.config['block_dir'] = '../B%02d-%s' % (int(blknum), blockname)
    
        # update current target site name
        (exists, site) = self.search('target_site')
        if not exists:
            # if target archive specified, get site associated to it
            (exists, archive) = self.search(TARGET_ARCHIVE)
            if not exists: 
                fwdie("Error:  Cannot determine target site (missing both target_site and target_archive)", PF_EXIT_FAILURE)
            site = self.config['archive'][archive]['site']

        site = site.lower()
        if site not in self.config['site']:
            print "Error: invalid site value (%s)" % (site)
            print "\tsite contains: ", self.config['site']
            fwdie("Error: Invalid site value (%s)" % (site), PF_EXIT_FAILURE)
        curdict['curr_site'] = site
        self.config['runsite'] = site

        # update current target archive name if using archive
        if ((USE_TARGET_ARCHIVE_INPUT in self and convertBool(self[USE_TARGET_ARCHIVE_INPUT])) or
            (USE_TARGET_ARCHIVE_OUTPUT in self and convertBool(self[USE_TARGET_ARCHIVE_OUTPUT])) ):
            (exists, archive) = self.search(TARGET_ARCHIVE)
            if not exists:
                fwdie("Error: Cannot determine target_archive value.   \n\tEither set target_archive or set to FALSE both %s and %s" % (USE_TARGET_ARCHIVE_INPUT, USE_TARGET_ARCHIVE_OUTPUT), PF_EXIT_FAILURE)
    
            archive = archive.lower()
            if archive not in self.config['archive']:
                print "Error: invalid target_archive value (%s)" % archive
                print "\tarchive contains: ", self.config['archive']
                fwdie("Error: Invalid target_archive value (%s)" % archive, PF_EXIT_FAILURE)
    
            curdict['curr_archive'] = archive

            if 'list_target_archives' in self.config:
                if not archive in self.config['list_target_archives']:  # assumes target archive names are not substrings of one another
                    self.config['list_target_archives'] += ',' + archive
            else:
                self.config['list_target_archives'] = archive
        elif ((USE_HOME_ARCHIVE_INPUT in self and convertBool(self[USE_TARGET_ARCHIVE_INPUT])) or
            (USE_HOME_ARCHIVE_OUTPUT in self and self[USE_HOME_ARCHIVE_OUTPUT] != 'never')):
            (exists, archive) = self.search(HOME_ARCHIVE)
            if not exists:
                fwdie("Error: Cannot determine home_archive value.   \n\tEither set home_archive or set correctly both %s and %s" % (USE_HOME_ARCHIVE_INPUT, USE_HOME_ARCHIVE_OUTPUT), PF_EXIT_FAILURE)
    
            archive = archive.lower()
            if archive not in self.config['archive']:
                print "Error: invalid home_archive value (%s)" % archive
                print "\tarchive contains: ", self.config['archive']
                fwdie("Error: Invalid home_archive value (%s)" % archive, PF_EXIT_FAILURE)
    
            curdict['curr_archive'] = archive
        else:
            curdict['curr_archive'] = None    # make sure to reset curr_archive from possible prev block value


        if 'submit_des_services' in self.config:
            self.config['des_services'] = self.config['submit_des_services']

        if 'submit_des_db_section' in self.config:
            self.config['des_db_section'] = self.config['submit_des_db_section']
    
        fwdebug(1, 'PFWCONFIG_DEBUG', "END") 

    
    def inc_blknum(self):
        """ increment the block number """
        # note config stores numbers as strings
        self.config[PF_BLKNUM] = str(int(self.config[PF_BLKNUM]) + 1)

    ###########################################################################
    def reset_blknum(self):
        """ reset block number to 1 """
        self.config[PF_BLKNUM] = '1'
    
    ###########################################################################
    def inc_jobnum(self, inc=1):
        """ Increment running job number """
        self.config[PF_JOBNUM] = str(int(self.config[PF_JOBNUM]) + inc)
        return self.config[PF_JOBNUM]
    

    ###########################################################################
    def inc_tasknum(self, inc=1):
        """ Increment blktask number """
        self.config[PF_TASKNUM] = str(int(self.config[PF_TASKNUM]) + inc)
        return self.config[PF_TASKNUM]
        

    ###########################################################################
    def inc_wrapnum(self):
        """ Increment running wrapper number """
        self.config[PF_WRAPNUM] = str(int(self.config[PF_WRAPNUM]) + 1)

    ###########################################################################
    def interpolate(self, value, opts=None):
        """ Replace variables in given value """
        fwdebug(5, 'PFWCONFIG_DEBUG', "BEG")
        fwdebug(6, 'PFWCONFIG_DEBUG', "\tinitial value = '%s'" % value)
        fwdebug(6, 'PFWCONFIG_DEBUG', "\tinitial opts = '%s'" % opts)

        maxtries = 1000    # avoid infinite loop
        count = 0
        done = False
        while not done and count < maxtries:
            done = True
    
            m = re.search("(?i)\$opt\{([^}]+)\}", value)
            while m and count < maxtries:
                count += 1
                var = m.group(1)
                print "opt var=",var
                parts = var.split(':')
                newvar = parts[0]
                if len(parts) > 1:
                    prpat = "%%0%dd" % int(parts[1])
                (haskey, newval) = self.search(newvar, opts)
                print "opt: type(newval):", newvar, type(newval) 
                if haskey:
                    if '(' in newval or ',' in newval: 
                        if 'expand' in opts and opts['expand']:
                            newval = '$LOOP{%s}' % var   # postpone for later expanding
                    elif len(parts) > 1:
                        newval = prpat % int(self.interpolate(newval, opts))
                else:
                    newval = ""
                print "val = %s" % newval
                value = re.sub("(?i)\$opt{%s}" % var, newval, value)
                print value
                done = False
                m = re.search("(?i)\$opt\{([^}]+)\}", value)

            m = re.search("(?i)\$\{([^}]+)\}", value)
            while m and count < maxtries:
                count += 1
                var = m.group(1)
                parts = var.split(':')
                newvar = parts[0]
                fwdebug(6, 'PFWCONFIG_DEBUG', "\twhy req: newvar: %s " % (newvar))
                if len(parts) > 1:
                    prpat = "%%0%dd" % int(parts[1])
                (haskey, newval) = self.search(newvar, opts)
                fwdebug(6, 'PFWCONFIG_DEBUG', 
                      "\twhy req: haskey, newvar, newval, type(newval): %s, %s %s %s" % (haskey, newvar, newval, type(newval)))
                if haskey:
                    newval = str(newval)
                    if '(' in newval or ',' in newval:
                        if opts is not None and 'expand' in opts and opts['expand']:
                            newval = '$LOOP{%s}' % var   # postpone for later expanding
                        fwdebug(6, 'PFWCONFIG_DEBUG', "\tnewval = %s" % newval)
                    elif len(parts) > 1:
                        try:
                            newval = prpat % int(self.interpolate(newval, opts))
                        except ValueError as err:
                            print str(err)
                            print "prpat =", prpat
                            print "newval =", newval
                            raise err
                    value = re.sub("(?i)\${%s}" % var, newval, value)
                    done = False
                else:
                    fwdie("Error: Could not find value for %s" % newvar, PF_EXIT_FAILURE)
                m = re.search("(?i)\$\{([^}]+)\}", value)


        valuedone = []
        if '$LOOP' in value:
            if opts is not None:
                opts['required'] = True
                opts['interpolate'] = False
            else:
                opts = {'required': True, 'interpolate': False}

            looptodo = [ value ]
            while len(looptodo) > 0 and count < maxtries:
                count += 1
                fwdebug(6, 'PFWCONFIG_DEBUG',
                        "todo loop: before pop number in looptodo = %s" % len(looptodo))
                value = looptodo.pop() 
                fwdebug(6, 'PFWCONFIG_DEBUG',
                        "todo loop: after pop number in looptodo = %s" % len(looptodo))

                fwdebug(3, 'PFWCONFIG_DEBUG', "todo loop: value = %s" % value)
                m = re.search("(?i)\$LOOP\{([^}]+)\}", value)
                var = m.group(1)
                parts = var.split(':')
                newvar = parts[0]
                if len(parts) > 1:
                    prpat = "%%0%dd" % int(parts[1])
                fwdebug(6, 'PFWCONFIG_DEBUG', "\tloop search: newvar= %s" % newvar)
                fwdebug(6, 'PFWCONFIG_DEBUG', "\tloop search: opts= %s" % opts)
                (haskey, newval) = self.search(newvar, opts)
                if haskey:
                    fwdebug(6, 'PFWCONFIG_DEBUG', "\tloop search results: newva1= %s" % newval)
                    newvalarr = fwsplit(newval) 
                    for nv in newvalarr:
                        fwdebug(6, 'PFWCONFIG_DEBUG', "\tloop nv: nv=%s" % nv)
                        if len(parts) > 1:
                            try:
                                nv = prpat % int(nv)
                            except ValueError as err:
                                print str(err)
                                print "prpat =", prpat
                                print "nv =", nv
                                raise err
                        fwdebug(6, 'PFWCONFIG_DEBUG', "\tloop nv2: nv=%s" % nv)
                        fwdebug(6, 'PFWCONFIG_DEBUG', "\tbefore loop sub: value=%s" % value)
                        valsub = re.sub("(?i)\$LOOP\{%s\}" % var, nv, value)
                        fwdebug(6, 'PFWCONFIG_DEBUG', "\tafter loop sub: value=%s" % valsub)
                        if '$LOOP{' in valsub:
                            fwdebug(6, 'PFWCONFIG_DEBUG', "\t\tputting back in todo list")
                            looptodo.append(valsub)
                        else:
                            valuedone.append(valsub)
                            fwdebug(6, 'PFWCONFIG_DEBUG', "\t\tputting back in done list")
                fwdebug(6, 'PFWCONFIG_DEBUG', "\tNumber in todo list = %s" % len(looptodo))
                fwdebug(6, 'PFWCONFIG_DEBUG', "\tNumber in done list = %s" % len(valuedone))
            fwdebug(6, 'PFWCONFIG_DEBUG', "\tEND OF WHILE LOOP = %s" % len(valuedone))
    
        if count >= maxtries:
            fwdie("Error: Interpolate function aborting from infinite loop\n. Current string: '%s'" % value, PF_EXIT_FAILURE)
    
        fwdebug(6, 'PFWCONFIG_DEBUG', "\tvaluedone = %s" % valuedone)
        fwdebug(6, 'PFWCONFIG_DEBUG', "\tvalue = %s" % value)
        fwdebug(5, 'PFWCONFIG_DEBUG', "END")

        if len(valuedone) > 1:
            return valuedone
        elif len(valuedone) == 1:
            return valuedone[0]
        else:
            return value
    
    ###########################################################################
    def get_block_name(self, blknum):
        """ Return block name based upon given block num """
        blknum = int(blknum)   # read in from file as string

        blockname = ''
        blockarray = re.sub(r"\s+", '', self.config[SW_BLOCKLIST]).split(',')
        if (1 <= blknum) and (blknum <= len(blockarray)):
            blockname = blockarray[blknum-1]
        return blockname

    
    ###########################################################################
    def get_condor_attributes(self, subblock):
        """Create dictionary of attributes for condor jobs"""
        attribs = {} 
        attribs[ATTRIB_PREFIX + 'isjob'] = 'TRUE'
        attribs[ATTRIB_PREFIX + 'project'] = self.config['project']
        attribs[ATTRIB_PREFIX + 'pipeline'] = self.config['pipeline']
        attribs[ATTRIB_PREFIX + 'run'] = self.config['submit_run']
        attribs[ATTRIB_PREFIX + 'block'] = self.config['current']['curr_block']
        attribs[ATTRIB_PREFIX + 'operator'] = self.config['operator']
        attribs[ATTRIB_PREFIX + 'runsite'] = self.config['runsite']
        attribs[ATTRIB_PREFIX + 'subblock'] = subblock
        if (subblock == '$(jobnum)'):
            if 'numjobs' in self.config:
                attribs[ATTRIB_PREFIX + 'numjobs'] = self.config['numjobs']
            if 'glidein_name' in self.config:
                attribs['GLIDEIN_NAME'] = self.config['glidein_name']
        return attribs
    
    
    ###########################################################################
    def get_dag_cmd_opts(self):
        """Create dictionary of condor_submit_dag command line options"""
        cmdopts = {} 
        for key in ['max_pre', 'max_post', 'max_jobs', 'max_idle']:
            (exists, value) = self.search('dagman_' + key)
            if exists:
                cmdopts[key] = value
        return cmdopts
            
    
    ###########################################################################
    def get_grid_info(self):
        """Create dictionary of grid job submission options"""
        vals = {}
        for key in ['stdout', 'stderr', 'queue', 'psn', 'job_type',
                    'max_wall_time', 'max_time', 'max_cpu_time',
                    'max_memory', 'min_memory', 'count', 'host_count',
                    'host_types', 'host_xcount', 'xcount',  'reservation_id',
                    'grid_resource', 'grid_type', 'grid_host', 'grid_port',
                    'batch_type', 'globus_extra', 'environment']:
            newkey = key.replace('_','')
            (exists, value) = self.search(key)
            if exists:
                vals[newkey] = value
            else:
                (exists, value) = self.search(newkey)
                if exists:
                    vals[newkey] = value
                else:
                    fwdebug(3, 'PFWCONFIG_DEBUG', "Could not find value for %s(%s)" % (key, newkey))
    
        print "get_grid_info:  returning vals=", vals
        return vals

    ###########################################################################
    def stagefile(self, opts):
        """ Determine whether should stage files or not """
        retval = True
        (dryrun_exists, dryrun) = self.search(PF_DRYRUN, opts)
        if dryrun_exists and convertBool(dryrun):
            retval = False
        (stagefiles_exists, stagefiles) = self.search(STAGEFILES, opts)
        if stagefiles_exists and not convertBool(stagefiles):
            retval = False
        return retval


    ###########################################################################
    def get_filename(self, filepat=None, searchopts=None):
        """ Return filename based upon given file pattern name """
        filename = ""

        origreq = False
        if searchopts is not None and 'required' in searchopts:
            origreq = searchopts['required']
            searchopts['required'] = False
            
        if not filepat:
            # first check for filename pattern override 
            (found, filenamepat) = self.search('filename', searchopts)
        
            if not found:
                # get filename pattern from global settings:
                (found, filepat) = self.search(SW_FILEPAT, searchopts)

                if not found:
                    fwdie("Error: Could not find file pattern %s" % SW_FILEPAT, PF_EXIT_FAILURE)
        else:
            fwdebug(2, 'PFWCONFIG_DEBUG', "given filepat = %s" % (filepat))

        
        if SW_FILEPATSECT not in self.config:
            wclutils.write_wcl(self.config)
            fwdie("Error: Could not find filename pattern section (%s)" % SW_FILEPATSECT, PF_EXIT_FAILURE)
        elif filepat in self.config[SW_FILEPATSECT]:
            filenamepat = self.config[SW_FILEPATSECT][filepat]
        else:
            print SW_FILEPATSECT, " keys: ", self.config[SW_FILEPATSECT].keys()
            fwdie("Error: Could not find filename pattern for %s" % filepat, PF_EXIT_FAILURE, 2)

        if searchopts is not None:
            searchopts['required'] = origreq
                
        filename = self.interpolate(filenamepat, searchopts)
        return filename


    ###########################################################################
    def get_filepath(self, pathtype, dirpat=None, searchopts=None):
        """ Return filepath based upon given pathtype and directory pattern name """
        filepath = ""
       
        # get filename pattern from global settings:
        if not dirpat:
            (found, dirpat) = self.search(DIRPAT, searchopts)

            if not found:
                fwdie("Error: Could not find dirpat", PF_EXIT_FAILURE)

        if dirpat in self.config[DIRPATSECT]:
            filepathpat = self.config[DIRPATSECT][dirpat][pathtype]
        else:
            fwdie("Error: Could not find pattern %s in directory patterns" % dirpat, PF_EXIT_FAILURE)
                
        filepath = self.interpolate(filepathpat, searchopts)
        return filepath

        
    ###########################################################################
    def combine_lists_files(self, modulename):
        """ Return python list of file and file list objects """
        fwdebug(3, 'PFWCONFIG_DEBUG', "BEG")
        
        moduledict = self[SW_MODULESECT][modulename]
        
        # create python list of files and lists for this module
        dataset = []
        if SW_LISTSECT in moduledict and len(moduledict[SW_LISTSECT]) > 0:
            if 'list_order' in moduledict:
                listorder = moduledict['list_order'].replace(' ','').split(',')
            else:
                listorder = moduledict[SW_LISTSECT].keys()
            for k in listorder:
                dataset.append((k, moduledict[SW_LISTSECT][k]))
        else:
            fwdebug(3, 'PFWCONFIG_DEBUG', "no lists")
        
        if SW_FILESECT in moduledict and len(moduledict[SW_FILESECT]) > 0:
            for k,v in moduledict[SW_FILESECT].items():
                dataset.append((k,v))
        else:
            fwdebug(3, 'PFWCONFIG_DEBUG', "no files")

        fwdebug(3, 'PFWCONFIG_DEBUG', "END")
        return dataset 

    ###########################################################################
    def set_names(self):
        """ set names for use in patterns (i.e., blockname, modulename) """


        for tsname, tsval in self.config.items():
            if isinstance(tsval, dict):
                for nsname, nsval in tsval.items():
                    if isinstance(nsval, dict): 
                        namestr = '%sname' % tsname
                        if namestr not in nsval: 
                            nsval[namestr] = nsname



    ###########################################################################
    # Determine whether should stage files or not
    def stagefiles(self, opts=None):
        """ Return whether to save stage files to target archive """
        retval = True

        notarget_exists, notarget = self.search(NO_TARGET, opts)
        if notarget_exists and convertBool(notarget):
            print "Do not stage file due to notarget\n"
            retval = False
        else:
            stagefiles_exists, stagefiles = self.search(SW_STAGEFILES, opts)
            if stagefiles_exists:
                #print "checking stagefiles (%s)" % stagefiles
                retval = convertBool(self.interpolate(stagefiles, opts))
                #print "after interpolation stagefiles (%s)" % retval
            else:
                envkey = 'DESDM_%' % SW_STAGEFILES.upper()
                if envkey in os.environ and not convertBool(os.environ[envkey]):
                    retval = False

        #print "stagefiles retval = %s" % retval
        return retval



    ###########################################################################
    # Determine whether should save files or not
    def savefiles(self, opts=None):
        """ Return whether to save files from job """
        retval = True

        savefiles_exists, savefiles = self.search(SAVE_FILE_ARCHIVE, opts)
        if savefiles_exists:
            fwdebug(3, "PFWUTILS_DEBUG", "checking savefiles (%s)" % savefiles)
            retval = convertBool(self.interpolate(savefiles, opts))
            fwdebug(3, "PFWUTILS_DEBUG", "after interpolation savefiles (%s)" % retval)
        else:
            envkey = 'DESDM_%' % SW_SAVEFILES.upper()
            if envkey in os.environ and not convertBool(os.environ[envkey]):
                retval = False

        fwdebug(3, "PFWUTILS_DEBUG", "savefiles retval = %s" % retval)
        return retval

    def __len__(self):
        return len(self.config)

    def items(self):
        return self.config.items()


    def get_param_info(self, vals, opts=None):
        info = {}
        for v, stat in vals.items():
            (found, value) = self.search(v, opts)
            if found:
                info[v] = value
            else:
                if stat.lower() == 'req':
                    fwdie("Error:  Config does not contain value for %s" % v, PF_EXIT_FAILURE, 2)

        return info

    def interpolateKeep(self, value, opts=None):
        """ Replace variables in given value """
        fwdebug(5, 'PFWCONFIG_DEBUG', "BEG")
        fwdebug(6, 'PFWCONFIG_DEBUG', "\tinitial value = '%s'" % value)
        fwdebug(6, 'PFWCONFIG_DEBUG', "\tinitial opts = '%s'" % opts)

        keep = {}

        maxtries = 1000    # avoid infinite loop
        count = 0
        done = False
        while not done and count < maxtries:
            done = True
    
            m = re.search("(?i)\$opt\{([^}]+)\}", value)
            while m and count < maxtries:
                count += 1
                var = m.group(1)
                print "opt var=",var
                parts = var.split(':')
                newvar = parts[0]
                if len(parts) > 1:
                    prpat = "%%0%dd" % int(parts[1])
                (haskey, newval) = self.search(newvar, opts)
                print "opt: type(newval):", newvar, type(newval) 
                if haskey:
                    if '(' in newval or ',' in newval: 
                        if 'expand' in opts and opts['expand']:
                            newval = '$LOOP{%s}' % var   # postpone for later expanding
                    elif len(parts) > 1:
                        newval = prpat % int(self.interpolate(newval, opts))
                        keep[newvar] = newval
                    else:
                        keep[newvar] = newval
                else:
                    newval = ""
                print "val = %s" % newval
                value = re.sub("(?i)\$opt{%s}" % var, newval, value)
                print value
                done = False
                m = re.search("(?i)\$opt\{([^}]+)\}", value)

            m = re.search("(?i)\$\{([^}]+)\}", value)
            while m and count < maxtries:
                count += 1
                var = m.group(1)
                parts = var.split(':')
                newvar = parts[0]
                fwdebug(6, 'PFWCONFIG_DEBUG', "\twhy req: newvar: %s " % (newvar))
                if len(parts) > 1:
                    prpat = "%%0%dd" % int(parts[1])
                (haskey, newval) = self.search(newvar, opts)
                fwdebug(6, 'PFWCONFIG_DEBUG', 
                      "\twhy req: haskey, newvar, newval, type(newval): %s, %s %s %s" % (haskey, newvar, newval, type(newval)))
                if haskey:
                    newval = str(newval)
                    if '(' in newval or ',' in newval:
                        if opts is not None and 'expand' in opts and opts['expand']:
                            newval = '$LOOP{%s}' % var   # postpone for later expanding
                        fwdebug(6, 'PFWCONFIG_DEBUG', "\tnewval = %s" % newval)
                    elif len(parts) > 1:
                        try:
                            newval = prpat % int(self.interpolate(newval, opts))
                            keep[newvar] = newval
                        except ValueError as err:
                            print str(err)
                            print "prpat =", prpat
                            print "newval =", newval
                            raise err
                    else:
                        keep[newvar] = newval

                    value = re.sub("(?i)\${%s}" % var, newval, value)
                    done = False
                else:
                    fwdie("Error: Could not find value for %s" % newvar, PF_EXIT_FAILURE)
                m = re.search("(?i)\$\{([^}]+)\}", value)

        print "keep = ", keep

        valpair = (value, keep)
        valuedone = []
        if '$LOOP' in value:
            if opts is not None:
                opts['required'] = True
                opts['interpolate'] = False
            else:
                opts = {'required': True, 'interpolate': False}

            looptodo = [ valpair ]
            while len(looptodo) > 0 and count < maxtries:
                count += 1
                fwdebug(6, 'PFWCONFIG_DEBUG',
                        "todo loop: before pop number in looptodo = %s" % len(looptodo))
                valpair = looptodo.pop() 
                fwdebug(6, 'PFWCONFIG_DEBUG',
                        "todo loop: after pop number in looptodo = %s" % len(looptodo))

                fwdebug(3, 'PFWCONFIG_DEBUG', "todo loop: value = %s" % valpair[0])
                m = re.search("(?i)\$LOOP\{([^}]+)\}", valpair[0])
                var = m.group(1)
                parts = var.split(':')
                newvar = parts[0]
                if len(parts) > 1:
                    prpat = "%%0%dd" % int(parts[1])
                fwdebug(6, 'PFWCONFIG_DEBUG', "\tloop search: newvar= %s" % newvar)
                fwdebug(6, 'PFWCONFIG_DEBUG', "\tloop search: opts= %s" % opts)
                (haskey, newval) = self.search(newvar, opts)
                if haskey:
                    fwdebug(6, 'PFWCONFIG_DEBUG', "\tloop search results: newva1= %s" % newval)
                    newvalarr = fwsplit(newval) 
                    for nv in newvalarr:
                        fwdebug(6, 'PFWCONFIG_DEBUG', "\tloop nv: nv=%s" % nv)
                        if len(parts) > 1:
                            try:
                                nv = prpat % int(nv)
                            except ValueError as err:
                                print str(err)
                                print "prpat =", prpat
                                print "nv =", nv
                                raise err
                        fwdebug(6, 'PFWCONFIG_DEBUG', "\tloop nv2: nv=%s" % nv)
                        fwdebug(6, 'PFWCONFIG_DEBUG', "\tbefore loop sub: value=%s" % value)
                        valsub = re.sub("(?i)\$LOOP\{%s\}" % var, nv, value)
                        keep = copy.deepcopy(valpair[1])
                        keep[newvar] = nv
                        fwdebug(6, 'PFWCONFIG_DEBUG', "\tafter loop sub: value=%s" % valsub)
                        if '$LOOP{' in valsub:
                            fwdebug(6, 'PFWCONFIG_DEBUG', "\t\tputting back in todo list")
                            looptodo.append((valsub, keep))
                        else:
                            valuedone.append((valsub, keep))
                            fwdebug(6, 'PFWCONFIG_DEBUG', "\t\tputting back in done list")
                fwdebug(6, 'PFWCONFIG_DEBUG', "\tNumber in todo list = %s" % len(looptodo))
                fwdebug(6, 'PFWCONFIG_DEBUG', "\tNumber in done list = %s" % len(valuedone))
            fwdebug(6, 'PFWCONFIG_DEBUG', "\tEND OF WHILE LOOP = %s" % len(valuedone))
    
        if count >= maxtries:
            fwdie("Error: Interpolate function aborting from infinite loop\n. Current string: '%s'" % value, PF_EXIT_FAILURE)
    
        fwdebug(6, 'PFWCONFIG_DEBUG', "\tvaluedone = %s" % valuedone)
        fwdebug(6, 'PFWCONFIG_DEBUG', "\tvalue = %s" % value)
        fwdebug(5, 'PFWCONFIG_DEBUG', "END")

        if len(valuedone) >= 1:
            return valuedone
        else:
            return [valpair]
        


if __name__ ==  '__main__' :
    if len(sys.argv) == 2:
        pfw = PfwConfig({'wclfile': sys.argv[1]})
        #pfw.save_file(sys.argv[2])
        print SW_BLOCKLIST in pfw
        print 'not_there' in pfw
        pfw.set_block_info()
        print pfw[PF_BLKNUM]
        pfw.inc_blknum()
        print pfw[PF_BLKNUM]
        pfw.reset_blknum()
        pfw.set_block_info()
        print pfw[PF_BLKNUM]
