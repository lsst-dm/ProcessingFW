#!/usr/bin/env python
# $Id:$
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

import intgutils.wclutils as wclutils
from pfwutils import debug
from pfwutils import pfwsplit

class PfwConfig:
    """ Contains configuration and state information for PFW """

    # order in which to search for values
    DEFORDER = ['file', 'list', 'exec', 'job', 'module', 'block', 'archive', 'site']

    # misc constants 
    ATTRIB_PREFIX='des_'
    SUCCESS = 0
    REPEAT = 100
    FAILURE = 10
    NOTARGET = 2
    WARNINGS = 3

    ###########################################################################
    def __init__(self, args):
        """ Initialize configuration object, typically reading from wclfile """
        self.config = {}

        if 'debug' in args:
            self.config['debug'] = args['debug']
        else:
            self.config['debug'] = 0

        if 'wclfile' in args:
            debug(3, 'PFWCONFIG_DEBUG', "Reading wclfile: %s" % (args['wclfile']))
            fh = open(args['wclfile'], "r")
            self.config = wclutils.read_wcl(fh) 
            fh.close()
            if 'debug' not in self.config:  # recheck since reset config
                self.config['debug'] = 0

        if 'notarget' in args:
            self.config['notarget'] = args['notarget']


        # during runtime save block_list as array
        self.block_array = pfwsplit(self.config['block_list'])
        self.config['num_blocks'] = len(self.block_array)
    
        # create a lookup by site id to get the site name
        # needed because config's keys are site name
        siteid2name = {}
        if 'site' in self.config:
            for sitename, site in self.config['site'].items():
                if 'site_id' in site:
                    siteid2name[site['site_id']] = sitename
        self.siteid2name = siteid2name
    
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
                                                  'curr_site' : ''})
            self.config['wrapnum'] = '0'
            self.config['blocknum'] = '0'
            self.config['jobnum'] = '1'

        self.set_block_info()

    ###########################################################################
    def save_file(self, filename):
        """Saves configuration in WCL format"""
        fh = open(filename, "w")
        wclutils.write_wcl(self.config, fh, True, 4)  # save it sorted
        fh.close()

    ###########################################################################
    #def has_key(self, key, opts=None):
    #    (found, value) = self.search(key, opts)
    #    return found

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
    #def get(self, key, default = None, opt = None):
    #    (found, val) = self.search(key, opt)
    #    if not found:
    #        val = default
    #    return val


    ###########################################################################
    def search(self, key, opt=None):
        """ Searches for key using given opt following hierarchy rules """ 
        debug(8, 'PFWCONFIG_DEBUG', "\tBEG")
        debug(8, 'PFWCONFIG_DEBUG',
                 "\tinitial key = '%s'" % key)
        debug(8, 'PFWCONFIG_DEBUG',
                 "\tinitial opts = '%s'" % opt)

        found = False
        value = ''
        key = key.lower()
    
        # start with stored current values
        curvals = copy.deepcopy(self.config['current'])

        # override with current values passed into function if given
        if opt is not None and 'currentvals' in opt:
            for k,v in opt["currentvals"].items():
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
                    currkey = curvals["curr_"+sect]
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
            raise Exception("Search failed")
    
        if found and opt and 'interpolate' in opt and opt['interpolate']:
            opt['interpolate'] = False
            value = self.interpolate(value, opt) 

        debug(8, 'PFWCONFIG_DEBUG', "\tEND")
        return (found, value)
    

    ########################################################################### 
    def check(self, cleanup=False):
        """ Check for missing data """
    
        # initialize counters
        errcnt = 0
        warncnt = 0
        changecnt = 0
        cleancnt = 0
        
        # just abort the check if do not have major sections of config
        if 'archive' not in self.config:
            raise Exception('Error: Could not find archive section')
        if 'block' not in self.config:
            raise Exception('Error: Could not find block section')
        if 'module' not in self.config:
            raise Exception('Error: Could not find module section')
    
        # make sure project is all uppercase
        # self.config['project'] = self['project'].upper()
    
        if 'operator' not in self.config:
            print 'Warning:  Must specify operator'
            print 'Using your Unix login for this submission.  Please fix in your submit file.'
            self.config['operator'] = getpass.getuser() 
            changecnt += 1
        elif self.config['operator'] == 'bcs':
            print 'Warning:  Operator cannot be shared login bcs.'
            print 'Using your Unix login for this submission.  Please fix in your submit file.'
            self.config['operator'] = getpass.getuser()
            changecnt += 1
    
        if 'project' not in self.config:
            print "Error: missing project"
            errcnt += 1

        if 'pipeline' not in self.config:
            print "Error: missing pipeline"
            errcnt += 1

        if 'pipever' not in self.config:
            print "Error: missing pipever"
            errcnt += 1

        if 'reqnum' not in self.config:
            print "Error: missing reqnum"
            errcnt += 1

        if 'attnum' not in self.config:
            print "Error: missing attnum"
            errcnt += 1

        if 'procunit' not in self.config:
            print "Error: missing procunit"
            errcnt += 1

        # target_node replaces depricated archive_node
        if 'archive_node' in self.config:
            if 'target_node' in self.config:
                print "\tWarning: have both target_node and depricated archive_node defined in global section."
                warncnt += 1
                if cleanup:
                    print "\tDeleting depricated archive_node"
                    del self.config['archive_node']
                    cleancnt += 1
            else:
                print "\tWarning: depricated use of archive_node in global section."
                warncnt += 1
                if cleanup:
                    print "\tSetting global target_node = global archive_node"
                    self.config['target_node'] = self.config['archive_node']
                    print "\tDeleting depricated archive_node"
                    del self.config['archive_node']
                    cleancnt += 1
                
        # submit_node must be set globally
        submit_node = None
        if 'submit_node' not in self.config:
            print 'Error: submit_node is not specified.'
            errcnt += 1
#        elif self.config['submit_node'] not in self.config['archive']:
#            print 'Error:  Could not find archive information for submit node %s' % self.config['submit_node']
#            errcnt += 1
#        elif 'archive_root' not in self.config['archive'][self.config['submit_node']]:
#            print 'Error:  archive_root not specified for submit node %s' % self.config['submit_node']
#            errcnt += 1
#        elif 'site_id' not in self.config['archive'][self.config['submit_node']]:
#            print 'Error: site_id not specified for submit node %s' % self.config['submit_node']
#            errcnt += 1
#        else:
#            submit_node = self.config['submit_node']
#            archiveroot = self.config['archive'][submit_node]['archive_root']
#            if not os.path.exists(archiveroot):
#                print 'Warning: archive_root (%s) from submit_node does not exist on disk' % archiveroot
#                warncnt += 1
#    
#            submit_siteid = self.config['archive'][submit_node]['site_id']
#            if submit_siteid not in self.siteid2name:
#                print 'Error: Could not find site information for site %s from submit node info.' % submit_siteid
#                errcnt += 1
#                submit_siteid = None
#            elif 'login_host' not in self.config['site'][self.siteid2name[submit_siteid]]:
#                print 'Error:  login_host is not defined for submit site %s (%s).\n' % (self.siteid2name[submit_siteid], submit_siteid)
#                errcnt += 1
#            elif os.uname()[1] != self.config['site'][self.siteid2name[submit_siteid]]['login_host']:
#                print 'Error:  submit node %s (%s) does not match submit host (%s).' % (submit_node, self.config['site'][self.siteid2name[submit_siteid]]['login_host'], os.uname()[1])
#                print 'Debugging tips: '
#                print '\tCheck submit_node value, '
#                print '                Check correct site_id defined for submit_node,'
#                print '\tcheck login_host defined for site linked to submit_node'
#                   errcnt += 1
    
    
        # Check block definitions for simple single module blocks.
        # Also check all blocks in block_list have definitions as well as all modules in their module_lists
        if 'block_list' not in self.config:
            print "Error: missing block_list" 
        else:
            self.config['block_list'] = re.sub(r"\s+", '', self.config['block_list'].lower())
            blocklist = self.config['block_list'].split(',')
    
            for blockname in blocklist:
                print "\tChecking block:", blockname
                if blockname in self.config['block']:
                    block = self.config['block'][blockname]
                    if 'module_list' in block:
                        block['module_list'] = re.sub(r"\s+", '', block['module_list'].lower())
                        module_list = block['module_list'].split(',')

                        for modulename in module_list:
                            if modulename not in self.config['module']:
                                print "\tError: missing definition for module %s from block %s" % (modulename, blockname)
                                errcnt += 1
                    elif blockname in self.config['module']:
                        print "\tWarning: Missing module_list definition for block %s" % (blockname)
                        if cleanup:
                            print "\t         Defaulting to module_list=%s" % (blockname)
                        block['module_list'] = blockname
                    else:
                        print "\tError: missing module_list definition for block %s" % (blockname)
                        errcnt += 1
                else:
                    if blockname in self.config['module']:
                        print "\tWarning: Missing block definition for %s" % blockname
                        if cleanup:
                            print "\t         Creating new block definition with module_list=%s" % (blockname)
                            self.config['block'][blockname] = { 'module_list': blockname }
                            block = self.config['block'][blockname]
                    else:
                        print "\tError: missing definition for block %s" % (blockname)
                        errcnt += 1
    
                if block: 
                    if 'archive_node' in block:
                        if 'target_node' in block:
                            print "\tWarning:  Have both archive_node and target_node defined in block %s" % (blockname)
                            warncnt += 1
                            if cleanup:
                                print "\t\tDeleting depricated archive_node"
                                del block['archive_node']
                                cleancnt += 1
                        else:
                            print "\tWarning:  deprecated archive_node defined in block %s" % (blockname)
                            warncnt += 1
                            if cleanup:
                                print "\t\tSetting target_node = archive_node"
                                block['target_node'] = block['archive_node']
                                print "\t\tDeleting depricated archive_node"
                                del block['archive_node']
                                cleancnt += 1
    
                    if 'target_node' in block:
                        target_node = block['target_node']
                    elif 'target_node' in self.config:
                        target_node = self.config['target_node']
                    else:
                        print "\tError: Could not determine target_node for block %s" % (blockname)
                        errcnt += 1
    
                    target_siteid = None
                    if target_node not in self.config['archive']:
                        print "\tError: missing definition for target node %s from block %s" % (target_node, blockname)
                        errcnt += 1
                    elif 'site_id' not in self.config['archive'][target_node]:
                        print "\tError: missing site_id for target node %s from block %s" % (target_node, blockname)
                        errcnt += 1
                    else:
                        target_siteid = self.config['archive'][target_node]['site_id']
    
            return (errcnt, warncnt, cleancnt)
    
    
    
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
        self.config['jobnum'] = '1'
        self.config['blocknum'] = '0'
        self.config['wrapnum'] = '0'
        self.set_block_info()
    
        self.config['submit_run'] = self.interpolate("r${reqnum}p${attnum:2}_${procunit}")
        self.config['run'] = self.config['submit_run']
    
        work_dir = self.config['submit_dir'] + '/' + \
                   os.path.splitext(self.config['submitwcl'])[0] + \
                   '_' + submit_time
        self.config['work_dir'] = work_dir
        self.config['uberctrl_dir'] = work_dir + "/runtime/uberctrl"
    
    
    ###########################################################################
    def set_block_info(self):
        """ Set currentvals to match current block number """
        debug(1, 'PFWCONFIG_DEBUG', "BEG")

        curdict = self.config['current']
        debug(4, 'PFWCONFIG_DEBUG', "\tcurdict = %s" % (curdict))

        blocknum = self.config['blocknum']

        blockname = self.get_block_name(blocknum) 
        if not blockname:
            raise Exception("Error: set_block_info cannot determine block name value for blocknum=%s" % blocknum)
        curdict['curr_block'] = blockname
    
        (exists, targetnode) = self.search('target_node')
        if not exists:
            raise Exception("Error: set_block_info cannot determine target_node value")
    
        if targetnode not in self.config['archive']:
            raise Exception("Error: invalid target_node value (%s)" % targetnode)
    
        curdict['curr_archive'] = targetnode
    
        if 'listtargets' in self.config:
            listt = self.config['listtargets']
            if not targetnode in listt:  # assumes targetnode names are not substrings of one another
                self.config['listtargets'] += ',' + targetnode
        else:
            self.config['listtargets'] = targetnode
        
#depricated?        curdict['curr_software'] = self['software_node']
    
        (exists, siteid) = self.search('site_id')
        if exists and self.siteid2name:
            runsite = self.siteid2name[siteid]
            self.config['runsite'] = runsite
            curdict['curr_site'] = runsite
        else:
            raise Exception('Error: set_block_info cannot determine run_site value')
        debug(1, 'PFWCONFIG_DEBUG', "END") 

    
    ###########################################################################
    def inc_block_num(self):
        """ increment the block number """
        # note config stores numbers as strings
        self.config['blocknum'] = str(int(self.config['blocknum']) + 1)
    
    ###########################################################################
    def reset_block_num(self):
        """ reset block number to 0 """
        self.config['blocknum'] = '0'
    
    ###########################################################################
    def inc_jobnum(self, inc):
        """ Increment running job number """
        self.config['jobnum'] = str(int(self.config['jobnum']) + inc)
    
    ###########################################################################
    def inc_wrapnum(self):
        """ Increment running wrapper number """
        self.config['wrapnum'] = str(int(self.config['wrapnum']) + 1)

    ###########################################################################
    def interpolate(self, value, opts=None):
        """ Replace variables in given value """
        debug(5, 'PFWCONFIG_DEBUG', "BEG")
        debug(6, 'PFWCONFIG_DEBUG', "\tinitial value = '%s'" % value)
        debug(6, 'PFWCONFIG_DEBUG', "\tinitial opts = '%s'" % opts)

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
                        newval = prpat % int(newval)
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
                if len(parts) > 1:
                    prpat = "%%0%dd" % int(parts[1])
                (haskey, newval) = self.search(newvar, opts)
                debug(6, 'PFWCONFIG_DEBUG', 
                      "\twhy req: newvar, newval, type(newval): %s %s %s" % (newvar, newval, type(newval)))
                if haskey:
                    if '(' in newval or ',' in newval:
                        if 'expand' in opts and opts['expand']:
                            newval = '$LOOP{%s}' % var   # postpone for later expanding
                        debug(6, 'PFWCONFIG_DEBUG', "\tnewval = %s" % newval)
                    elif len(parts) > 1:
                        try:
                            newval = prpat % int(newval)
                        except ValueError as err:
                            print str(err)
                            print "prpat =", prpat
                            print "newval =", newval
                            raise err
                    value = re.sub("(?i)\${%s}" % var, newval, value)
                    done = False
                else:
                    raise Exception("Could not find value for %s" % var)
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
                debug(6, 'PFWCONFIG_DEBUG',
                        "todo loop: before pop number in looptodo = %s" % len(looptodo))
                value = looptodo.pop() 
                debug(6, 'PFWCONFIG_DEBUG',
                        "todo loop: after pop number in looptodo = %s" % len(looptodo))

                debug(3, 'PFWCONFIG_DEBUG', "todo loop: value = %s" % value)
                m = re.search("(?i)\$LOOP\{([^}]+)\}", value)
                var = m.group(1)
                parts = var.split(':')
                newvar = parts[0]
                if len(parts) > 1:
                    prpat = "%%0%dd" % int(parts[1])
                debug(6, 'PFWCONFIG_DEBUG', "\tloop search: newvar= %s" % newvar)
                debug(6, 'PFWCONFIG_DEBUG', "\tloop search: opts= %s" % opts)
                (haskey, newval) = self.search(newvar, opts)
                if haskey:
                    debug(6, 'PFWCONFIG_DEBUG', "\tloop search results: newva1= %s" % newval)
                    newvalarr = pfwsplit(newval) 
                    for nv in newvalarr:
                        debug(6, 'PFWCONFIG_DEBUG', "\tloop nv: nv=%s" % nv)
                        if len(parts) > 1:
                            try:
                                nv = prpat % int(nv)
                            except ValueError as err:
                                print str(err)
                                print "prpat =", prpat
                                print "nv =", nv
                                raise err
                        debug(6, 'PFWCONFIG_DEBUG', "\tloop nv2: nv=%s" % nv)
                        debug(6, 'PFWCONFIG_DEBUG', "\tbefore loop sub: value=%s" % value)
                        valsub = re.sub("(?i)\$LOOP\{%s\}" % var, nv, value)
                        debug(6, 'PFWCONFIG_DEBUG', "\tafter loop sub: value=%s" % valsub)
                        if '$LOOP{' in valsub:
                            debug(6, 'PFWCONFIG_DEBUG', "\t\tputting back in todo list")
                            looptodo.append(valsub)
                        else:
                            valuedone.append(valsub)
                            debug(6, 'PFWCONFIG_DEBUG', "\t\tputting back in done list")
                debug(6, 'PFWCONFIG_DEBUG', "\tNumber in todo list = %s" % len(looptodo))
                debug(6, 'PFWCONFIG_DEBUG', "\tNumber in done list = %s" % len(valuedone))
            debug(6, 'PFWCONFIG_DEBUG', "\tEND OF WHILE LOOP = %s" % len(valuedone))
    
        if count >= maxtries:
            raise Exception("Interpolate function aborting from infinite loop\n. Current string: '%s'" % value)
    
        debug(6, 'PFWCONFIG_DEBUG', "\tvaluedone = %s" % valuedone)
        debug(6, 'PFWCONFIG_DEBUG', "\tvalue = %s" % value)
        debug(5, 'PFWCONFIG_DEBUG', "END")

        if len(valuedone) > 1:
            return valuedone
        elif len(valuedone) == 1:
            return valuedone[0]
        else:
            return value
    
    ###########################################################################
    def get_block_name(self, blocknum):
        """ Return block name based upon given block num """
        blocknum = int(blocknum)   # read in from file as string

        blockname = ''
        blockarray = re.sub(r"\s+", '', self.config['block_list']).split(',')
        if (0 <= blocknum) and (blocknum < len(blockarray)):
            blockname = blockarray[blocknum]
        return blockname

    
    ###########################################################################
    def get_condor_attributes(self, subblock):
        """Create dictionary of attributes for condor jobs"""
        attribs = {} 
        attribs[self.ATTRIB_PREFIX + 'isdesjob'] = 'TRUE'
        attribs[self.ATTRIB_PREFIX + 'project'] = self.config['project']
        attribs[self.ATTRIB_PREFIX + 'run'] = self.config['submit_run']
        attribs[self.ATTRIB_PREFIX + 'block'] = self.config['current']['curr_block']
        attribs[self.ATTRIB_PREFIX + 'operator'] = self.config['operator']
        attribs[self.ATTRIB_PREFIX + 'runsite'] = self.config['runsite']
        attribs[self.ATTRIB_PREFIX + 'subblock'] = subblock
#        if (subblock == '$(jobnum)'):
#            attribs[self.ATTRIB_PREFIX + 'numjobs'] = self.config['numjobs']
#            if ('glidein_name' in self.config):
#                attribs['GLIDEIN_NAME'] = self.config['glidein_name']
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
        return vals

    ###########################################################################
    def stagefile(self, opts):
        """ Determine whether should stage files or not """
        retval = True
        (notarget_exists, notarget) = self.search('notarget', opts)
        if (notarget_exists and notarget):  
            retval = False
        (stagefiles_exists, stagefiles) = self.search('stagefiles', opts)
        if (stagefiles_exists and not stagefiles):  
            retval = False
        return retval


    ###########################################################################
    def get_filename(self, filepat=None, searchopts=None):
        """ Return filename based upon given file pattern name """
        filename = ""

        if not filepat:
            # first check for filename pattern override 
            (found, filenamepat) = self.search('filename', searchopts)
        
            if not found:
                # get filename pattern from global settings:
                (found, filepat) = self.search('filepat', searchopts)

                if not found:
                    raise Exception("Could not find filepat")

        if filepat in self.config['filename_patterns']:
            filenamepat = self.config['filename_patterns'][filepat]
        else:
            raise Exception("Could not find filename pattern for %s" % filepat)
                
        filename = self.interpolate(filenamepat, searchopts)
        return filename


    ###########################################################################
    def get_filepath(self, pathtype, dirpat=None, searchopts=None):
        """ Return filepath based upon given pathtype and directory pattern name """
        filepath = ""

        # get filename pattern from global settings:
        if not dirpat:
            (found, dirpat) = self.search('dirpat', searchopts)

            if not found:
                raise Exception("Could not find dirpat")

        if dirpat in self.config['dir_patterns']:
            filepathpat = self.config['dir_patterns'][dirpat][pathtype]
        else:
            raise Exception("Could not find pattern %s in dir_patterns" % dirpat)
                
        filepath = self.interpolate(filepathpat, searchopts)
        return filepath

        
    ###########################################################################
    def combine_lists_files(self, modulename):
        """ Return python list of file and file list objects """
        print "\tModule %s\n" % (modulename)
        
        moduledict = self['module'][modulename]
        
        # create python list of files and lists for this module
        dataset = []
        if 'list' in moduledict and len(moduledict['list']) > 0:
            if 'list_order' in moduledict:
                listorder = moduledict['list_order'].replace(' ','').split(',')
            else:
                listorder = moduledict['list'].keys()
            for k in listorder:
                dataset.append((k, moduledict['list'][k]))
        else:
            print "\t\tNo lists"
        
        if 'file' in moduledict and len(moduledict['file']) > 0:
            for k,v in moduledict['file'].items():
                dataset.append((k,v))
        else:
            print "\t\tNo files"

        return dataset 


if __name__ ==  '__main__' :
    if len(sys.argv) == 2:
        pfw = PfwConfig({'wclfile': sys.argv[1]})
        #pfw.save_file(sys.argv[2])
        print 'block_list' in pfw
        print 'not_there' in pfw
        pfw.set_block_info()
        print pfw['blocknum']
        pfw.inc_block_num()
        print pfw['blocknum']
        pfw.reset_block_num()
        pfw.set_block_info()
        print pfw['blocknum']
