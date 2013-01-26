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

from processingfw.pfwdefs import *
import intgutils.wclutils as wclutils
import processingfw.pfwdb as pfwdb

from pfwutils import debug
from pfwutils import pfwsplit
import pfwutils

class PfwConfig:
    """ Contains configuration and state information for PFW """

    # order in which to search for values
    DEFORDER = [SW_FILESECT, SW_LISTSECT, 'exec', 'job', SW_MODULESECT, SW_BLOCKSECT, 'archive', 'site']

    ###########################################################################
    def __init__(self, args):
        """ Initialize configuration object, typically reading from wclfile """
        self.config = OrderedDict()

        if 'debug' in args:
            self.config['debug'] = args['debug']
        else:
            self.config['debug'] = 0

        if 'pfwconfig' in args:
            pfwconfig = os.environ['PROCESSINGFW_DIR'] + '/etc/pfwconfig.des' 
            debug(3, 'PFWCONFIG_DEBUG', "Reading pfwconfig: %s" % (pfwconfig))
            fh = open(pfwconfig, "r")
            wclutils.updateDict(self.config, wclutils.read_wcl(fh))
            fh.close()

        if 'querydb' in args and args['querydb']:
            dbh = pfwdb.PFWDB()
            wclutils.updateDict(self.config, dbh.get_database_defaults())

        if 'wclfile' in args:
            debug(3, 'PFWCONFIG_DEBUG', "Reading wclfile: %s" % (args['wclfile']))
            fh = open(args['wclfile'], "r")
            wclutils.updateDict(self.config, wclutils.read_wcl(fh))
            fh.close()
            if 'debug' not in self.config:  # recheck since reset config
                self.config['debug'] = 0
            self.config['wclfile'] = args['wclfile']

#        runwcl = OrderedDict()
#        if 'wclfile' in args:
#            debug(3, 'PFWCONFIG_DEBUG', "Reading wclfile: %s" % (args['wclfile']))
#            fh = open(args['wclfile'], "r")
#            runwcl = wclutils.read_wcl(fh)
#            fh.close()
#
#        pfwwcl = OrderedDict()
#        dbwcl = OrderedDict()
#        opwcl = OrderedDict()
#        if '_config' in runwcl:
#            if 'pfwconfig' in runwcl['_config']:
#                #pfwconfig = os.environ['PROCESSINGFW_DIR'] + '/etc/pfwconfig.des' 
#                pfwconfig = runwcl['_config']['pfwconfig']
#                debug(3, 'PFWCONFIG_DEBUG', "Reading pfwconfig: %s" % (pfwconfig))
#                fh = open(pfwconfig, "r")
#                pfwwcl = wclutils.read_wcl(fh)
#                fh.close()
#
#            if 'dbconfig' in runwcl['_config']:
#                dbconfig = runwcl['_config']['dbconfig']
#                debug(3, 'PFWCONFIG_DEBUG', "Reading dbconfig: %s" % (dbconfig))
#                fh = open(dbconfig, "r")
#                dbwcl = wclutils.read_wcl(fh)
#                fh.close()
#
#            if 'opconfig' in runwcl['_config']:
#                opconfig = runwcl['_config']['opconfig']
#                debug(3, 'PFWCONFIG_DEBUG', "Reading opconfig: %s" % (opconfig))
#                fh = open(opconfig, "r")
#                opwcl = wclutils.read_wcl(fh)
#                fh.close()
#
#        # combine configs
#        self.config = pfwwcl
#        wclutils.updateDict(self.config, dbwcl)
#        wclutils.updateDict(self.config, opwcl)

        self.set_names()

        if NOTARGET in args:
            self.config[NOTARGET] = args[NOTARGET]

        # during runtime save blocklist as array
        self.block_array = pfwsplit(self.config[SW_BLOCKLIST])
        self.config['num_blocks'] = len(self.block_array)
    
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
            self.config[WRAPNUM] = '0'
            self.config[PF_BLKNUM] = '1'
            self.config[TASKNUM] = '0'
            self.config[JOBNUM] = '1'

        self.set_block_info()

    ###########################################################################
    def save_file(self, filename):
        """Saves configuration in WCL format"""
        fh = open(filename, "w")
        wclutils.write_wcl(self.config, fh, True, 4)  # save it sorted
#        wclutils.write_wcl(self.config['_config'], fh, True, 4)  # save it sorted
#        wclutils.write_wcl(self.config['current'], fh, True, 4)  # save it sorted
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
        debug(8, 'PFWCONFIG_DEBUG', "\tBEG")
        debug(8, 'PFWCONFIG_DEBUG',
                 "\tinitial key = '%s'" % key)
        debug(8, 'PFWCONFIG_DEBUG',
                 "\tinitial opts = '%s'" % opt)

        found = False
        value = ''
        key = key.lower()

        # if key contains period, use it exactly instead of scoping rules
        if '.' in key:
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
            raise Exception("Search failed (%s)" % key)
    
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
        if SW_BLOCKSECT not in self.config:
            raise Exception('Error: Could not find block section')
        if SW_MODULESECT not in self.config:
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

        if REQNUM not in self.config:
            print "Error: missing reqnum"
            errcnt += 1

        if ATTNUM not in self.config:
            print "Error: missing attnum"
            errcnt += 1

        if UNITNAME not in self.config:
            print "Error: missing unitname"
            errcnt += 1

        # targetnode replaces depricated archive_node
        if 'archive_node' in self.config:
            if 'targetnode' in self.config:
                print "\tWarning: have both targetnode and depricated archive_node defined in global section."
                warncnt += 1
                if cleanup:
                    print "\tDeleting depricated archive_node"
                    del self.config['archive_node']
                    cleancnt += 1
            else:
                print "\tWarning: depricated use of archive_node in global section."
                warncnt += 1
                if cleanup:
                    print "\tSetting global targetnode = global archive_node"
                    self.config['targetnode'] = self.config['archive_node']
                    print "\tDeleting depricated archive_node"
                    del self.config['archive_node']
                    cleancnt += 1
                
        # submitnode must be set globally
        submitnode = None
        if 'submitnode' not in self.config:
            print 'Error: submitnode is not specified.'
            errcnt += 1
#        elif self.config['submitnode'] not in self.config['archive']:
#            print 'Error:  Could not find archive information for submit node %s' % self.config['submitnode']
#            errcnt += 1
#        elif 'archive_root' not in self.config['archive'][self.config['submitnode']]:
#            print 'Error:  archive_root not specified for submit node %s' % self.config['submitnode']
#            errcnt += 1
#        elif 'site_id' not in self.config['archive'][self.config['submitnode']]:
#            print 'Error: site_id not specified for submit node %s' % self.config['submitnode']
#            errcnt += 1
#        else:
#            submitnode = self.config['submitnode']
#            archiveroot = self.config['archive'][submitnode]['archive_root']
#            if not os.path.exists(archiveroot):
#                print 'Warning: archive_root (%s) from submitnode does not exist on disk' % archiveroot
#                warncnt += 1
#    
#            submit_siteid = self.config['archive'][submitnode]['site_id']
#            if submit_siteid not in self.siteid2name:
#                print 'Error: Could not find site information for site %s from submit node info.' % submit_siteid
#                errcnt += 1
#                submit_siteid = None
#            elif 'loginhost' not in self.config['site'][self.siteid2name[submit_siteid]]:
#                print 'Error:  loginhost is not defined for submit site %s (%s).\n' % (self.siteid2name[submit_siteid], submit_siteid)
#                errcnt += 1
#            elif os.uname()[1] != self.config['site'][self.siteid2name[submit_siteid]]['loginhost']:
#                print 'Error:  submit node %s (%s) does not match submit host (%s).' % (submitnode, self.config['site'][self.siteid2name[submit_siteid]]['loginhost'], os.uname()[1])
#                print 'Debugging tips: '
#                print '\tCheck submitnode value, '
#                print '                Check correct site_id defined for submitnode,'
#                print '\tcheck loginhost defined for site linked to submitnode'
#                   errcnt += 1
    
    
        # Check block definitions for simple single module blocks.
        # Also check all blocks in blocklist have definitions as well as all modules in their modulelists
        if SW_BLOCKLIST not in self.config:
            print "Error: missing %s" % SW_BLOCKLIST 
        else:
            self.config[SW_BLOCKLIST] = re.sub(r"\s+", '', self.config[SW_BLOCKLIST].lower())
            blocklist = self.config[SW_BLOCKLIST].split(',')
    
            for blockname in blocklist:
                print "\tChecking block:", blockname
                if blockname in self.config[SW_BLOCKSECT]:
                    block = self.config[SW_BLOCKSECT][blockname]
                    if SW_MODULELIST in block:
                        block[SW_MODULELIST] = re.sub(r"\s+", '', block[SW_MODULELIST].lower())
                        modulelist = block[SW_MODULELIST].split(',')

                        for modulename in modulelist:
                            if modulename not in self.config[SW_MODULESECT]:
                                print "\tError: missing definition for module %s from block %s" % (modulename, blockname)
                                errcnt += 1
                    elif blockname in self.config[SW_MODULESECT]:
                        print "\tWarning: Missing modulelist definition for block %s" % (blockname)
                        if cleanup:
                            print "\t         Defaulting to modulelist=%s" % (blockname)
                        block[SW_MODULELIST] = blockname
                    else:
                        print "\tError: missing modulelist definition for block %s" % (blockname)
                        errcnt += 1
                else:
                    if blockname in self.config[SW_MODULESECT]:
                        print "\tWarning: Missing block definition for %s" % blockname
                        if cleanup:
                            print "\t         Creating new block definition with modulelist=%s" % (blockname)
                            self.config[SW_BLOCKSECT][blockname] = { SW_MODULELIST: blockname }
                            block = self.config[SW_BLOCKSECT][blockname]
                    else:
                        print "\tError: missing definition for block %s" % (blockname)
                        errcnt += 1
    
                if block: 
                    if 'archive_node' in block:
                        if 'targetnode' in block:
                            print "\tWarning:  Have both archive_node and targetnode defined in block %s" % (blockname)
                            warncnt += 1
                            if cleanup:
                                print "\t\tDeleting depricated archive_node"
                                del block['archive_node']
                                cleancnt += 1
                        else:
                            print "\tWarning:  deprecated archive_node defined in block %s" % (blockname)
                            warncnt += 1
                            if cleanup:
                                print "\t\tSetting targetnode = archive_node"
                                block['targetnode'] = block['archive_node']
                                print "\t\tDeleting depricated archive_node"
                                del block['archive_node']
                                cleancnt += 1
    
                    if 'targetnode' in block:
                        targetnode = block['targetnode']
                    elif 'targetnode' in self.config:
                        targetnode = self.config['targetnode']
                    else:
                        print "\tError: Could not determine targetnode for block %s" % (blockname)
                        errcnt += 1
    
                    target_sitename = None
                    if targetnode not in self.config['archive']:
                        print "\tError: missing definition for target node %s from block %s" % (targetnode, blockname)
                        errcnt += 1
                    elif 'sitename' not in self.config['archive'][targetnode]:
                        print "\tError: missing sitename for target node %s from block %s" % (targetnode, blockname)
                        errcnt += 1
    
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
        self.config[JOBNUM] = '1'
        self.config[PF_BLKNUM] = '1'
        self.config[TASKNUM] = '0'
        self.config[WRAPNUM] = '0'
        self.set_block_info()
    
        self.config['submit_run'] = self.interpolate("${unitname}_r${reqnum}p${attnum:2}")
        self.config['run'] = self.config['submit_run']
    
        work_dir = self.config['submit_dir'] + '/' + \
                   os.path.splitext(self.config['submitwcl'])[0] + \
                   '_' + submit_time
        self.config['work_dir'] = work_dir
        self.config['uberctrl_dir'] = work_dir + "/runtime/uberctrl"
    
    
    ###########################################################################
    def set_block_info(self):
        """ Set current vals to match current block number """
        debug(1, 'PFWCONFIG_DEBUG', "BEG")

        curdict = self.config['current']
        debug(4, 'PFWCONFIG_DEBUG', "\tcurdict = %s" % (curdict))

        blknum = self.config[PF_BLKNUM]

        blockname = self.get_block_name(blknum) 
        if not blockname:
            raise Exception("Error: set_block_info cannot determine block name value for blknum=%s" % blknum)
        curdict['curr_block'] = blockname
    
        (exists, targetnode) = self.search('targetnode')
        if not exists:
            raise Exception("Error: set_block_info cannot determine targetnode value")
    
        if targetnode not in self.config['archive']:
            print "Error: invalid targetnode value (%s)" % targetnode
            print "\tArchive contains: ", self.config['archive']
            raise Exception("Error: invalid targetnode value (%s)" % targetnode)
    
        curdict['curr_archive'] = targetnode
    
        if 'listtargets' in self.config:
            listt = self.config['listtargets']
            if not targetnode in listt:  # assumes targetnode names are not substrings of one another
                self.config['listtargets'] += ',' + targetnode
        else:
            self.config['listtargets'] = targetnode
        
#depricated?        curdict['curr_software'] = self['software_node']
    
        (exists, sitename) = self.search('sitename')
        if exists and sitename in self.config['site']:
            self.config['runsite'] = sitename
            curdict['curr_site'] = sitename
        else:
            raise Exception('Error: set_block_info cannot determine run_site value')
        debug(1, 'PFWCONFIG_DEBUG', "END") 

    
    ###########################################################################
    def inc_blknum(self):
        """ increment the block number """
        # note config stores numbers as strings
        self.config[PF_BLKNUM] = str(int(self.config[PF_BLKNUM]) + 1)
        self.config[TASKNUM] = '0'
    
    ###########################################################################
    def reset_blknum(self):
        """ reset block number to 1 """
        self.config[PF_BLKNUM] = '1'
        self.config[TASKNUM] = '0'
    
    ###########################################################################
    def inc_jobnum(self, inc):
        """ Increment running job number """
        self.config[JOBNUM] = str(int(self.config[JOBNUM]) + inc)
    

    ###########################################################################
    def inc_tasknum(self, inc=1):
        """ Increment blktask number """
        self.config[TASKNUM] = str(int(self.config[TASKNUM]) + inc)
        return self.config[TASKNUM]
        

    ###########################################################################
    def inc_wrapnum(self):
        """ Increment running wrapper number """
        self.config[WRAPNUM] = str(int(self.config[WRAPNUM]) + 1)

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
                debug(6, 'PFWCONFIG_DEBUG', "\twhy req: newvar: %s " % (newvar))
                if len(parts) > 1:
                    prpat = "%%0%dd" % int(parts[1])
                (haskey, newval) = self.search(newvar, opts)
                debug(6, 'PFWCONFIG_DEBUG', 
                      "\twhy req: haskey, newvar, newval, type(newval): %s, %s %s %s" % (haskey, newvar, newval, type(newval)))
                if haskey:
                    newval = str(newval)
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
                    raise Exception("Could not find value for %s" % newvar)
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
        attribs[ATTRIB_PREFIX + 'run'] = self.config['submit_run']
        attribs[ATTRIB_PREFIX + 'block'] = self.config['current']['curr_block']
        attribs[ATTRIB_PREFIX + 'operator'] = self.config['operator']
        attribs[ATTRIB_PREFIX + 'runsite'] = self.config['runsite']
        attribs[ATTRIB_PREFIX + 'subblock'] = subblock
#        if (subblock == '$(jobnum)'):
#            attribs[ATTRIB_PREFIX + 'numjobs'] = self.config['numjobs']
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
                else:
                    print "Could not find value for %s(%s)" % (key, newkey)
    
        print "get_grid_info:  returning vals=", vals
        return vals

    ###########################################################################
    def stagefile(self, opts):
        """ Determine whether should stage files or not """
        retval = True
        (notarget_exists, notarget) = self.search(NOTARGET, opts)
        if (notarget_exists and notarget):  
            retval = False
        (stagefiles_exists, stagefiles) = self.search(STAGEFILES, opts)
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
                (found, filepat) = self.search(SW_FILEPAT, searchopts)

                if not found:
                    raise Exception("Could not find file pattern %s" % SW_FILEPAT)

        
        if SW_FILEPATSECT not in self.config:
            wclutils.write_wcl(self.config)
            raise Exception("Could not find filename pattern section (%s)" % SW_FILEPATSECT)
        elif filepat in self.config[SW_FILEPATSECT]:
            filenamepat = self.config[SW_FILEPATSECT][filepat]['pattern']
        else:
            print SW_FILEPATSECT, " keys: ", self.config[SW_FILEPATSECT].keys()
            raise Exception("Could not find filename pattern for %s" % filepat)
                
        filename = self.interpolate(filenamepat, searchopts)
        return filename


    ###########################################################################
    def get_filepath(self, pathtype, dirpat=None, searchopts=None):
        """ Return filepath based upon given pathtype and directory pattern name """
        filepath = ""
       
        # get filename pattern from global settings:
        if not dirpat:
            (found, dirpat) = self.search(SW_DIRPAT, searchopts)

            if not found:
                raise Exception("Could not find dirpat")

        if dirpat in self.config[SW_DIRPATSECT]:
            filepathpat = self.config[SW_DIRPATSECT][dirpat][pathtype]
        else:
            raise Exception("Could not find pattern %s in directory patterns" % dirpat)
                
        filepath = self.interpolate(filepathpat, searchopts)
        return filepath

        
    ###########################################################################
    def combine_lists_files(self, modulename):
        """ Return python list of file and file list objects """
        print "\tModule %s\n" % (modulename)
        
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
            print "\t\tNo lists"
        
        if SW_FILESECT in moduledict and len(moduledict[SW_FILESECT]) > 0:
            for k,v in moduledict[SW_FILESECT].items():
                dataset.append((k,v))
        else:
            print "\t\tNo files"

        return dataset 

    def set_names(self):
        """ set names for use in patterns (i.e., blockname, modulename) """

        for blk, blkdict in self.config[SW_BLOCKSECT].items():
            if 'blockname' not in blkdict:
                blkdict['blockname'] = blk 
    
        for mod, moddict in self.config[SW_MODULESECT].items():
            if 'modulename' not in moddict:
                moddict['modulename'] = mod 


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
