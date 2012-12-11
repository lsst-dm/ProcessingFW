#!/usr/bin/env python
# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit. 
# $LastChangedDate::                      $:  # Date of last commit.

""" Utilities for interactions with Condor """

import subprocess
import os
import re

class CondorException(Exception):
    "class for Condor exceptions"
    def __init__(self, txt):
        Exception.__init__(self)
        self.txt = txt
    def __str__(self):
        return self.txt


def condor_version():
    """Calls condor_version command and returns the version
       in string format easy to compare"""

    cmd = 'condor_version'

    try:
        process = subprocess.Popen(cmd.split(), shell=False, 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.STDOUT)
        process.wait()
        if process.returncode != 0:
            raise CondorException('Problem running condor_version - non-zero exit code')
    except:
        raise CondorException('Error: Could not run condor_version. Check PATH.')

    version = ''
    out = process.communicate()[0]
    result = re.search('CondorVersion: (\d+)\.(\d+)\.(\d+)', out)
    if result:
        version = '%03d.%03d.%03d' % (int(result.group(1)), 
            int(result.group(2)), int(result.group(3)))
    else:
        raise CondorException('Could not determine condor_version (%s)' % out)

    #print 'version = '%s'\n' % version
    return version


###########################################################################
def compare_condor_version(ver2):
    """Compare running condor version against given version"""
    # similar to strcmp 
    # < 0 if current < ver2
    #   0 if current = ver2 
    # > 0 if current > ver2

    if type(ver2) is float:
        ver2 = str(ver2)
    elif type(ver2) is not str:
        print "Invalid ver2 type: ", type(ver2), ver2 
        raise Exception("Invalid ver2 type")

    comp = 0
     
    # repad numbers to ensure easy comparision 
    result = re.search('(\d+)\.(\d+)\.(\d+)', ver2)
    if result:
        ver2 = '%03d.%03d.%03d' % (int(result.group(1)), 
               int(result.group(2)), int(result.group(3)))
    else:
        result = re.search('(\d+)\.(\d+)', ver2)
        if result:
            ver2 = '%03d.%03d.%03d' % (int(result.group(1)), 
                   int(result.group(2)), 0)
        else:
            raise CondorException('Invalid version format')

    currver = condor_version()
    if currver == ver2:
        comp = 0
    elif currver < ver2:
        comp = -1
    else:
        comp = 1
    
    return comp



###########################################################################
def condor_submit(submitfile):
    """Call condor_submit on given condor description file"""

    cmd = 'condor_submit %s' % (submitfile)

    try:
        process = subprocess.Popen(cmd.split(), shell=False,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)
        process.wait()
    except:
        raise CondorException('Error: Could not run condor_submit.  Check PATH.')

    return (process.returncode, process.communicate())


###########################################################################
def create_resource(info):
    """ Create string for globus_rsl line in condor description file """
    gridresource = ''

    print '\ncreateResource: ', info

    if 'gridresource' in info:
        gridresource += info['gridresource']
    else:
        # check for needed info to build string
        if 'gridtype' in info:
            gridtype = info['gridtype'].lower()
            if gridtype == 'prews':  # handle deprecated prews keyword
                gridtype = 'gt2'
            elif gridtype != 'gt2' and gridtype != 'gt5':
                raise CondorException('Invalid gridtype %s' % gridtype)
        else:
            gridtype = 'gt5'

        if 'gridhost' in info:
            gridhost = info['gridhost']
        else:
            raise CondorException('Missing gridhost')

        if 'batchtype' in info:
            batchtype = info['batchtype'].lower()
        else:
            raise CondorException('Missing batchtype')

        # create gridresource string
        gridresource = gridtype + ' ' + gridhost
        if 'gridport' in info:
            gridresource += ':' + info['gridport']
        gridresource += '/jobmanager-' + batchtype

    return gridresource


###########################################################################
def create_rsl(info):
    """Create RSL for grid job"""
    rslparts = []

    for key in ['stdout', 'stderr']:
        if key in info:
            rslparts.append('(%s=%s)' % (key, info[key]))

    if 'batchtype' in info:
        batchtype = info['batchtype'].lower()
        if batchtype != 'fork': 
            # used psn to distinguish from DESDM project
            if 'psn' in info:  
                rslparts.append('(project=%s)' % info['psn'])
                
            batchkeys = ('maxwalltime', 'maxtime', 'queue', 'jobtype', 
                         'maxmemory', 'minmemory', 'hostxcount', 'xcount', 
                         'hosttypes', 'count', 'reservationid')
            for key in batchkeys:
                if key in info:
                    rslparts.append('(%s=%s)' % (key, info[key]))

    if 'globusextra' in info:
        rslparts.append('%s' % info['globusextra'])
            
    if 'environment' in info:
        env = ''
        infoenv = info['environment'] 
        if type(infoenv) is dict:
            for (key, val) in infoenv.items():
                env += '(%s %s)' % (key.upper(), val)
        else:
            env = infoenv
        rslparts.append('(environment=%s)' % env)

    return ''.join(rslparts)


def create_condor_env(envvars):
    """Create string for environment line in condor description file"""
    # see rules in environment section of condor_submit manual page
    envparts = ['SUBMIT_CONDORID=$(Cluster).$(Process)']

    if type(envvars) is dict:
        for (key, val) in envvars.items():
            # Any literal double quote marks within the string must 
            # be escaped by repeating the double quote mark
            val = val.replace('"', '""')  

            # To insert a literal single quote mark, repeat the 
            # single quote mark anywhere inside of a section surrounded 
            # by single quote marks
            result = re.search("'", val)
            if result:
                val = "'%s'" % val.replace("'", "''")

            # Each environment entry has the form <name>=<value>
            # Use white space (space or tab characters) to separate 
            #     environment entries. 
            envparts.append('%s=%s' % (key.upper, val))
    else:
        envparts.append(envvars)
    
    # put double quote marks around the entire argument string.
    return '"%s"' % ' '.join(envparts)




def write_condor_descfile(jobname, filename, jobattribs, userattribs=None):
    """Creates <name>.condor description file 
       Assumes info contains valid condor key, value"""

    print 'write_condor_descfile', jobname
    print jobattribs

    # default some values if not given
    if 'log' not in jobattribs:
        jobattribs['log'] = '%s.log' % jobname

    if 'output' not in jobattribs:
        jobattribs['output'] = '%s.out' % jobname

    if 'error' not in jobattribs:
        jobattribs['error'] = '%s.err' % jobname

    if 'universe' not in jobattribs:
        jobattribs['universe'] = 'vanilla'

    condorfh = open(filename, 'w')

    for key, val in jobattribs.items():
        condorfh.write('%s = %s\n' % (key, val))

    if userattribs:
        for key, val in userattribs.items():
            print key, val,
            if val.lower() != 'true' and val.lower() != 'false':
                val = '"%s"' % val
            print val
            condorfh.write('+%s = %s\n' % (key, val))

    condorfh.write('queue\n')
    condorfh.close()



def parse_condor_user_log(logfilename):
    """parses a condor log into a dictionary"""
     
    #print "parse_condor_user_log:  logfilename=", logfilename
    log = open(logfilename)
    lines = log.read().split('\n...\n') 
    log.close()

    jobinfo = {}
    for line in lines:
        if re.search('\S', line):
            splitline = line.split('\n')   
            result = re.match('(\d+)\s+\((\d+).\d+.\d+\)\s+(\d+\/\d+\s+\d+:\d+:\d+)\s+(.+)', splitline[0])
            if result:
                code = result.group(1)
                jobnum = result.group(2)
                time = result.group(3)
                #desc = result.group(4)
    
                if code == '000':
                    jobinfo[jobnum] = { 'jobid':jobnum,
                                        'clusterid':jobnum,
                                        'machine':'',
                                        'jobstat':'UNSUB',
                                        'submittime':time }
                    if len(splitline) > 1:
                        result = re.match('\s*DAG Node:\s+(\S+)\s*', splitline[1])
                        if result:
                            jobinfo[jobnum]['jobname'] = result.group(1)
                elif code == '001':
                    jobinfo[jobnum]['jobstat'] = 'RUN'
                    jobinfo[jobnum]['starttime'] = time
                #elif code == '002':
                #    pass  # Error in executable
                #elif code == '003':
                #    pass  # Job was checkpointed
                #elif code == '004':
                #    pass  # Job evicted from machine
                elif code == '005':
                    jobinfo[jobnum]['jobstat'] = 'DONE'
                    jobinfo[jobnum]['endtime'] = time
                    result = re.search('return value (\d+)', splitline[1]) 
                    if result:
                        jobinfo[jobnum]['retval'] = result.group(1)
                #elif code == '006':
                #    pass  # Image size of job updated
                #elif code == '007':
                #    pass  # Shadow threw an exception
                #elif code == '008':
                #    pass  # Generic Log Event
                elif code == '009':
                    jobinfo[jobnum]['jobstat'] = 'FAIL'
                    jobinfo[jobnum]['endtime'] = time
                #elif code == '010':
                #    pass  # Job was suspended
                #elif code == '011': 
                #    pass  # Job was unsuspended
                elif code == '012':
                    jobinfo[jobnum]['jobstat'] = 'ERR'
                    result = re.search('(\S+)', splitline[1])
                    if result:
                        jobinfo[jobnum]['holdreason'] = result.group(1)
                elif code == '013':
                    jobinfo[jobnum]['jobstat'] = 'UNSUB'
                #elif code == '014':
                #    pass  # Parallel Node executed
                #elif code == '015': 
                #    pass  # Parallel Node terminated
                elif code == '016':
             #016 (471.000.000) 04/11 11:48:08 POST Script terminated.
             #        (1) Normal termination (return value 100)
             #    DAG Node: fail
             #...
                    jobinfo[jobnum]['endtime'] = time
                    result = re.search('return value (\d+)', splitline[1]) 
                    if result:
                        retval = result.group(1)
                        if retval == 100:
                            jobinfo[jobnum]['jobstat'] = 'FAIL'
                        else:
                            jobinfo[jobnum]['jobstat'] = 'DONE'
                elif code == '017':  #  Job submitted to Globus
                    #  Beware of out of order log entries
                    if ('starttime' not in jobinfo[jobnum] or 
                        (jobinfo[jobnum]['starttime'] != time)):
                        jobinfo[jobnum]['jobstat'] = 'PEND'
                    result = re.search('RM-Contact:\s+(\S+)', splitline[1])
                    if result:
                        jobinfo[jobnum]['gridresource'] = result.group(1)
                #elif code == '018': 
                #    pass  # Globus Submit failed
                #elif code == '019':  
                #    pass  # Globus Resource Up
                #elif code == '020': 
                #    pass  # Globus Resource Down
                #elif code == '021': 
                #    pass  # Remote Error
                #elif code == '027': 
                #    pass  # Job submitted to grid resource, 
                #          # same info as case 017
                else: 
                    jobinfo[jobnum]['jobstat'] = 'U%s' % (code)
            else:
                print 'warning unknown line: %s'  % (line)
            

    return jobinfo


def condor_q(args_str=''):
    """Given condor_q args, calls condor_q -l [args] and parses output into dictionary"""

    qjobs = {}
    job = {}
    condorid = -9999

    condorq_cmd = ['condor_q', '-l']
    condorq_cmd.extend(args_str.split())
    
    try:
        process = subprocess.Popen(condorq_cmd, 
                                   shell=False, 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE)
        out = ""
        buf = os.read(process.stdout.fileno(), 5000)
        while process.poll() == None or len(buf) != 0:
            out += buf
            buf = os.read(process.stdout.fileno(), 5000)

        if process.returncode != 0:
            print "Cmd = ", condorq_cmd
            raise CondorException('Problem running condor_q - non-zero exit code'+process.communicate()[0])
    except Exception as err:
        raise CondorException('Error: Could not run condor_q. Check PATH.\n'+str(err))
        
    
    lines = out.split('\n')
    for line in lines:
        if re.match('--', line):  # skip condor_q line starting with --
            pass
        elif not re.search('\S', line):
            if len(job) > 0:   # blank lines separate jobs
                qjobs[condorid] = dict(job)
                job.clear()
                condorid = -9999
        else:
            # divide line into key/value pair
            result = re.search('(\S+)\s*=\s*(\S+)', line)
            key = result.group(1).lower()
            value = re.sub('"', '', result.group(2))

            # there are 2 args, make sure to appropriately store condor args
            if re.search('args', key) and re.match('-f', value):
                key = 'condorargs'
            job[key] = value
            if re.match('clusterid', key):
                condorid = value   # save clusterid as key for qjobs dict
 
    # don't forget to save the last job into big hash table
    if len(job) > 0:
        qjobs[condorid] = dict(job)
        job.clear()

    return qjobs


def condorq_dag(args_str=''):
    """ Call condor_q and return in dag trees """

    qjobs = condor_q(args_str)

    top_jobs = []  # top dagman jobs
    orphan_jobs = []  # jobs whose parents aren't in queue or non-dagman jobs
 
    for jobid, jobinfo in qjobs.iteritems():
        if not 'children' in jobinfo:
            jobinfo['children'] = []

        if 'dagmanjobid' in jobinfo: # should have parent
            if jobinfo['dagmanjobid'] in qjobs:  # if have parent 
                if 'children' in qjobs[jobinfo['dagmanjobid']]:
                    qjobs[jobinfo['dagmanjobid']]['children'].append(jobid)
                else:
                    qjobs[jobinfo['dagmanjobid']]['children'] = [jobid]
            else:
                orphan_jobs.append(jobid)  # lost parent
        else:
            if 'dagman' in os.path.basename(qjobs[jobid]['cmd']): 
                top_jobs.append(jobid)
            else:  # either saveruntime job or operator manually running job
                orphan_jobs.append(jobid)  

    return (qjobs, top_jobs, orphan_jobs)
     


######################################################################
def add2dag(dagfile, cmdopts, attributes, initialdir, debugfh):
    """ Create the condor description file for a DAG with added attributes """ 
    print "add2dag: cwd =", os.getcwd()
    cmd = 'condor_submit_dag -f -no_submit -notification never '

    assert(type(cmdopts) == dict)
    assert(type(attributes) == dict)
        
    if compare_condor_version('7.6.0') >= 0:
        cmd += ' -autorescue 0 -no_recurse '
    elif compare_condor_version('7.1.0') >= 0:
        cmd += ' -oldrescue 1 -autorescue 0 -no_recurse '
    else:
        raise Exception('Using condor that is too old')

    if 'dagman_max_pre' in cmdopts:
        cmd += ' -MaxPre %s' % cmdopts['dagman_max_pre']

    if 'dagman_max_post' in cmdopts:
        cmd += ' -MaxPost %s' % cmdopts['dagman_max_post']

    if 'dagman_max_jobs' in cmdopts:
        cmd += ' -maxjobs %s' % cmdopts['dagman_max_jobs']

    if 'dagman_max_idle' in cmdopts:
        cmd += ' -maxidle %s' % cmdopts['dagman_max_idle']

    # write additional lines to file and ask condor_submit_dag to include
    #    note: insert_sub_file works with empty file
    if compare_condor_version("7.1") > 0: 
        addfile = dagfile + '.add.txt'
        with open(addfile, 'w') as addfh:
            if initialdir:
                addfh.write("initialdir=%s\n" % initialdir)
            for key, val in attributes.items():
                if val.lower() != 'true' and val.lower() != 'false':
                    val = '"%s"' % val
                addfh.write('+%s=%s\n' % (key, val))
        cmd += ' -insert_sub_file ' + addfile

    cmd += ' ' + dagfile

    debugfh.write('cmd> %s\n' % (cmd))
    process = subprocess.Popen(cmd.split(), shell=False,
                               stdout=debugfh,
                               stderr=debugfh)
    process.wait()
    stat = process.returncode
    print "stat = ", stat
    debugfh.write('condor_submit_dag exit code: %s\n' % stat)

    if stat == 0:
        dagfile += '.condor.sub'

        condorfh = open(dagfile, 'r')
        condorstr = condorfh.read()
        condorfh.close()

        # Work around condor_submit_dag bug (6.7.20, 6.8.0-6.8.3, 6.9.1)
        # 'The OnExitRemove expression generated for DAGMan by 
        # condor_submit_dag evaluated to UNDEFINED for some values 
        # of ExitCode, causing condor_dagman to go on hold.'
        result = re.search('on_exit_remove\s*=\s*\(\s*ExitSignal\s*==\s*11\s*||\s*\(ExitCode\s*>=0\s*&&\s*ExitCode\s*<=\s*2\)\)', condorstr)
        if result:            
            condorstr.replace('on_exit_remove\s+=[^\n]+\n', 
                'on_exit_remove = ( ExitSignal =?= 11 || (ExitCode =!= UNDEFINED && ExitCode >=0 && ExitCode <= 2))\n')

#        if attributes and len(attributes) > 0:
#            add2condor(condorstr, attributes, debugfh)

        condorfh = open(dagfile, 'w')
        condorfh.write(condorstr)
        condorfh.close()
    else:
        raise CondorException("condor_submit_dag failed")


######################################################################
def add2condor(condorstr, attributes, debugfh):
    """add some attributes to condor submit file"""

    debugfh.write('add2condor')
    debugfh.write('Pre-change\n')
    debugfh.write('============\n')
    debugfh.write(condorstr)
    debugfh.write('\n============\n')

    # add attributes to condor submit file 
    print attributes
    info = ''
    for key, val in attributes.items():
        info += '+' + key + '="' + val + '"\n'
    info += '\nqueue\n'
    condorstr.replace('\nqueue', info)

    debugfh.write('Post-change\n')
    debugfh.write('============\n')
    debugfh.write(condorstr)
    debugfh.write('\n============\n')




def check_condor(minver):
    """ Check for Condor in path as well as daemons running """

    # checking condor executables are in path
    cmd = 'condor_submit notthere.condor'
    try:
        process = subprocess.Popen(cmd.split(), shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
        process.wait()
    except:
        raise CondorException('Could not find condor_submit\n' + \
               'Make sure Condor binaries are in your path')

    # checking running on this machine
    cmd = 'condor_q'
    process = subprocess.Popen(cmd.split(), shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
    process.wait()
#    if process.returncode:
#        raise CondorException('Condor is not running on this machine' + \
#                              'Contact your condor administrator')

    # check have new enough version of condor
    if compare_condor_version(minver) < 0:
        raise CondorException('Condor version must be at least ' + minver)



def get_grid_proxy_timeleft():
    """ Check timeleft on grid proxy """

    cmd = 'grid-proxy-info -timeleft'
    process = subprocess.Popen(cmd.split(), shell=False,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)
    process.wait()

    out = process.communicate()[0]
    timeleft = -1
    if process.returncode == 0:
        result = re.search('ERROR', out)
        if not result:
            timeleft = int(out)
    return timeleft


def get_job_status_str(jobnum, qjobs):
    statusstr = "UNK"

    # Condor Job Status: 
    #    1 = Idle, 2 = Running, 3 = Removed, 4 = Completed, and 5 = Held
    condorstatus = {'1':"PEND", '2':"RUN",'3':"DEL",'4':"DONE",'5':"ERR"}
    # Grid job status:
    #    1 = Pend, 2 = Running, 32 = Unsub
    gridstatus = {'1':"PEND", '2':"RUN", '32':"UNSUB"}

    statusnum = 0
    if jobnum in qjobs and 'jobstatus' in qjobs[jobnum]:
        statusnum = qjobs[jobnum]['jobstatus']
        if statusnum in condorstatus:
            statusstr = condorstatus[statusnum]

        # if grid job, use remote status
        if statusnum == 1:
            if 'jobuniverse' in qjobs[jobnum] and \
                qjobs[jobnum]['jobuniverse'] == 9 and \
                'globusstatus' in qjobs[jobnum]:

                if qjobs[jobnum]['globusstatus'] in gridstatus:
                    statusstr = gridstatus[qjobs[jobnum]['globusstatus']]

    return(statusstr)


if __name__ ==  '__main__' :
    pass
