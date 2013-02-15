#!/usr/bin/env python

import re
import os
import sys
import inspect

""" Miscellaneous support functions for framework """

#######################################################################
def fwdebug(msglvl, envdbgvar, msgstr):
    # environment debug variable overrides code set level
    if envdbgvar in os.environ:
        dbglvl = os.environ[envdbgvar]
    elif '_' in envdbgvar:
        prefix = envdbgvar.split('_')[0]
        if '%s_DEBUG' % prefix in os.environ:
            dbglvl = os.environ['%s_DEBUG' % prefix]
        else:
            dbglvl = 0
    else:
        dbglvl = 0

    if int(dbglvl) >= int(msglvl): 
        print "%s: %s" % (inspect.stack()[1][3], msgstr)


#######################################################################
def fwdie(msg, exitcode, depth=1):
    frame = inspect.stack()[depth]
    file = os.path.basename(frame[1])
    print "\n\n%s:%s:%s: %s" % (file, frame[3], frame[2], msg) 
    
    sys.exit(exitcode)


#######################################################################
def fwsplit(fullstr, delim=','):
    """ Split by delim and trim substrs """
    fullstr = re.sub('[()]', '', fullstr) # delete parens if exist
    items = []
    for item in [x.strip() for x in fullstr.split(delim)]:
        if ':' in item:
            rangevals = fwsplit(item, ':')
            items.extend(map(str, range(int(rangevals[0]), 
                                        int(rangevals[1])+1)))
        else:
            items.append(item)
    return items

def convertBool(var):
    #print "Before:", var, type(var)
    newvar = None
    if var is not None:
        tvar = type(var)
        if tvar == int:
            newvar = bool(var)
        elif tvar == str:
            try:
                newvar = bool(int(var))
            except ValueError:
                if var.lower() in ['y','yes','true']:
                    newvar = True
                elif var.lower() in ['n','no','false']:
                    newvar = False
        elif tvar == bool:
            newvar = var
        else:
            raise Exception("Type not handled (var, type): %s, %s" % (var, type(var)))
    else:
        newvar = False
    #print "After:", newvar, type(newvar)
    #print "\n\n"
    return newvar
