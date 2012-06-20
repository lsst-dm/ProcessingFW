# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
    Define some exceptions.

    Developed at: 
    The National Center for Supercomputing Applications (NCSA).
  
    Copyright (C) 2012 Board of Trustees of the University of Illinois. 
    All rights reserved.
"""

__version__ = "$Rev$"

class MetadataConfigError (Exception):
    "Represent an error in the METADATA and/or FILETYPE tables."

    def __init__ (self, msg):
        Exception.__init__ (self, msg)

class DuplicateDBFiletypeError (MetadataConfigError):
    "Duplicate filetype in the FILETYPE table in the database."

    def __init__ (self, msg = None):
        if not msg:
            msg = 'Filetypes differing only by case exist in filetype table.'
        MetadataConfigError.__init__ (self, msg)

class DuplicateDBHeaderError (MetadataConfigError):
    "Duplicate header in the METADATA table in the database."

    def __init__ (self, msg = None):
        if not msg:
            msg = 'Header names differing only by case exist in metadata table.'
        MetadataConfigError.__init__ (self, msg)

class IdMetadataHeaderError (MetadataConfigError):
    "Disallowed id header found in METADATA table in the database."

    def __init__ (self, msg = None):
        if not msg:
            msg = 'Id header found in metadata table.'
        MetadataConfigError.__init__ (self, msg)
