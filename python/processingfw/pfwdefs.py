# when changing values, check if change also needed in $PROCESSINGFW_DIR/etc/pfwconfig.des

FILETYPE  = 'filetype'
FILENAME  = 'filename'
METATABLE = 'metadata_table'
USED  = 'used'
WGB   = 'was_generated_by'
WDF   = 'was_derived_from'

INPUTS = USED
OUTPUTS = WGB
ANCESTRY = WDF


# SW_  submit wcl
# IW_  (wrapper) input wcl
# OW_  (wrapper) output wcl
# PF_  processing fw 
# DB_  database table/column names

REQNUM = 'reqnum'
ATTNUM = 'attnum'
UNITNAME = 'unitname'

SW_DIRPAT = 'dirpat'
SW_FILEPAT = 'filepat'
SW_BLOCKLIST = 'blocklist'
SW_MODULELIST = 'modulelist'
SW_BLOCKSECT = 'block'
SW_MODULESECT = 'module'
SW_LISTSECT = 'list'
SW_FILESECT = 'file'
SW_FILEPATSECT = 'filename_pattern'
SW_DIRPATSECT = 'directory_pattern'
SW_QUERYFIELDS = 'query_fields'
SW_EXECPREFIX = 'myexec_'

WRAPSECT = 'wrapper'
PROVSECT = 'provenance'
METASECT = 'file_metadata'
IW_LISTSECT = 'list'
IW_FILESECT = 'filespecs'
IW_EXECPREFIX = 'exec_'
OW_EXECPREFIX = IW_EXECPREFIX

TASKNUM = 'tasknum'
JOBNUM = 'jobnum'
WRAPNUM = 'wrapnum'
LISTENTRY = 'line'
NOTARGET = 'notarget'
STAGEFILES = 'stagefiles'


ATTRIB_PREFIX='des_'
PF_SUCCESS = 0
PF_REPEAT = 100
PF_FAILURE = 10
PF_OPDELETE = 5
PF_NOTARGET = 2
PF_WARNINGS = 3

PF_BLKNUM = 'blknum'
PF_CURRVALS = 'currentvals'
