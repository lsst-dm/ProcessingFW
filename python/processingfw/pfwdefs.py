# when changing values, check if change also needed in $PROCESSINGFW_DIR/etc/pfwconfig.des

FILETYPE  = 'filetype'
FILENAME  = 'filename'
METATABLE = 'metadata_table'
USED  = 'used'
WGB   = 'was_generated_by'
WDF   = 'was_derived_from'

SW_INPUTS = USED
SW_OUTPUTS = WGB
SW_ANCESTRY = 'ancestry'

IW_INPUTS = USED
IW_OUTPUTS = WGB
IW_ANCESTRY = WDF

OW_INPUTS = USED
OW_OUTPUTS = WGB
OW_ANCESTRY = WDF

# SW_  submit wcl
# IW_  (wrapper) input wcl
# OW_  (wrapper) output wcl
# PF_  processing fw 
# DB_  database table/column names

REQNUM = 'reqnum'
ATTNUM = 'attnum'
UNITNAME = 'unitname'

SW_EXEC_DEF = 'exec_def'
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
SW_EXECPREFIX = 'exec_'
SW_STAGEFILES = 'stagefiles'
SW_WRAPSECT = 'wrapper'

IW_EXEC_DEF = 'exec_def'
IW_LISTSECT = 'list'
IW_FILESECT = 'filespecs'
IW_EXECPREFIX = 'exec_'
IW_WRAPSECT = 'wrapper'

OW_EXECPREFIX = IW_EXECPREFIX
OW_PROVSECT = 'provenance'
OW_METASECT = 'file_metadata'

PF_TASKNUM = 'tasknum'
PF_JOBNUM = 'jobnum'
PF_WRAPNUM = 'wrapnum'
PF_LISTENTRY = 'line'
PF_USE_DB_IN = 'use_db_in'
PF_USE_DB_OUT = 'use_db_out'
PF_USE_QCF = 'use_qcf'
PF_DRYRUN = 'dry_run'


ATTRIB_PREFIX='des_'
PF_EXIT_SUCCESS = 0
PF_EXIT_NEXTBLOCK = 100
PF_EXIT_FAILURE = 1
PF_EXIT_OPDELETE = 5
PF_EXIT_DRYRUN = 2
PF_EXIT_WARNINGS = 3

PF_BLKNUM = 'blknum'
PF_CURRVALS = 'currentvals'
