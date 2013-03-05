# when changing values, check if change also needed in $PROCESSINGFW_DIR/etc/pfwconfig.des
#
# SW_  submit wcl
# IW_  (wrapper) input wcl
# OW_  (wrapper) output wcl
# PF_  processing fw 
# DB_  database table/column names
######################################################################

FILETYPE  = 'filetype'
FILENAME  = 'filename'
METATABLE = 'metadata_table'
USED  = 'used'
WGB   = 'was_generated_by'
WDF   = 'was_derived_from'

REQNUM = 'reqnum'
ATTNUM = 'attnum'
UNITNAME = 'unitname'
ATTRIB_PREFIX='des_'

COPY_CACHE = 'copycache'
USE_CACHE = 'usecache'
DATA_DEF = 'data_def'
DIRPAT = 'dirpat'
DIRPATSECT = 'directory_pattern'

SW_DIVIDE_JOBS_BY = 'divide_jobs_by'
SW_INPUTS = USED
SW_OUTPUTS = WGB
SW_ANCESTRY = 'ancestry'
SW_EXEC_DEF = 'exec_def'
SW_FILEPAT = 'filepat'
SW_BLOCKLIST = 'blocklist'
SW_MODULELIST = 'modulelist'
SW_BLOCKSECT = 'block'
SW_MODULESECT = 'module'
SW_LISTSECT = 'list'
SW_FILESECT = 'file'
SW_FILEPATSECT = 'filename_pattern'
SW_QUERYFIELDS = 'query_fields'
SW_EXECPREFIX = 'exec_'
SW_STAGEFILES = 'stagefiles'
SW_WRAPSECT = 'wrapper'
SW_WRAPPER_DEBUG = 'wrapper_debug'


IW_INPUTS = USED
IW_OUTPUTS = WGB
IW_ANCESTRY = WDF
IW_EXEC_DEF = 'exec_def'
IW_DATA_DEF = 'data_def'
IW_LISTSECT = 'list'
IW_FILESECT = 'filespecs'
IW_EXECPREFIX = 'exec_'
IW_WRAPSECT = 'wrapper'

OW_INPUTS = USED
OW_OUTPUTS = WGB
OW_ANCESTRY = WDF
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
PF_EXIT_SUCCESS = 0
PF_EXIT_NEXTBLOCK = 100
PF_EXIT_FAILURE = 1
PF_EXIT_OPDELETE = 5
PF_EXIT_DRYRUN = 2
PF_EXIT_WARNINGS = 3
PF_BLKNUM = 'blknum'
PF_CURRVALS = 'currentvals'
