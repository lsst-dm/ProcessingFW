# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

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


HOME_ARCHIVE = 'home_archive'
USE_HOME_ARCHIVE_INPUT = 'use_home_archive_input'
USE_HOME_ARCHIVE_OUTPUT = 'use_home_archive_output'
VALID_HOME_ARCHIVE_INPUT = ['target_archive', 'job', 'all', 'never']
VALID_HOME_ARCHIVE_OUTPUT = ['wrapper', 'job', 'block', 'run', 'never']
CREATE_JUNK_TARBALL = 'create_junk_tarball'


TARGET_ARCHIVE = 'target_archive'
USE_TARGET_ARCHIVE_INPUT =  'use_target_archive_input'
USE_TARGET_ARCHIVE_OUTPUT = 'use_target_archive_output'


STAGE_FILES = 'stagefiles'
SAVE_FILE_ARCHIVE = 'savefiles'



DIRPAT = 'dirpat'
DIRPATSECT = 'directory_pattern'


SW_LABEL = 'label'
SW_JOB_ENVIRONMENT = 'job_environment'
SW_DIVIDE_JOBS_BY = 'divide_jobs_by'
SW_INPUTS = USED
SW_OUTPUTS = WGB
SW_ANCESTRY = 'ancestry'
SW_EXEC_DEF = 'exec_def'
SW_FILEPAT = 'filepat'
SW_FILEPATSECT = 'filename_pattern'
SW_BLOCKLIST = 'blocklist'
SW_MODULELIST = 'modulelist'
SW_BLOCKSECT = 'block'
SW_MODULESECT = 'module'
SW_LISTSECT = 'list'
SW_FILESECT = 'file'
SW_QUERYFIELDS = 'query_fields'
SW_EXECPREFIX = 'exec_'
SW_WRAPSECT = 'wrapper'
SW_WRAPPER_DEBUG = 'wrapper_debug'
SW_OUTPUT_OPTIONAL = 'optional'

SW_JOB_BASE_DIR = 'jobroot'   # must match column name in ops_site


IW_INPUTS = USED
IW_OUTPUTS = WGB
IW_ANCESTRY = WDF
IW_EXEC_DEF = 'exec_def'
IW_LISTSECT = 'list'
IW_FILESECT = 'filespecs'
IW_EXECPREFIX = 'exec_'
IW_WRAPSECT = 'wrapper'
IW_OUTPUT_OPTIONAL = 'optional'

#IW_META_HEADERS = 'headers'
#IW_META_COMPUTE = 'compute'
#IW_META_WCL = 'wcl'
#IW_UPDATE_HEAD_PREFIX = 'hdrupd_'
#IW_UPDATE_WHICH_HEAD = 'headers'
#IW_REQ_META = 'req_metadata'
#IW_OPT_META = 'opt_metadata'

# lower case because appears as wcl section and wcl sections are converted to lowercase
#META_HEADERS = 'h'
#META_COMPUTE = 'c'
#META_WCL = 'w'
#META_REQUIRED = 'r'
#META_OPTIONAL = 'o'

OW_INPUTS = USED
OW_OUTPUTS = WGB
OW_ANCESTRY = WDF
OW_EXECPREFIX = IW_EXECPREFIX
OW_PROVSECT = 'provenance'
OW_METASECT = 'file_metadata'

PF_RUN_PAT = '%(unitname)s_r%(reqnum)dp%(attnum)02d'
OPS_RUN_DIR = 'ops_run_dir'
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
