#!/usr/bin/ksh
#--------------------------------------------------------------------------------#
#  Name: cip_audit_inst_upd.sh                                                 #
#  Purpose: The Scripts is used to create the dynamic vsql scripts that can be   #
#       used to Insert a new reocrd or update the existing record                #
#        with the  help of info provided in the parameters.                      #
#  History                                                                       #
#  Version    Author      Date          Comments                                 #
#     1       Kalyan     09/07/2019     Initial creation                         #
#     2       Praveen    09/13/2019     Modified to insert or update Audit table #
#--------------------------------------------------------------------------------#

# The parameter to provide the environment variables profile
#. $1

# -- Parameter assignment into local variables -- #
Scriptname=`basename $0 | cut -d"." -f1`
Profile=`echo $1 | sed 's/^.*\///g' | cut -d"." -f1`
inst_upd_ind=$2
table=etl_batch_audit
batch_nm=$3
scheduler=$4
Timestamp=`date +'%Y%m%d_%H%M%S'`
TBL_SERVER=$DBSERVER
#Log_file=$LOGDIR/"${Scriptname}_${Profile}_${table}_${Timestamp}.log"
vsql_log_file=$LOGDIR/"vsql_${Scriptname}_${Profile}_${table}_${Timestamp}.log"

# -- Function to log the messages into the log file -- #
function logger
{
        logmsg=$1
        logtme=`date +'%m-%d-%Y(MM-DD-YYYY) %H:%M:%S'`
        echo $logmsg $logtme
        #echo $logmsg $logtme >> $Log_file
}

# -- Function to run the vertica sqls using the vsql -- #
function vsql_executor
{
        if [ $# -gt 1 ]; then
                logger "Too many parameters. Please pass only the query to run as single parameter."
                exit 1
        fi

        if [ -e $vsql_log_file ]; then
                logger "$vsql_log_file is available. Hence, recreating the empty file."
                rm -f $vsql_log_file
                touch $vsql_log_file
        else
                logger "$vsql_log_file not available. Hence, creating the empty file."
                touch $vsql_log_file
        fi

        vsql -h $TBL_SERVER -U $VSQL_USER -w $VSQL_PWD -d $VSQL_DB -p $VSQL_PORT -o $vsql_log_file -a -t -c "$1" 2>>$vsql_log_file

        grep '^ERROR [0-9][0-9]*:' $vsql_log_file > /dev/null
        if [ $? -eq 0 ]; then
                logger "ERROR: Error in the vsql process. Please find below the vsql logs from $vsql_log_file."
                cat $vsql_log_file
                cat $vsql_log_file >> $Log_file
                exit 1
        elif [ $? -gt 1 ]; then
                logger "ERROR: Error in the grep command. Please check the syntax on the grep and also check the $vsql_log_file file."
                exit 1
        else
                logger "VSQL executed successfully."
        fi
}
if [[ $# -ne 3 && $# -ne 4 ]]; then
        logger "ERROR: Number of parameters are not as expected. Minimum 3 parameters are needed and they are as below:"
        logger "Param1 - Profile file with path"
        logger "Param2 - Load status indicator as I if Insert or S if Update"
        logger "Param3 - batch name"
        logger "Param4 - batch scheduler"
        exit 1
fi

logger "Started execution of the script"

logger "To check the column list from the v_catalog.columns for the work table."
vsql_executor "Select Trim(column_name) from v_catalog.columns Where Lower(Trim(table_schema)) = Lower(Trim('$Audit_DB')) and Lower(Trim(table_name)) = Lower(Trim('$table'))and is_identity = False order by ordinal_position;"


Insert_col_list=`cat $vsql_log_file | perl -pe 's/\n/,/g'`
Insert_col_list=`echo $Insert_col_list | sed 's/\,[,]*$//g'`

if [ $# -eq 4 ]; then
	logger "Creating the Insert statement to insert the record into $Audit_DB.$table"
	logger "Setting the columns for the insert query."
	Insert_Query="Insert into $Audit_DB.$table ($Insert_col_list) VALUES(current_timestamp, NULL, '$scheduler', '$batch_nm', '$inst_upd_ind', current_timestamp);"
	logger "Below is the insert query that is going to be triggered:"
	logger "#####################################################################################################################################"
	logger "$Insert_Query"
	logger "#####################################################################################################################################"
	vsql_executor "SET SESSION AUTOCOMMIT TO on; $Insert_Query"
	vsql_executor "SET SESSION AUTOCOMMIT TO on; Select Analyze_statistics('$Audit_DB.$table');"
	logger "Have loaded about `cat $vsql_log_file | head -1` records of data"
elif [ $# -eq 3 ]; then
	logger "Creating the Update statement to update the record in $Audit_DB.$table"
    	Update_Query="update $Audit_DB.$table set batch_end_dtm=current_timestamp, batch_load_stus_ind='$inst_upd_ind',row_process_dtm=current_timestamp WHERE audit_id IN (SELECT MAX(audit_id) FROM $Audit_DB.$table WHERE batch_end_dtm IS NULL AND batch_name='$batch_nm' );"
    	logger "Run the update query to update the existing record"
    	logger "#####################################################################################################################################"
    	logger "${Update_Query}"
    	logger "#####################################################################################################################################"
    	vsql_executor "SET SESSION AUTOCOMMIT TO on; ${Update_Query}"
	logger "Have loaded about `cat $vsql_log_file | head -1` records of data"
else 
	logger " Incorrect load status has been passed, exiting the script."
	exit 1
fi

logger "Removal of all the intermediate files created and used is in progress."
find $LOGDIR -name "*${Scriptname}_${Profile}_${table}_${Timestamp}*" -exec rm -f {} \;
if [ $? -ne 0 ]; then
        logger "ERROR: Error in deleting the intermediate files that were created for the script execution."
        exit 1
fi

exit 0

