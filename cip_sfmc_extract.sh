#!/usr/bin/ksh
#------------------------------------------------------------------------------#
#  Name: cip_src_to_wrk_load.sh                                                #
#  Author: Kalyan                                                              #
#  Created on: 09/08/2019                                                      #
#  Purpose: The Scripts is used to execute the vsql scripts that can are found #
#	in the etl_transformation_master metadata table. This will query and run   #
#	through the list of sql statements that are defined for a source code and  #
#	work table provided through the arguments to this script.                  #
#------------------------------------------------------------------------------#

# The parameter to provide the environment variables profile
#. $1

# -- Parameter assignment into local variables -- #
Scriptname=`basename $0 | cut -d"." -f1`
Profile=`echo $1 | sed 's/^.*\///g' | cut -d"." -f1`
Source_cd=$2
Wrk_Table=$3
Trans_seq=$4
Today=`date +'%Y-%m-%d'`
Timestamp=`date +'%Y%m%d_%H%M%S'`
TBL_SERVER=$DBSERVER
Log_file=$LOGDIR/"${Scriptname}_${Profile}_${Source_cd}_${Wrk_Table}_${Timestamp}.log"
vsql_log_file=$LOGDIR/"vsql_${Scriptname}_${Profile}_${Source_cd}_${Wrk_Table}_${Timestamp}.log"

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
	logger "Param2 - Source code from the transformation master"
	logger "Param3 - Work table name"
	logger "Param4 (Optional) - Transformation sequence number (Optional parameter to run if in case only one transformation has to be triggered. Mostly used for the debugging purposes.)"
	exit 1
fi

logger "Started execution of the script"
if [ $# -eq 3 ]; then
	logger "Instantiating the process for execution of the sqls defined for the $Wrk_Table"
	logger "Executing the query to get the count of transformations available to be executed."
	vsql_executor "Select count(*) from edw_audit.etl_transformation_master where lower(trim(source_cd)) = lower(trim('${Source_cd}')) and lower(trim(work_table)) = lower(trim('${Wrk_Table}'));"
	
	Trans_cnt=`cat $vsql_log_file | head -1`
	counter=1
	while [ $counter -le ${Trans_cnt} ]
	do
		logger ""
		logger "#####################################################################################################################################"
		logger ""
		
		logger "Iterating the loop for the $counter time. There are about `expr ${Trans_cnt} - ${counter}` approx. iterations left."
		vsql_executor "With FINAL as (Select to_char(transformation_seq) as TS,coalesce(regexp_replace(trim(transformation_desc),'(?>\r\n|\n|\r|\t)',' '),'') as TD,regexp_replace(trim(sql_statement),'(?>\r\n|\n|\r|\t)',' ') as SS from (Select row_number() over (order by transformation_seq) as rw_nm, transformation_seq, transformation_desc, sql_statement from edw_audit.etl_transformation_master where lower(trim(source_cd)) = lower(trim('${Source_cd}')) and lower(trim(work_table)) = lower(trim('${Wrk_Table}')) and '${Today}' between valid_from and valid_to) DRVD where rw_nm = ${counter}) Select TS::varchar(65000) from FINAL UNION ALL Select TD::varchar(65000) from FINAL UNION ALL Select SS::varchar(65000) from FINAL;"
		
		Trans_seq=`cat $vsql_log_file | head -1`
		Trans_desc=`cat $vsql_log_file | head -2 | tail -1`
		Sql_stmt=`cat $vsql_log_file | head -3 | tail -1`
		
		logger "Retrieved the sql statement for ${Trans_seq} sequence number."
		logger "Description - \'${Trans_desc}\'"
		logger ""
		logger "====================================================================================================================================="
		vsql_executor "SET SESSION AUTOCOMMIT TO on; ${Sql_stmt}"
		logger "====================================================================================================================================="
		
		logger ""
		logger "#####################################################################################################################################"
		logger ""
		counter=`expr $counter + 1`
	done
elif [ $# -eq 4 ]; then
	logger "Received the transformation sequence number. Hence executing the script to run for only $Trans_seq for the $Wrk_Table."
	logger ""
	logger "#####################################################################################################################################"
	logger ""
	
	vsql_executor "With FINAL as (Select to_char(transformation_seq) as TS,coalesce(regexp_replace(trim(transformation_desc),'(?>\r\n|\n|\r|\t)',' '),'') as TD,regexp_replace(trim(sql_statement),'(?>\r\n|\n|\r|\t)',' ') as SS from (Select transformation_seq, transformation_desc, sql_statement from edw_audit.etl_transformation_master where lower(trim(source_cd)) = lower(trim('${Source_cd}')) and lower(trim(work_table)) = lower(trim('${Wrk_Table}')) and '${Today}' between valid_from and valid_to) DRVD where transformation_seq = ${Trans_seq}) Select TS::varchar(65000) from FINAL UNION ALL Select TD::varchar(65000) from FINAL UNION ALL Select SS::varchar(65000) from FINAL;"

	Sql_stmt=`cat $vsql_log_file | head -3 | tail -1 | sed 's/^[ ]*$/Empty/g'`
	
	if [ "${Sql_stmt}" == "Empty" ]; then
		logger ""
		logger "ERROR: Please check if the transformation_seq in the table has the mentioned passed number (${Trans_seq}) and also has current date between the valid dates."
		logger ""
		exit 1
	else	
		Trans_seq=`cat $vsql_log_file | head -1`
		Trans_desc=`cat $vsql_log_file | head -2 | tail -1`

		logger "Retrieved the sql statement for ${Trans_seq} sequence number."
		logger "Description - \'${Trans_desc}\'"
		logger ""
		logger "====================================================================================================================================="
		vsql_executor "SET SESSION AUTOCOMMIT TO on; ${Sql_stmt}"
		logger "====================================================================================================================================="
	fi
	
	logger ""
	logger "#####################################################################################################################################"
	logger ""
fi


logger "Analyzing the stats on table ${Wrk_Table}"
vsql_executor "SET SESSION AUTOCOMMIT TO on; Select Analyze_statistics('${sfmc_extract}.${Wrk_Table}');"


#logger "Removal of all the intermediate files created and used is in progress."
#find $LOGDIR -name "*${Scriptname}_${Profile}_${Source_cd}_${Wrk_Table}_${Timestamp}*" -exec rm -f {} \;
#if [ $? -ne 0 ]; then
#	logger "ERROR: Error in deleting the intermediate files that were created for the script execution."
#	exit 1
#fi

exit 0
