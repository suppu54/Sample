#!/bin/ksh
##########################################################################################################
# Created By: Kalyan		                                                                             #
# Created Date: 09/07/2019                                                                         #
# Version: 1.0                                                                                           #
# Description: This script creates encrypted password files.											 #
##########################################################################################################

. $1

echo $PROJECTDIR
cd $PROJECTDIR
echo "Please enter your hostname/domain: \c"
read host
host_name=`echo $host | tr [:upper:] [:lower:]`
echo "Please enter your username: \c"
read -r user
user1=`echo $user | sed 's/\\\\/\\\\\\\\\\\\\\\\/g'`
echo "Please enter your password: \c" 
read -r pass
echo $pass | /usr/local/bin/pcrypt -e cipper > .${host_name}_$(echo $user | sed 's/^[^\\]*\\//g').pwd.crypt
