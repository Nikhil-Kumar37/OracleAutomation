"""

        .SYNOPSIS
          Fetch Instances - ORACLE

        .DESCRIPTION
          The script will fetch the instances available in the oracle database.

        .INPUTS
          Inputs are given by CMD Line argument - username, server, password

        .OUTPUT
          A Outupt will be sent to the workflow with Json format.
          output = {"retCode" : "1", "result" : "abc,def", "retDesc" : "success"}


        .EXAMPLE
            >Python -W ignore "E:\DATABASE_ORACLE_FETCH_INSTANCES.py" --oracleServerName <IP Address/Hostname> --username <Username> --password <Password> 


        .NOTES

          Script Name    : DATABASE_ORACLE_FETCH_INSTANCES
          Script Version : 1.0
          Author         : Nikhil Kumar
          Creation Date  : 18-05-2020

"""

##### IMPORTING REQUIRED MODULES #####

import paramiko
import argparse
import re
from ITOPSA_STANDALONE_LIB_PY import *

##### Log Path for the module #####

logPath=os.path.realpath(__file__)
path(logPath)

##### GETTING COMMANDLINE PARAMETERS #####

parser=argparse.ArgumentParser()
parser.add_argument('--oracleServerName')
parser.add_argument('--username')
parser.add_argument('--password')



args=parser.parse_args()

##### DEFINING VARIABLES #####
oracleServerName = args.oracleServerName
username = args.username
password = args.password


write_log(4, "Obtained required inputs")

#####CHECK MANDATORY VARIABLES#####

check_mandatory_vars([oracleServerName, username, password])

write_log(4, "Mandatory variable check performed")
          
##### CONNECTING TO REMOTE SERVER #####

try:
        write_log(1, "Taking Remote connetion to server")
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(oracleServerName, username=username, password=password)
        write_log(1, "Connection Successful")

except Exception as err:
        output = {"retCode" : "1", "result" : "Connecting to remote server failed %s"%err.message, "retDesc" : "Failure"}
        write_log(3, err.message)
        exit_script(3,"Script command execution failed", output)





##### SEND USER CREATION COMMAND TO SERVER #####



try:
        
	write_log(1, "Executing Instance fetch command")
	fetchInstanceListCmd = "cat /etc/oratab | grep -v '#' | cut -d ':' -f1 | grep -v -e '^$'"
	
	stdin, stdout, stderr = ssh.exec_command(fetchInstanceListCmd)
	fetchInstanceListCmdOutput = stdout.readlines()
	
	

	if fetchInstanceListCmdOutput is None or fetchInstanceListCmdOutput=="[]":
		output = {"retCode" : "1", "result" : "Unable to fetch Instances for Oracle database '%s'"%oracleServerName, "retDesc" : "No Instance are avialable"}
		write_log(3, "No Instance are avialable")
		exit_script(3,"No Instance are avialable", output)
	
	else:
                
                #Remove "u" from list
                fetchInstanceListCmdOutput = ','.join(map(str,fetchInstanceListCmdOutput))
                #Replace "\n" with empty value
                fetchInstanceListCmdOutput = fetchInstanceListCmdOutput.replace("\n","")
		#print(fetchInstanceListCmdOutput)
		
		write_log(1,"Instance fetched successfully")
		output = {"retCode" : "0", "result" : "%s"%(fetchInstanceListCmdOutput), "retDesc" : "Success"}
		exit_script(1,"Instance fetched successfully", output)
                          
		
			
except Exception as err:
        output = {"retCode" : "1", "result" : "Error - %s"%(err.message), "retDesc" : "Failure"}
        write_log(3, "Script execution failed: %s"%(err.message))
        exit_script(3,"Script execution failed", output)
finally:
        write_log(5,"Closing the SSH Connection")
	if(ssh):
            ssh.close()
	write_log(5,"SSH session closed")
