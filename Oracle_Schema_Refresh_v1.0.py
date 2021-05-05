"""        
        .SYNOPSIS
        Oracle Schema Refresh

        .DESCRIPTION
        Exporting a schema in Oracle DB and importing it in another DB with prechecks and postchecks included.

        Task performed at source server side :
        
        1. Login to server and switching user to 'oracle' (if required)
        2. Verification of SID and checking its running or not
        3. Deleting dump files older than 30 days
        4. Checking file system size for the mount point
        5. Checking size of ORADATA file system
        6. Altering DB (Incase of 12c version)
        7. Schema size check
        8. Default tablespace check
        9. Distinct tablespace name check
        10. Checking object type and count
        11. Checking Owner,Directory name and Directory path for Source SID
        12. Exporting schema
        13. Copying file to target server

        Task performed at target server side :

        1. Login to server and switching user to 'oracle' (if required)
        2. Verification of SID and checking its running or not
        3. Deleting dump files older than 30 days
        4. Checking file system size for the mount point
        5. Checking size of ORADATA file system
        6. Altering DB (Incase of 12c version)
        7. Schema cleanup
        8. Log mode change to no archive log (Incase of schema size is equal or gretaer than 10gb)
        9. Schema size check
        10. Default tablespace check
        11. Distinct tablespace name check
        12. Checking object type and count
        13. Checking owner,directory name and directory path for target SID
        14. Importing schema
        15. Log mode change to archive log (Incase of schema size is equal or gretaer than 10gb)
        16. Post import schema size check
        17. Post import default tablespace check
        18. Post import distinct tablespace name check
        19. Post import checking object type and count
        20. Recompiling all invalid objects (utlrp.sql cmd)

        Other tasks :

        1. Log file creation and update
        2. Send mail

        .INPUTS
        Inputs will be taken from the command line.

        Required inputs :

        1. Source_HostName
        2. Source_SID
        3. Source_SchemaName
        4. Source_UserName
        5. Source_Password
        6. Target_HostName
        7. Target_SID
        8. Target_SchemaName
        9. Target_UserName
        10. Target_Password
        11. Target_System_Password
        12. TicketNumber
        13. source_parfile_path(Incase of parfile)
        14. target_parfile_path(Incase of parfile)
 
        .OUTPUT
        Output will be sent on mail with output file on successful execution and execution steps will be stored in log file of the same folder where script will be present.

        .EXAMPLE

        PS D:\> python Oracle_Schema_Refresh.py


        Script Name    : Oracle_Schema_Refresh.py
        Script Version : 1.1
        Author         : Nikhil Kumar
        Creation Date  : 30th Oct 2020

"""

#!/usr/bin/env python
from sys import path
import sys, os, re
import time
import datetime
from os import system, getcwd, path, makedirs
import logging as log
from getpass import getpass
from netmiko import ConnectHandler
import paramiko
import argparse
import json, random, string
import smtplib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
from  datetime import datetime
#### Scripts Starts Here ####

print("******************************************************************************************************************************************")
print("Script execution started.")

try:
        
        #### Current Date Details Fetch ####
        
        now = datetime.now()
        dt = now.strftime("%d%m%y")
        dt=str(dt)
        
        #### Input Collection From User ####

        try:
                 
                TicketNumber = input("Enter Ticket Number \t\t\t: ")
                print("\n")
                print("Please enter SOURCE DATABASE details.")
                
                Source_HostName = input("Enter SOURCE DB SERVERNAME with FQDN \t\t\t: ")
                Source_SID = input("Enter Source {0} Server SID \t\t\t: ".format(Source_HostName))
                Source_SID_1= Source_SID
                Source_SchemaName= input("Enter Source {0} SCHEMANAME to Export \t\t\t:  ".format(Source_HostName))
                Source_SchemaName= Source_SchemaName.upper()
                print("\n")
                Source_UserName = input("Enter Source {0}  'UserName' \t\t\t: ".format(Source_HostName))
                Source_Password = getpass("Enter Source {0}  'UserName' PASSWORD \t\t\t: ".format(Source_HostName))

                ## If switching user to oracle requires password, then please uncomment below lines ##
                print("\n")
                if (Source_UserName.lower() !=  "oracle"):
               
                        oracle_Password = getpass("Enter Source Server {0} 'ORACLE' User Password \t\t\t: ".format(Source_HostName))
                
                if (Source_UserName.lower() ==  "oracle"):

                        oracle_Password = Source_Password
          
                Source_System_Password = getpass("Enter Source Server {0} 'SYSTEM' User Password \t\t\t: ".format(Source_HostName))
                print("\n")                        
                print("Please enter TARGET DATABASE details.")
                Target_HostName = input("Enter TARGET DB SERVERNAME with FQDN \t\t\t: ")
                Target_server = Target_HostName.split(".")
                Target_server = Target_server[0]
                Target_server = Target_server.lower()
                if (Target_server[-1] == "p"):
                    print("Provided input servername PRODUCTION hence terminatting the script")
                    exit()              
                Target_SID = input("Enter Target {0} Server SID \t\t\t: ".format(Target_HostName))
                Target_SID_1= Target_SID                
                Target_SchemaName= input("Enter Target server {0} Schemaname to Import \t\t\t: ".format(Target_HostName))
                Target_SchemaName= Target_SchemaName.upper()  
                print("\n")
                ##Target_UserName = input("Enter Target SERVER {0} UserName: ".format(Target_HostName))
                ##Target_Password = getpass("Enter Target Server {0}  UserName Password: ".format(Target_HostName))

                Target_UserName = Source_UserName
                Target_Password = Source_Password
                ## If switching user to oracle requires password, then please uncomment below lines ##
                
                #if (Target_UserName.lower() !=  "oracle"):
                                        
                        #T_Oracle_Pass = getpass("Enter Target Server 'oracle' Password: ")
              
                Target_System_Password = getpass("Enter Target Server {0} 'SYSTEM' User Password \t\t\t: ".format(Target_HostName))

                ## Parfile check ##
                
                parfile_check = input("Enter 'yes' if parfile is there, else 'No' \t\t\t: ")
        
                if (parfile_check.lower() ==  "yes"):
                        
                        source_parfile_path = input("Enter Source Parfile Path \t\t\t: ")
                        target_parfile_path = input("Enter Target Parfile Path \t\t\t: ")

                elif (parfile_check.lower() ==  "no"):

                        print("Thank you for your inputs.")

                else:

                        print("Please enter the parameters properly")           
                        exit()
                
                print("Please verify inputs :")
                
                Source_input = "Source Hostname : {0}, Source SID : {1}, Source Schema Name : {2}, Source Username : {3}".format(Source_HostName,Source_SID,Source_SchemaName,Source_UserName)
                Target_input = "Target Hostname : {0}, Target SID : {1}, Target Schema Name : {2}, Target Username : {3}".format(Target_HostName,Target_SID,Target_SchemaName,Target_UserName)
                
                print(Source_input)
                print(Target_input)

                flag_inputs= input("Enter 'yes' if inputs are correct else 'No' \t\t\t: ")
        
                if (flag_inputs.lower() ==  "yes"):

                        print("Thank you for confirming.")

                elif (flag_creds.lower() ==  "no"):

                        print("Terminating the script. Please retrigger the script.")
                        exit()

                else:
                
                        print("Please enter the parameters properly")           
                        exit()             

        except Exception as err:
                        
                log_output = "Failed to collect input. Error - {1}".format(err)
                print(log_output)
                exit()

        #### Log File Creation ####
        
        try:
                
                logfile = "Oracle_Schema_Refresh_%s_%s.txt" %(TicketNumber,dt)

                ## Checking OS Type ##
                
                if os.name == 'nt':
                        
                        logdir = os.getcwd()+"\\logs"
                        #logdir = "/tmp"
                        filename = "%s\\%s" %(logdir,logfile)
                        
                if os.name == 'posix':
                        
                        logdir = os.getcwd()+"/logs"
                        #logdir = "/tmp"
                        filename = "%s/%s" %(logdir,logfile)
                        
                if not path.exists(logdir):
                        
                        os.makedirs(logdir)

                log.basicConfig(filename=filename, format='%(lineno)s %(asctime)s %(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p',filemode = 'w', level=log.INFO)
                
                log.info('******************************************************************************************************************************************')

                log.info('Script execution started.')
                
                logfile_output = "Log file created. Log file path - {}".format(filename)
                print(logfile_output)
                
        except Exception as err:
                
                print("Failed to create log file {0}. Error - {1}".format(filename,err))

        #### End Of Log File Creation ####
        
        REPOPATH = "/usr/lic/oracle/dba/automation/Schema_Refresh_Script/REPOLOG/scriptrunlog.txt"
        DT = datetime.now()
        SCRIPTPATH  = os.path.realpath(__file__)
        
        #### Recording the details in repologfile ###
        f = open(REPOPATH,"a+")
        f.write("%s  %s  %s  Export done by %s from %s@%s for schema %s to target %s@%s for schema %s\r\n"%(DT,SCRIPTPATH,filename,Source_UserName,Source_SID,Source_HostName,Source_SchemaName,Target_SID,Target_HostName,Target_SchemaName))
        f.close()
        #### Log File Update ####

        log.info('Input collected')
        log.info('Source Hostname : {0}, Source SID : {1}, Source Schema Name : {2}, Source Username : {3}'.format(Source_HostName,Source_SID,Source_SchemaName,Source_UserName))
        log.info('Target Hostname : {0}, Target SID : {1}, Target Schema Name : {2}, Target Username : {3}'.format(Target_HostName,Target_SID,Target_SchemaName,Target_UserName))

        ##### Function - Script Exit and Failure Mail #####

        def exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber):

                try:
                        ## Failure mail content ##
        
                        print("Preparing for sending mail...")
                        log.info('Preparing for sending mail...')

                        Sourceserver = Source_HostName
                        Targetserver = Target_HostName

                        SourceDB = Source_SID
                        TargetDB = Target_SID

                        SourceSchema = Source_SchemaName
                        TargetSchema = Target_SchemaName

                         ##MailIds##

                        Mail_id = 'abc@gmail.com'

                        Body_message ="""Schema refresh failed

                        Source server name : %s
                        Target server name : %s

                        Source database name : %s
                        Target database name : %s

                        Source Schema names : %s
                        Target schema names : %s

                        """ %(Sourceserver,Targetserver,SourceDB,TargetDB,SourceSchema,TargetSchema)

                        cmd = 'echo "%s" | mailx -s "Schema_Refresh|%s" -a %s %s' %(Body_message,TicketNumber,filename,Mail_id)

                        output = os.system(cmd)

                        print('Mail sent')
                        log.info('Mail sent')
        
                        print_output = "You can check script output in {} file.".format(filename)
                        print(print_output)
                        print("******************************************************************************************************************************************")
                        exit()

                except Exception as err:
                        
                        log_output = "Failed to send failure mail. Error - {}".format(err)
                        print(log_output)
                        log.info('Failed to send failure mail. Error - {}'.format(err))
                        
                        print_output = "You can check script output in {} file.".format(filename)
                        print(print_output)
                        print("******************************************************************************************************************************************")
                        exit()

        ##### End of Function - Script Exit and Failure Mail #####

        ##### Function - Success Mail #####

        def mail_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber):

                try:
                       
                       ## Success mail content ##
                        
                        print("Preparing for sending mail...")
                        log.info('Preparing for sending mail...')

                        Sourceserver = Source_HostName 
                        Targetserver = Target_HostName

                        SourceDB = Source_SID
                        TargetDB = Target_SID

                        SourceSchema = Source_SchemaName
                        TargetSchema = Target_SchemaName

                        ##MailIds##

                        Mail_id = 'abc@gmail.com'

                        Body_message ="""Schema refresh success 

                        Source server name : %s
                        Target server name : %s

                        Source database name : %s
                        Target database name : %s

                        Source Schema names : %s
                        Target schema names : %s

                        """ %(Sourceserver,Targetserver,SourceDB,TargetDB,SourceSchema,TargetSchema)

                        cmd = 'echo "%s" | mailx -s "Schema_Refresh|%s" -a %s %s' %(Body_message,TicketNumber,filename,Mail_id)

                        output = os.system(cmd)

                        print('Mail sent')
                        log.info('Mail sent')

                        log.info('Script completed successfully.')
                        print("Script completed successfully.")

                        print("------------100% Execution Completed------------")
                        log.info('------------100% Execution Completed------------')
        
                        print_output = "You can check script output in {} file.".format(filename)
                        print(print_output)
                        print("******************************************************************************************************************************************")

                except Exception as err:
                        
                        log_output = "Failed to send success mail. Error - {}".format(err)
                        print(log_output)
                        log.info('Failed to send success mail. Error - {}'.format(err))
                        
                        print_output = "You can check script output in {} file.".format(filename)
                        print(print_output)
                        print("******************************************************************************************************************************************")
                        exit()

        ##### End of Function - Success Mail #####
        
        ##### Function - Connecting To Server #####

        def Server_Connection(HostName,UserName,Password,filename,logfile):

                try:
                        
                        log.info('Connecting to server {0} with {1} user.'.format(HostName,UserName))
                        log_output = "Connecting to server {0} with {1} user.".format(HostName,UserName)
                        print(log_output)

                        Server = {
                                "host": HostName,
                                "username": UserName,
                                "password": Password,
                                "device_type": 'linux'
                                }
                        net_connect = ConnectHandler(**Server)

                        log_output = "Connected to server {}.".format(HostName)
                        print(log_output)
                        log.info('Connected to server {}.'.format(HostName))

                        return net_connect
                
                except Exception as err:
                        
                        log_output = "Failed to connect to {0} server. Error - {1}".format(HostName,err)
                        print(log_output)
                        log.info('Unable to connect to {0} server. Error - {1}'.format(HostName,err))
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)
                                       
        #### End of Function - Connecting To Server #####

        #### Function - Switching User ####

        def switch_user(UserName,filename,logfile):

                try:

                        if (UserName.lower() !=  "oracle"):

                                log.info('Switching to oracle user.')

                                switch_cmd = "sudo su - oracle"

                                log.info('Running CMD - {}'.format(switch_cmd))
                                switch_output = net_connect.send_command_timing(switch_cmd)
                                log.info('CMD Output - {}'.format(switch_output))

                                ## Need to comment below line if password not require to switch user ##
                                #switch_output = net_connect.send_command_timing(Oracle_Pass)
                                
                                log.info('User changed to oracle.')
                                print("User changed to oracle.")

                except Exception as err:
                
                                log_output = "Unable to switch user to oracle. Error - {1}".format(err)
                                print(log_output)
                                log.info('Unable to switch user to oracle. Error - {1}'.format(err))
                                
                                exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)
        ### End of Function - Switching User ####

        #### Function - SID Validation ####

        def SID_Check(SID,filename,logfile):

                try:
                        
                        SID_Test = "ora_pmon_{}".format(SID)

                        log.info('{} SID Validation'.format(SID))
                
                        command = "ps -ef | grep -w {} | grep -v grep".format(SID_Test)
                   
                        log.info('Running CMD - {}'.format(command))
                        
                        command_output = net_connect.send_command_timing(command)
  
                        log.info('SID check Output - {}'.format(command_output))

                        print(command_output)

                        if SID_Test in command_output:
                        
                                status = "{} SID is running.".format(SID)
                                log.info('{} SID is running.'.format(SID))
                                print(status)
                        
                        else:
                        
                                status = "{} SID is not running.".format(SID)
                                log.info('{} SID is not running.'.format(SID))
                                print(status)
                                
                                exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)
                                
                except Exception as err:
                        
                        log_output = "Failed to validate {0} SID. Error - {1}".format(SID,err)
                        print(log_output)
                        log.info('Failed to validate {0} SID. Error - {1}'.format(SID,err))
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        #### End of Function - SID Validation ####

        #### Function - File System Size Precheck ####

        def FileSystem_Check(SID,filename,logfile):
                
                try:

                        Mount_Path = "/usr/lic/oracle/dba/datapump/{}".format(SID) ## Need to confirm this path ##

                        log.info('Checking file system size for the mount point {} of {} SID.'.format(Mount_Path,SID))
                        
                        command1 = "df -PhT {} | tail -1 | tr -s ' ' | tr -d '%' ".format(Mount_Path)

                        log.info('Running CMD - {}'.format(command1))
                        
                        command_output1 = net_connect.send_command_timing(command1)
                        log.info('CMD Output - {}'.format(command_output1))
                        output_format = command_output1.split()

                        utilization = int(output_format[5])

                        if (utilization > 80):
                        
                                fs_size_output = "File system for the mount point {0} of {1} SID is more than 80% used. It's {2}% used.".format(Mount_Path,SID,utilization)
                                print(fs_size_output)
                                log.info('File system for the mount point {0} of {1} SID is {2}% used.'.format(Mount_Path,SID,utilization))
                                
                                flag_size= input("Enter 'yes' if you want script to continue else 'No': ")
                                
                                if (flag_size.lower() ==  "yes"):

                                        log.info('User wants to continue the script.')
                                        print("Thank you for the input.")

                                elif (flag_size.lower() ==  "no"):
                                        
                                        print("Terminating the script.")
                                        log.info('User wants to terminate the script.')
                                        
                                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)
                                else:

                                        log.info('Invalid input by user. Terminating the script')
                                        print("Please enter the parameters properly")
                                       
                                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber) 
                        else:
                        
                                log_output = "File system for the mount point {0} of {1} SID is {2}% used.".format(Mount_Path,SID,utilization)
                                print(log_output)
                                log.info('File system for the mount point {0} of {1} SID is {2}% used.'.format(Mount_Path,SID,utilization))
                        
                except Exception as err:
                
                        log_output = "Failed to check file system size for the mount point of {0} SID. Error - {1}".format(SID,err)
                        print(log_output)
                        log.info('Failed to check file system size for the mount point of {0} SID. Error - {1}'.format(SID,err))
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        #### End of Function - File System Size Precheck ####

        #### Function - ORADATA File System Precheck ####

        def Oradata_FileSystemCheck(SID,filename,logfile,version):

                try:
                
                        if (version.lower() == "yes"):
                        
                                File_Path = "/usr/lic/oracle/home/oradata/{}".format(SID)
                                
                        else:   
                        
                                File_Path = "/usr/lic/oracle/home/oradata" ## Need to validate this path ##

                        log.info('Checking size of ORADATA file system {}.'.format(File_Path))
                        
                        command2 = "df -PhT {} | tail -1 | tr -s ' ' | tr -d '%' ".format(File_Path)

                        log.info('Running CMD - {}'.format(command2))
                        
                        command_output2 = net_connect.send_command_timing(command2)
                        
                        log.info('CMD Output - {}'.format(command_output2))
                        
                        output_format1 = command_output2.split()

                        oradata_utilization = int(output_format1[5])

                        if (oradata_utilization > 80):
                                
                                fs_size_output = "{0} ORADATA file system {1} is more than 80% utilized. It's {2}% used.".format(SID,File_Path,oradata_utilization)
                                print(fs_size_output)
                                log.info('{0} ORADATA file system {1} is {2}% used.'.format(SID,File_Path,oradata_utilization))
                                
                                flag_size= input("Enter 'yes' if you want script to continue else 'No': ")
                                
                                if (flag_size.lower() ==  "yes"):

                                        log.info('User wants to continue the script.')
                                        print("Thank you for the input.")

                                elif (flag_size.lower() ==  "no"):
                                        
                                        print("Terminating the script.")
                                        log.info('User wants to terminate the script.')
                                        
                                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)                                        

                                else:
                                        log.info('Invalid input by user. Terminating the script')
                                        print("Please enter the parameters properly")
                                       
                                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber) 

                        else:
                                
                                log_output = "{0} ORADATA file system {1} is less than 80% utilized. It's {2}% used.".format(SID,File_Path,oradata_utilization)
                                print(log_output)
                                log.info('{0} ORADATA file system {1} is {2}% used.'.format(SID,File_Path,oradata_utilization))

                except Exception as err:
                        
                        log_output = "Failed to check oradata file system size. Error - {}".format(err)
                        print(log_output)
                        log.info('Failed to check oradata file system size. Error - {}'.format(err))
                       
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber) 

        #### End of Function - ORADATA File System Precheck ####

        #### Function - Older File Delete ####

        def Older_File_Delete(SID,filename,logfile):

                try:

                        Dump_Path = "/usr/lic/oracle/dba/datapump/{}/".format(SID)

                        log.info('Checking older dump files from {}.'.format(Dump_Path))

                        ## Printing 30 days older dump files ##

                        olderFileList = "find {} -mtime +30 -print".format(Dump_Path)

                        log.info('Running CMD - {}'.format(olderFileList))
                        
                        olderFileOutput = net_connect.send_command_timing(olderFileList)

                        olderFileOutput1 = olderFileOutput.rsplit("\n",1)[0]

                        if ("[oracle@" in olderFileOutput1):

                                print("There is no 30 days older file present.")
                                log.info('There is no 30 days older file present.')

                        elif(olderFileOutput1):     

                                
                                olderFileOutput2 = "Below are the list of 30 days older files : \n{}".format(olderFileOutput1)
                                print(olderFileOutput2)
                                
                                log.info('Below are the list of 30 days older files : \n{}'.format(olderFileOutput1))
                                
                                ## Checking with user to delete old dump files or not ##
                                
                                flag_file = input("Enter 'yes' if you want to delete files else 'No': ")
                                
                                if (flag_file.lower() ==  "yes"):
                                                
                                        print("Deleting older dump files.")
                                        log.info('Deleting older dump files from {}.'.format(Dump_Path))
                                
                                        command3 = "find {} -mtime +30 -delete".format(Dump_Path)
                                        log.info('Running CMD - {}'.format(command3))          
                                        command_output3 = net_connect.send_command_timing(command3)
                                                        
                                        log_output = "Deleted 30 days older dump files from {}.".format(Dump_Path)
                                        print(log_output)
                                        log.info('Deleted 30 days older dump files from {}.'.format(Dump_Path))

                                elif (flag_file.lower() ==  "no"):

                                        log.info('User does not want to delete files. Terminating the script')
                                        print("Terminating the script.")
                                        
                                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

                                else:
                                                
                                        log.info('Invalid input by user. Terminating the script')
                                        print("Please enter the parameters properly")
                                        
                                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

                        else:

                                print("There is no 30 days older file present.")
                                log.info('There is no 30 days older file present.')

                except Exception as err:
                        
                        log_output = "Failed to delete old dump files. Error - {}".format(err)
                        print(log_output)
                        log.info('Failed to delete old dump files. Error - {}'.format(err))
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        #### End of Function - Older File Delete ####

        #### Function - Version Check ####
            
        def version_check(SID,filename,logfile):

                try:
                
                        #### Checking DB Version ####

                        login_cmd = "export ORACLE_SID=%s\nexport ORAENV_ASK=NO\n. oraenv > /dev/null 2>&1\nsqlplus / as sysdba;" %SID

                        log.info('Running CMD - {}'.format(login_cmd))
                        
                        version_output = net_connect.send_command_timing(login_cmd)
                        log.info('CMD Output - {}'.format(version_output))

                        ## Formatting output to get version details ##
                        
                        ##version = version_output.splitlines()
                        ##version1 = version[11]
                        #print(version)
                        #version1 = version[7]
                        
                        ##version_log = "Version - {}.".format(version1)
                        ##log.info('Version - {}.'.format(version1))
                        ##print(version_log)

                        ## Checking if Oracle version is 12c ##
                        
                        if "12c" in version_output:
                                
                                return "Yes"
                        
                        else:
                                
                                return "No"
                        
                except Exception as err:
                         
                        log_output = "Failed to check DB version of SID {0}. Error - {1}".format(SID,err)
                        print(log_output)
                        log.info('Failed to check DB version of SID {0}. Error - {1}'.format(SID,err))
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        #### End of Function - Version Check ####

        #### Function - Schema Size ####
            
        def Schema_Size(SID,SchemaName,filename,logfile):

                try:
                
                        #### Checking Schema Size ####

                        log.info('Collecting schema size details.')
                        
                        size_cmd = """SELECT SUM(BYTES)/1024/1024/1024 "GB" FROM DBA_SEGMENTS WHERE OWNER = '%s';""" %(SchemaName)

                        log.info('Running CMD - {}'.format(size_cmd))
                        command_output5 = net_connect.send_command_timing(size_cmd)
                        log.info('CMD Output - {}'.format(command_output5))

                        ## Formatting schema size output ##
                        
                        schema_output = command_output5.splitlines()
                        schema_output1 = schema_output[3]
                        if schema_output1:
                            schema_output1 = schema_output1
                        else:
                            schema_output1 = 0.0                      
                        schema_output2 = float(schema_output1)
                        schema_output3 = "{:.2f}".format(schema_output2)
                        
                        
                        size = "Size of {0} schema of {1} SID is {2}gb.".format(SchemaName,SID,schema_output1)                     
                        print(size)
                        log.info('Size of {0} schema of {1} SID is {2}gb'.format(SchemaName,SID,schema_output1))

                        return schema_output3

                except Exception as err:
                         
                        log_output = "Failed to check size of schema {0} of SID {1}. Error - {2}".format(SchemaName,SID,err)
                        print(log_output)
                        log.info('Failed to check size of schema {0} of SID {1}. Error - {2}'.format(SchemaName,SID,err))
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        #### End of Function - Schema Size ####

        #### Function - Username & Tablespace Details Fetch ####
                
        def username_tablespace(SID,SchemaName,filename,logfile):

                try:
                
                        #### Collecting Username, Default Tablespace Details. ####

                        log.info('Collecting username, default tablespace details.')
                        
                        user_cmd = "select USERNAME,DEFAULT_TABLESPACE from dba_users where USERNAME like '%s';" %(SchemaName)

                        log.info('Running CMD - {}'.format(user_cmd))
                        
                        command_output6 = net_connect.send_command_timing(user_cmd)
                        log.info('CMD Output - {}'.format(command_output6))

                        ## Formatting output ##
                        
                        username_output = command_output6.splitlines()
                        username_output1 = username_output[5]
                        default_tablespace = username_output[6]
                        
                        log_output = "Username of schema {0} is {1} and default tablespace is {2}.".format(SchemaName,username_output1,default_tablespace)                     
                        print(log_output)
                        log.info('Username of schema {0} is {1} and default tablespace is {2}.'.format(SchemaName,username_output1,default_tablespace))

                        return username_output1, default_tablespace
                        
                        
                except Exception as err:
                        
                        log_output = "Failed to check username and default tablespace details of {0} schema. Error - {1}".format(SchemaName,err)
                        print(log_output)
                        log.info('Failed to check username and default tablespace details of {0} schema. Error - {1}'.format(SchemaName,err))
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        #### End of Function - Username & Tablespace Details Fetch ####

        #### Function - Distinct Tablespace Details Fetch ####
                
        def distinct_tablespace(SID,SchemaName,filename,logfile):

                try:
                
                        #### Collecting Distinct Tablespace Details. ####

                        log.info('Collecting distinct tablespace details.')
                        
                        tablespace_cmd = "select distinct(TABLESPACE_NAME)  from dba_segments  where OWNER = '%s';" %(SchemaName)

                        log.info('Running CMD - {}'.format(tablespace_cmd))
                        
                        command_output7 = net_connect.send_command_timing(tablespace_cmd)
                        log.info('CMD Output - {}'.format(command_output7))

                        ## Formatting output ##

                        output_formatted = re.sub(r'\SQL>.*','',command_output7,re.I)
                        postFormat = output_formatted.split("\n",3)[3]
                        postFormat1 = postFormat.rsplit("\n",3)[0]
                        
                        log_output = "Distinct tablespace name of schema {0} : \n{1}".format(SchemaName,postFormat1)                     
                        print(log_output)
                        log.info('Distinct tablespace name of schema {0} : \n{1}'.format(SchemaName,postFormat1))

                        return postFormat1
                        
                except Exception as err:
                        
                        log_output = "Failed to check distinct tablespace details of {0} schema. Error - {1}".format(SchemaName,err)
                        print(log_output)
                        log.info('Failed to check distinct tablespace details of {0} schema. Error - {1}'.format(SchemaName,err))
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        #### End of Function - Distinct Tablespace Details Fetch ####

        #### Function - Object Type and Count Details Fetch ####
                
        def object_type(SID,SchemaName,filename,logfile):

                try:
                
                        #### Collecting Object Type and Count Details. ####

                        log.info('Collecting Object Type and Count Details.')

                        objectType_cmd = "select object_type, count(*) from dba_objects where owner = '%s' group by object_type;" %(SchemaName)

                        log.info('Running CMD - {}'.format(objectType_cmd))
                        
                        command_output8 = net_connect.send_command_timing(objectType_cmd)
                        log.info('CMD Output - {}'.format(command_output8))

                        ## Formatting output ##

                        #### Need To Modify Output Based Upon Infra ####
                        postFormat = command_output8.split("\n",3)[3]
                        postFormat1 = postFormat.rsplit("\n",2)[0]
                        #objectType_output1 = postFormat1.replace('\t\t\t\t ',' - ')
                        regex = re.compile(r'[\t]')
                        postFormat2 = regex.sub(" ", postFormat1)
                        objectType_output1 = re.sub(' +', ' - ',postFormat2)
                      
                        log_output = "Object type and count of schema {0} : \n{1}".format(SchemaName,objectType_output1)                     
                        print(log_output)
                        log.info('Object type and count of schema {0} : \n{1}'.format(SchemaName,objectType_output1))

                        return objectType_output1
                        
                except Exception as err:
                        
                        log_output = "Failed to check object type and count details of {0} schema. Error - {1}".format(SchemaName,err)
                        print(log_output)
                        log.info('Failed to check object type and count details of {0} schema. Error - {1}'.format(SchemaName,err))
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        #### End of Function - Object Type and Count Details Fetch ####

        #### Function - Directory Details Fetch ####
                
        def directory_details(SID,filename,logfile):

                try:
                
                        #### Collecting Directory Details. ####

                        log.info('Collecting directory details.')

                        path_input = '/usr/lic/oracle/dba/datapump/{}'.format(SID)
                        
                        directory_cmd = "SELECT OWNER,DIRECTORY_NAME,DIRECTORY_PATH FROM dba_directories where DIRECTORY_PATH like '%s';" %path_input

                        log.info('Running CMD - {}'.format(directory_cmd))
                        
                        command_output9 = net_connect.send_command_timing(directory_cmd)
                        log.info('CMD Output - {}'.format(command_output9))

                        ## Formatting output ##
                        
                        directory_output = command_output9.splitlines()
                        owner_output = directory_output[7]
                        directory = directory_output[8]
                        path_output = directory_output[9]
                        
                        log_output = "Owner : {0}, Directory Name : {1} , Directory Path : {2} and SID : {3}.".format(owner_output,directory,path_output,SID)                     
                        print(log_output)
                        log.info('Owner : {0}, Directory Name : {1} , Directory Path : {2} and SID : {3}.'.format(owner_output,directory,path_output,SID))

                        return owner_output, directory, path_output
                        
                except Exception as err:
                        
                        log_output = "Failed to fetch directory details of {0} SID. Error - {1}".format(SID,err)
                        print(log_output)
                        log.info('Failed to fetch directory details of {0} SID. Error - {1}'.format(SID,err))
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        #### End of Function - Directory Details Fetch ####

        #### Function - Alter Session ####
                
        def alter_session(SID,filename,logfile):

                try:
                
                        #### Alter Session. ####

                        log.info('Checking pdbs.')

                        alter_cmd = "show pdbs;"

                        log.info('Running CMD - {}'.format(alter_cmd))
                        
                        alter_output = net_connect.send_command_timing(alter_cmd)
                        log.info('CMD Output - {}'.format(alter_output))

                        ## Formatting output ##

                        ## Need To Modify Based upon Infra ##
                        
                        postFormat = alter_output.split("\n",4)[4]
                        postFormat1 = postFormat.rsplit("\n",2)[0]
                        regex = re.compile(r'[\t]')
                        postFormat2 = regex.sub(" ", postFormat1)
                        postFormat3 = re.sub(' +', ' ',postFormat2)
                        postFormat4 = postFormat3.splitlines()
                        postFormat5 = postFormat4[0]
                        postFormat6 = list(postFormat5.split(" "))
                        container_name = postFormat6[2]

                        #### Checking Open Mode Permission ####
                        
                        postFormat8 = postFormat6[3]
                        permission1 = str(postFormat8.lower())
                        postFormat9 = postFormat6[4]
                       #print(postFormat6)
                        permission2 = str(postFormat9.lower())
                        restricted = postFormat6[5]
                        restricted1 = str(restricted.lower())

                        if "yes" in restricted1:

                                restricted_check = "Container {0} is having restricted value as YES".format(container_name)
                                print(restricted_check)

                                log.info('{}'.format(restricted_check))
                                log.info('Terminating the script')
                                
                                exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

                        if "read" in  permission1 and "write" in permission2:
                                
                                permission_check = "Container {0} is having {1} {2} as open mode.".format(container_name,permission1,permission2)
                                print(permission_check)

                                log.info('Altering Session.')
                                
                                session_cmd = "alter session set container=%s;" %container_name

                                log.info('Running CMD - {}'.format(session_cmd))
                                
                                alter_output = net_connect.send_command_timing(session_cmd)
                                
                                alter_output1 = str(alter_output)

                                log_output = "Session altered successfully to {} container.".format(container_name)
                                print(log_output)
                                log.info('Session altered successfully to {} container .'.format(container_name))

                                """
                                ## Need to check ##
                                
                                if "Session altered" in alter_output:
                                        
                                        log_output = "Session altered successfully to {} container.".format(container_name)
                                        print(log_output)
                                        log.info('Session altered successfully to {} container .'.format(container_name))

                                else:
                                        
                                        log_output = "Failed to alter session to {} container.".format(container_name)
                                        print(log_output)
                                        log.info('Failed to alter session to {} container .'.format(container_name))
                                        
                                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

                                """
                                                    
                        else:
                                      
                                permission_check = "Container {0} is having {1} {2} as open mode.".format(container_name,permission1,permission2)
                                print(permission_check)
                                log.info('Container {0} is having {1} {2} as open mode.'.format(container_name,permission1,permission2))
                                
                                exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)
                        return container_name        
                                                            
                except Exception as err:
                        
                        log_output = "Failed to alter session. Error - {}".format(err)
                        print(log_output)
                        log.info('Failed to alter session. Error - {}'.format(err))
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        #### End of Function - Alter Session ####

        #### Function - Schema Cleanup ####

        def schema_clean(SID,Password,SchemaName):
                
                try:

                        #### Schema Cleanup. ####

                        log.info('Schema cleanup started.')
                        print("Schema cleanup started.")

                        exit_cmd = "exit"
                        net_connect.send_command_timing(exit_cmd)

                        sysuser = "system"
                        system_login = "sqlplus %s/%s@%s" %(sysuser,Password,SID)
                        
                        clean_cmd = "exec clean_schema ('%s');" %SchemaName
                        
                        log.info('Logging with system user.')
                        
                        login_output = net_connect.send_command_timing(system_login)
                        log.info('CMD Output - {}'.format(login_output))
                        
                        if "logon denied" in login_output :

                            print("System password invalid.")
                            log.info('System password invalid.')
                            exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

                        log.info('Running CMD - {}.'.format(clean_cmd))
                        
                        schema_output = net_connect.send_command_timing(clean_cmd)
                        
                        log.info('CMD Output - {}'.format(schema_output))
                        
                        exit_cmd = "exit"

                        out1 = net_connect.send_command_timing(exit_cmd)
                        
                        log.info('output is - {}'.format(out1))

                        log.info('Logging with sys user.')

                        login_cmd = "sqlplus / as sysdba"
                        
                        out = net_connect.send_command_timing(login_cmd)
                        
                        log.info('output is - {}'.format(out))

                        recyclebin_cmd = "purge dba_recyclebin;"
                       
                        log.info('Running CMD - {}'.format(recyclebin_cmd))
                        
                        recyclebin_output = net_connect.send_command_timing(recyclebin_cmd)
                        
                        log.info('CMD Output - {}'.format(recyclebin_output))
                        
                        net_connect.send_command_timing(exit_cmd)

                        log_output = "Schema cleanup for {0} SID successfully completed.".format(SID)
                        print(log_output)
                        log.info('purge recyclebin for {0} SID successfully completed.'.format(SID))
                        print(recyclebin_output)
                except Exception as err:
        
                        log_output = "Unable to clean {0} schema for {1} SID. Error - {2}".format(SchemaName,SID,err)
                        print(log_output)
                        log.info('Unable to clean {0} schema for {1} SID. Error - {2}'.format(SchemaName,SID,err))
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        #### End of Function - Schema Cleanup ####
       
        ##### Connecting To Source Server #####

        print("------------Source Server------------")
        log.info('------------Source Server------------')

        net_connect = Server_Connection(Source_HostName,Source_UserName,Source_Password,filename,logfile)

        #### Source Server - Switching User ####

        switch_user(Source_UserName,filename,logfile)

        #### Precheck 1 - Source Server SID Validation ####
        
        
        SID_Check(Source_SID,filename,logfile)

        print("------------10% Execution Completed------------")
        log.info('------------10% Execution Completed------------')

        #### Precheck 2 - Deleting 1 Month Old Dump Files of Source Server ####

        # Older_File_Delete(Source_SID,filename,logfile)
        
        #### Precheck 3 - Source Server File System Size for the Mount Point ####

        # FileSystem_Check(Source_SID,filename,logfile)
                        
        #### Precheck 4 - Size of ORADATA File System of Source Server ####
        
        DBVersion = version_check(Source_SID,filename,logfile)
     
        if (DBVersion.lower() == "yes"):

                #### Altering Session Source Server #### 
                
                PDB=alter_session(Source_SID,filename,logfile)
                
                exit_cmd = "exit"
                net_connect.send_command_timing(exit_cmd)
                
                Oradata_FileSystemCheck(PDB,filename,logfile,DBVersion)
                
                
        else:
        
                exit_cmd = "exit"
                net_connect.send_command_timing(exit_cmd)
                
                Oradata_FileSystemCheck(Source_SID,filename,logfile,DBVersion)

        print("------------20% Execution Completed------------")
        log.info('------------20% Execution Completed------------')

        #### Logging To DB And Checking Version Of Source Server ####
        
        log.info('DB Version Check.')
        print("Logging to DB.")
        log.info('Logging to DB.')

        DBVersion = version_check(Source_SID,filename,logfile)

        if (DBVersion.lower() == "yes"):

                #### Altering Session Source Server #### 
                
                PDB=alter_session(Source_SID,filename,logfile)
                
        
        #### Source Server Schema Size Check ####

        schemaSize = Schema_Size(Source_SID,Source_SchemaName,filename,logfile)
        schemaSize1 = float(schemaSize)
        
        #### Source Server Username and Default Tablespace Details Fetch ####

        username_output, default_tablespace = username_tablespace(Source_SID,Source_SchemaName,filename,logfile)

        #### Source Server Distinct Tablespace Details Fetch ####

        distinctTable = distinct_tablespace(Source_SID,Source_SchemaName,filename,logfile)
        
        #### Source Server Object Type and Count Details Fetch ####

        objectType_output = object_type(Source_SID,Source_SchemaName,filename,logfile)
     
        #### Source Server Directory Details Fetch ####

        if (DBVersion.lower() == "yes"):

            owner_output, directory, path_output = directory_details(PDB,filename,logfile)

        else:
            owner_output, directory, path_output = directory_details(Source_SID,filename,logfile)

        #### Source Server invalid obhects Fetch ####

        source_invalid_cmd = "select object_type,object_name from dba_objects where owner='%s' AND STATUS='INVALID';" %(Source_SchemaName)
        log.info('Running CMD - {}.'.format(source_invalid_cmd))
        source_invalid_output = net_connect.send_command_timing(source_invalid_cmd)
        log.info('Source server invalid status output below :\n{}'.format(source_invalid_output))
        print("source schema invalid objects")

        #### Exiting From DB ####

        exit_cmd = "exit"
        
        net_connect.send_command_timing(exit_cmd)
        
        log.info('Exiting from source DB console.')
        print("Exiting from source DB console.")
        
        if (DBVersion.lower() == "yes"):
        
             #### Precheck 2 - Deleting 1 Month Old Dump Files of Source Server ####

            Older_File_Delete(PDB,filename,logfile)

            #### Precheck 3 - Source Server File System Size for the Mount Point ####

            FileSystem_Check(PDB,filename,logfile)

        else:
             #### Precheck 2 - Deleting 1 Month Old Dump Files of Source Server ####

            Older_File_Delete(Source_SID,filename,logfile)

            #### Precheck 3 - Source Server File System Size for the Mount Point ####

            FileSystem_Check(Source_SID,filename,logfile)

        print("------------30% Execution Completed------------")
        log.info('------------30% Execution Completed------------')


        ####  Exporting Schema From Source Server ####
        
        try:

                #### Exporting Schema ####

                log.info('Exporting schema...')
                print("Exporting schema...")
                
                if (DBVersion.lower() == "yes"):
                        Source_SID = PDB

                dumpfile1 = "{0}/{1}_{2}_{3}".format(path_output,Source_SID,Source_SchemaName,dt)
 
                   ## Deleting dump files if same name already exists ##

                Size_threshold = 10.00

                if (schemaSize1 >= Size_threshold):

                    file_delete_cmd = 'rm -f {}_*'.format(dumpfile1)

                    file_delete_output = net_connect.send_command_timing(file_delete_cmd)

                else:

                    file_delete_cmd = 'rm -f {}*'.format(dumpfile1)

                    file_delete_output = net_connect.send_command_timing(file_delete_cmd)
                                      
			## Checking if parfile exists ##
                if (parfile_check.lower() ==  "yes"):

                        log.info('Using parfile for exporting the schema.')
                        
                        parfile_cmd = "expdp userid=system/%s@%s parfile=%s" %(Source_System_Password,Source_SID,source_parfile_path)
                        
                        command_output10 = net_connect.send_command(parfile_cmd,expect_string=r'completed',delay_factor=1000)
                        
                        parfile_nopasswd = "expdp userid=system/****@%s parfile=%s" %(Source_SID,source_parfile_path)
                        
                        export_print = "Export CMD - \n{}".format(parfile_nopasswd)
                        print(export_print)

                        log.info('Running CMD - {}'.format(parfile_nopasswd))
                                                                                     
                        log.info('Export cmd output : \n{}'.format(command_output10))
                
                else:

                        ## Checking if schemasize is more than 10gb ##
                        
                        if (schemaSize1 >= Size_threshold):

                                log.info('Source schema size is more than 10gb.')

                                ## Checking CPU core ##
                                
                                cpu_cmd = 'nproc'
                                CPU_Core = net_connect.send_command_timing(cpu_cmd)
                                CPU_Core = CPU_Core.splitlines()
                                CPU_Core1 = int(CPU_Core[0])
                                cpu_count = int(CPU_Core1 / 2)

                                dump_file = "{0}_{1}_{2}_%U.dmp".format(Source_SID,Source_SchemaName,dt)
                                
                                export_cmd = "expdp system/%s@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log schemas=%s PARALLEL=%s flashback_time=systimestamp" %(Source_System_Password,Source_SID,dump_file,directory,Source_SID,Source_SchemaName,dt,Source_SchemaName,cpu_count)
                                command_output10 = net_connect.send_command(export_cmd,expect_string=r'completed',delay_factor=1000)
                                
                                export_nopasswd = "expdp system/****@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log schemas=%s PARALLEL=%s flashback_time=systimestamp" %(Source_SID,dump_file,directory,Source_SID,Source_SchemaName,dt,Source_SchemaName,cpu_count) 
                  
                                log.info('Running CMD - {}'.format(export_nopasswd))
                                export_print = "Export CMD - \n{}".format(export_nopasswd)
                                print(export_print)

                                log.info('Export cmd output : \n{}'.format(command_output10))

                        else:
                                
                                log.info('Schema size is less than 10gb.')

                                export_cmd = "expdp system/%s@%s dumpfile=%s_%s_%s.dmp directory=%s logfile=%s_%s_%s.log schemas=%s flashback_time=systimestamp" %(Source_System_Password,Source_SID,Source_SID,Source_SchemaName,dt,directory,Source_SID,Source_SchemaName,dt,Source_SchemaName)
                                

                                
                                command_output10 = net_connect.send_command(export_cmd,expect_string=r'completed',delay_factor=1000)

                                export_nopasswd = "expdp system/****@%s dumpfile=%s_%s_%s.dmp directory=%s logfile=%s_%s_%s.log schemas=%s flashback_time=systimestamp" %(Source_SID,Source_SID,Source_SchemaName,dt,directory,Source_SID,Source_SchemaName,dt,Source_SchemaName)
                                
                                log.info('Running CMD - {}'.format(export_nopasswd))
                                export_print = "Export CMD - \n{}".format(export_nopasswd)
                                print(export_print)

                                log.info('Export cmd output : \n{}'.format(command_output10))

                if ("successfully completed" in command_output10):

                        log_output = "Schema exported successfully."                    
                        print(log_output)
                        log.info('Schema exported successfully.')

                elif "logon denied" in command_output10:
                    print("System password invalid.")
                    log.info('System password invalid.')
                    exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

                else:

                        log_output = "Schema export Failed."                    
                        print(log_output)
                        log.info('Schema export Failed.')
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)                       
                
        except Exception as err:

                log_output = "Schema Export Failed. Error - {}".format(err)
                print(log_output)
                log.info('Schema Export Failed. Error - {}'.format(err))
                
                exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        #### End of Exporting Schema From Source Server ####

        #### Copying File To Target Server ####
        finally:

               net_connect.disconnect()
        """       
        try:

                target_dump = "/usr/lic/oracle/dba/datapump/{}".format(Target_SID)
                
                scp_cmd = "sudo scp %s*.dmp oracle@%s:%s" %(dumpfile1,Target_HostName,target_dump)
                log.info('Running CMD - {}'.format(scp_cmd))
                scp_output = net_connect.send_command_timing(scp_cmd)

                ## Checking for yes/no prompt while SCP ##
                
                if "yes" in  scp_output.lower():
                        
                        scp_cmd1 = "yes"
                        scp_output1 = net_connect.send_command_timing(scp_cmd1)
                        
                scp_output2 = net_connect.send_command_timing(oracle_Password)
                log.info('{}'.format(scp_output2))

                log_output = "File copied successfully from source server to target server {} directory.".format(target_dump)                     
                print(log_output)
                log.info('File copied successfully from source to target {} directory.'.format(target_dump))

                

                ## Need to check this ##
                
                if ("10%" in  scp_output2):
                        
                        log_output = "{0} file copied successfully from source to target {1} directory.".format(dump_file,target_dump)                     
                        print(log_output)
                        log.info('{0} file copied successfully from source to target {1} directory.'.format(dump_file,target_dump))
                        
                else:
                                 
                        log_output = "Unable to copy {0} file from source to target {1} directory".format(dump_file,target_dump)                     
                        print(log_output)
                        log.info('Unable to copy {0} file from source to target {1} directory.'.format(dump_file,target_dump))
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

                
                
        except Exception as err:

                log_output = "Dump file copy failed. Error - {}".format(err)
                print(log_output)
                log.info('Dump file copy failed. Error - {}'.format(err))
                
                exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)
         """
        #### End of Copying File To Target Server ####

        ##### Disconnecting from Source Server #####

        ##### finally:
        
              #####  net_connect.disconnect()
        
        print("------------Execution Completed At Source Server------------")
        log.info('------------Execution Completed At Source Server------------')

        print("------------40% Execution Completed------------")
        log.info('------------40% Execution Completed------------')
        
        ##### Connecting To Target Server #####

        print("------------Target Server------------")
        log.info('------------Target Server------------')

        net_connect = Server_Connection(Target_HostName,Target_UserName,Target_Password,filename,logfile)
        
        #### Target Server - Switching User ####

        switch_user(Target_UserName,filename,logfile)
        
        #### Precheck 1 - Target Server SID Validation ####

        SID_Check(Target_SID,filename,logfile)

        print("------------50% Execution Completed------------")
        log.info('------------50% Execution Completed------------')

        #### Precheck 2 - Deleting 1 Month Old Dump Files of Target Server ####

        # Older_File_Delete(Target_SID,filename,logfile)

        #### Precheck 3 - Target Server File System Size for the Mount Point ####

        # FileSystem_Check(Target_SID,filename,logfile)
                        
        #### Precheck 4 - Size of ORADATA File System of Target Server ####
        Target_DBVersion = version_check(Target_SID,filename,logfile)
     
        if (Target_DBVersion.lower() == "yes"):

                #### Altering Session Source Server ####
                
                PDB1=alter_session(Target_SID,filename,logfile)
                
                exit_cmd = "exit"
                net_connect.send_command_timing(exit_cmd)
                
                Oradata_FileSystemCheck(PDB1,filename,logfile,Target_DBVersion)
                
                
        else:
        
                exit_cmd = "exit"
                net_connect.send_command_timing(exit_cmd)
                
                Oradata_FileSystemCheck(Target_SID,filename,logfile,Target_DBVersion)
                
        print("------------60% Execution Completed------------")
        log.info('------------60% Execution Completed------------')

        #### Logging To DB And Checking Version Of Source Server ####
        
        log.info('DB Version Check.')
        print("Logging to DB.")
        log.info('Logging to DB.')

        Target_DBVersion = version_check(Target_SID,filename,logfile)
           
        if (Target_DBVersion.lower() == "yes"):
                
                #### Altering Session Source Server ####
                
                PDB1 = alter_session(Source_SID,filename,logfile)
         
        #### Target Server Directory Details Fetch ####
        
        if (Target_DBVersion.lower() == "yes"):
         
            target_owner, target_directory, target_path = directory_details(PDB1,filename,logfile)
        
        else:
            target_owner, target_directory, target_path = directory_details(Target_SID,filename,logfile)

        #### Target Server Schema Size Check ####

        Target_schemaSize = Schema_Size(Target_SID,Target_SchemaName,filename,logfile)
        Target_schemaSize1 = float(Target_schemaSize)

        #### Target Server Username and Default Tablespace Details Fetch ####

        target_uname, target_tablespace = username_tablespace(Target_SID,Target_SchemaName,filename,logfile)

        #### Target Server Distinct Tablespace Details Fetch ####

        targetDistinct = distinct_tablespace(Target_SID,Target_SchemaName,filename,logfile)

        #### Target Server Object Type and Count Details Fetch ####

        targetObject = object_type(Target_SID,Target_SchemaName,filename,logfile)

        #### Target Server Schema Cleanup ####

        print("Checking clean schema module.")
        log.info('Checking clean schema module.')
                        
        cleanSchema = "select OBJECT_NAME from dba_objects where OBJECT_NAME like 'CLEAN_SCHEMA';"
        log.info('Running CMD - {}'.format(cleanSchema))
        
        cleanSchema_output = net_connect.send_command_timing(cleanSchema)
        
        log.info('{}'.format(cleanSchema_output))

        ## Output formatting ##
        
        cleanSchema1_output = cleanSchema_output.splitlines()
        cleanSchema2_output = cleanSchema1_output[3]

        if "CLEAN_SCHEMA" in cleanSchema2_output:

                print("CLEAN_SCHEMA module is there.")
                log.info('CLEAN_SCHEMA module is there.')
                schema_clean(Target_SID,Target_System_Password,Target_SchemaName)

        else:

                print("CLEAN_SCHEMA module is not there. Terminating the script.")
                log.info('CLEAN_SCHEMA module is not there. Terminating the script.')
                
                exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        print("------------70% Execution Completed------------")
        log.info('------------70% Execution Completed------------')
        
        Target_DBVersion11 = version_check(Target_SID,filename,logfile)

        if (Target_DBVersion11.lower() == "yes"):

                #### Altering Session Source Server ####

                alter_session(Source_SID,filename,logfile)
        
            #### No Archieve Log Mode ####
                
        try:

                ## Checking Log Mode ##

                threshold_schema1 = 10.00

                ## Checking target schema is more than 10gb or not ####

                if (schemaSize1 >= threshold_schema1):

                        log.info('Target schema size is more than 10gb.')
                        print("Target schema size is more than 10gb.")

                        logCheck_cmd = "select name,open_mode from v$database;"

                        log.info('Running CMD - {}'.format(logCheck_cmd))
                                
                        logCheck_output = net_connect.send_command_timing(logCheck_cmd)
                        
                        log.info('{}'.format(logCheck_output))

                        ## Output formatting ##
                        
                        logCheck1_output = logCheck_output.splitlines()
                        logCheck2_output = logCheck1_output[3]
                        logCheck3_output = list(logCheck2_output.split(" "))
                        perm1_out = logCheck3_output[2]
                        perm1 = str(perm1_out.lower())
                        perm2_out = logCheck3_output[3]
                        perm2 = str(perm2_out.lower())

                        ## Checking open mode ##
                        
                        if "read" in  perm1 and "write" in perm2:

                                perm_check = "{0} SID is having {1} {2} as open mode.".format(Target_SID,perm1,perm2)
                                print(perm_check)

                        else:

                                perm_check = "{0} SID is having {1} {2} as open mode.".format(Target_SID,perm1,perm2)
                                print(perm_check)
                                log.info('{0} SID is having {1} {2} as open mode.'.format(Target_SID,perm1,perm2))
                                
                                exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

                        ## Checking log mode ##
                                
                        archive_cmd = "archive log list;"
                        
                        log.info('Running CMD - {}'.format(archive_cmd))
                        
                        archive_output = net_connect.send_command_timing(archive_cmd)
                        
                        log.info('CMD Output - {}'.format(archive_output))

                        ## Output formatting ##
                        
                        archive1_output = archive_output.splitlines()
                        archive2_output = archive1_output[0]
                        
                        if "No Archive Mode" in  archive2_output:
                                
                                print("DB log mode - No Archieve log mode.")
                                log.info('DB log mode - No Archieve log mode')

                        else:
                                
                                log_output = "DB log mode - Archieve log mode."                     
                                print(log_output)
                                log.info('DB log mode - Archieve log mode.')

                                ## Changing DB log mode ##
                                
                                log.info('Changing DB log mode to No Archieve mode.')
                                print("Changing DB log mode to No Archieve mode.")

                                print("Shutting down DB.")
                                log.info('Shutting down DB.')
                                
                                shutdown_cmd = "shutdown immediate;"
                                log.info('Running CMD - {}'.format(shutdown_cmd))
                                
                                shutdown_output = net_connect.send_command(shutdown_cmd,expect_string=r'shut down')
                                log.info('CMD Output - {}'.format(shutdown_output))

                                print("DB starting with mount.")
                                log.info('DB starting with mount.')

                                startup_cmd = "startup mount;"
                                log.info('Running CMD - {}'.format(startup_cmd))
                                
                                startup_output = net_connect.send_command(startup_cmd,expect_string=r'mounted.')
                                log.info('CMD Output - {}'.format(startup_output))

                                print("Altering DB to no archive log mode.")
                                log.info('Altering DB to no archive log mode.')

                                alterlog_cmd = "alter database noarchivelog;"
                                log.info('Running CMD - {}'.format(alterlog_cmd))
                                
                                alterlog_output = net_connect.send_command_timing(alterlog_cmd)
                                log.info('CMD Output - {}'.format(alterlog_output))

                                print("Altering DB to open.")
                                log.info('Altering DB to open.')

                                open_cmd3 = "alter database open;"
                                log.info('Running CMD - {}'.format(open_cmd3))

                                open_output3 = net_connect.send_command_timing(open_cmd3)
                                log.info('CMD Output - {}'.format(open_output3))

                                """
                                print("Shutting down DB.")
                                log.info('Shutting down DB.')
                                 
                                shutdown1_cmd = "shutdown immediate;"
                                log.info('Running CMD - {}'.format(shutdown1_cmd))
                                
                                shutdown1_output = net_connect.send_command(shutdown1_cmd,expect_string=r'shut down')
                                log.info('{}'.format(shutdown1_output))

                                print("DB starting with restrict.")
                                log.info('DB starting with restrict.')

                                restrict_cmd = "startup restrict;"
                                log.info('Running CMD - {}'.format(restrict_cmd))
                                
                                restrict_output = net_connect.send_command(restrict_cmd,expect_string=r'opened.')
                                log.info('{}'.format(restrict_output))
                                """
                                archivelog = "archive log list;"
                                log.info('Running CMD - {}'.format(archivelog))
                                
                                archivelog_output = net_connect.send_command_timing(archivelog)
                                log.info('CMD Output - {}'.format(archivelog_output))

                                logins_command = "select logins from v$instance;"
                                log.info('Running CMD - {}'.format(logins_command))
                                
                                logins_output = net_connect.send_command_timing(logins_command)
                                log.info('CMD Output - {}'.format(logins_output))

                                ## Output formatting ##
                                
                                logins1_output = logins_output.splitlines()
                                logins2_output = logins1_output[3]

                                if "RESTRICTED" in logins2_output :

                                        print("DB log mode is no archive log mode and logins are restricted.")
                                        log.info('DB log mode is no archive log mode and logins are restricted.')
                                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)
                                else:

                                        print("Unable to change DB log mode to no archive or logins to restricted.")
                                        log.info('Unable to change DB log mode to no archive or logins to restricted.')
                                        
                                
        except Exception as err:
                        
                log_output = "Failed to check DB log mode before import. Error - {}".format(err)
                print(log_output)
                log.info('Failed to check DB log mode before import. Error - {}'.format(err))
                
                exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        #### End of No Archieve Log Mode ####

        #### Exiting From DB ####

        exit_cmd = "exit"
        net_connect.send_command_timing(exit_cmd)
        
        log.info('Exiting from target DB console.')
        print("Exiting from target DB console.")

        if (Target_DBVersion.lower() == "yes"):

             #### Precheck 2 - Deleting 1 Month Old Dump Files of Source Server ####

            Older_File_Delete(PDB1,filename,logfile)

            #### Precheck 3 - Source Server File System Size for the Mount Point ####

            FileSystem_Check(PDB1,filename,logfile)

        else:
             #### Precheck 2 - Deleting 1 Month Old Dump Files of Source Server ####

            Older_File_Delete(Target_SID,filename,logfile)

            #### Precheck 3 - Source Server File System Size for the Mount Point ####

            FileSystem_Check(Target_SID,filename,logfile)
        #### Performing scp to copy dumpfile from source to destination ####
        try:
            if (Target_DBVersion.lower() == "yes"):
                target_dump = "/usr/lic/oracle/dba/datapump/{}".format(PDB1)
            else:
                target_dump = "/usr/lic/oracle/dba/datapump/{}".format(Target_SID)
            scp_cmd= "sudo scp oracle@%s:%s*.dmp %s"%(Source_HostName,dumpfile1,target_dump)
            log.info('Running CMD - {}'.format(scp_cmd))
            scp_output = net_connect.send_command_timing(scp_cmd)
            log.info('CMD Output - {}'.format(scp_output))
            ##if "password" in scp_output:
                ##scp_output = net_connect.send_command_timing(oracle_Password)

	 ## Checking for yes/no prompt while SCP ##
            if "yes" in scp_output.lower():
                scp_cmd1 = "yes"
                scp_output1 = net_connect.send_command_timing(scp_cmd1)
            scp_output2 = net_connect.send_command_timing(oracle_Password)
            log.info('CMD Output - {}'.format(scp_output2))
            log_output = "File copied successfully from source server to target server {} directory.".format(target_dump)
            print(log_output)
            log.info('File copied successfully from source to target {} directory.'.format(target_dump))
            permission = "sudo chown oracle:oinstall {0}/{1}_{2}_{3}_*.dmp".format(target_dump,Source_SID,Source_SchemaName,dt)
            print(permission)
            permission1 = "chmod 777 {0}/{1}_{2}_{3}_*.dmp".format(target_dump,Source_SID,Source_SchemaName,dt)
            print(permission1)
            scp_per = net_connect.send_command_timing(permission)
            print(scp_per)
            log.info('CMD Output - {}'.format(scp_per))
            scp_per1 = net_connect.send_command_timing(permission1)
            print(scp_per1)
            log.info('CMD Output - {}'.format(scp_per1))
        except Exception as err:
            log_output = "Dump file copy failed. Error - {}".format(err)
            print(log_output)
            log.info('Dump file copy failed. Error - {}'.format(err))
            exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)
             
        ####  Importing Schema To Target Server ####

        try:

                #### Importing Schema ####

                log.info('Importing schema...')
                print("Importing schema...")

                TargetSizeThreshold = 10.00
                
                ## Checking for parfile ##
                if (Target_DBVersion.lower() == "yes"):
                        Target_SID = PDB1
                
                if (parfile_check.lower() ==  "yes"):

                        log.info('Using parfile for importing the schema.')
                        
                        parfile_import = "impdp userid=system/%s@%s parfile=%s" %(Target_System_Password,Target_SID,target_parfile_path)

                        parfile_nopasswd = "impdp userid=system/****@%s parfile=%s" %(Target_SID,target_parfile_path)

                        log.info('Running CMD - {}'.format(parfile_nopasswd))
                        import_print = "Import CMD - \n{}".format(parfile_nopasswd)
                        print(import_print)
                   
                        import_output = net_connect.send_command(parfile_import,expect_string=r'completed',delay_factor=1000)
                        
                        log.info('Import cmd output : \n{}'.format(import_output))

                else:

                        ## Checking if schema size is more than 10gb ##
                        
                        if (schemaSize1 >= TargetSizeThreshold):

                                log.info('Target schema size is more than 10gb.')

                                ## Checking CPU core ##
                                
                                target_cpu = 'nproc'
                                log.info('Running CMD - {}'.format(target_cpu))
                                Core_Output = net_connect.send_command_timing(target_cpu)
                                log.info('CMD Output - {}'.format(Core_Output))
                                
                                Core_Output = Core_Output.splitlines()
                                Core_Output1 = int(Core_Output[0])
                                #Core_Output1 = int(Core_Output)
                                target_cpu_count = int(Core_Output1 / 2)

                                target_dump_file = "{0}_{1}_{2}_%U.dmp".format(Source_SID,Source_SchemaName,dt)
                                
                                if (Source_SchemaName == Target_SchemaName):
                                                           
                                        if (default_tablespace == target_tablespace):
                                        
                                                ##No remap##
                                                import_cmd = "impdp system/%s@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log schemas=%s PARALLEL=%s" %(Target_System_Password,Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt,Target_SchemaName,target_cpu_count)
                                                
                                                import_nopasswd = "impdp system/****@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log schemas=%s PARALLEL=%s" %(Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt,Target_SchemaName,target_cpu_count)
                                                
                                                import_print = "Import CMD - \n{}".format(import_nopasswd)
                                                print(import_print)

                                                log.info('Running CMD - {}'.format(import_nopasswd))                                                
                                                
                                                import_output = net_connect.send_command(import_cmd,expect_string=r'completed',delay_factor=1000)
                                                
                                                log.info('Import cmd output : \n{}'.format(import_output))
                                                
                                        else:
                                        
                                                ##remap tablespace##
                                                import_cmd = "impdp system/%s@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log schemas=%s PARALLEL=%s remap_tablespace=%s:%s" %(Target_System_Password,Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt,Target_SchemaName,target_cpu_count,default_tablespace,target_tablespace)
                               
                                                import_nopasswd = "impdp system/****@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log schemas=%s PARALLEL=%s remap_tablespace=%s:%s" %(Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt,Target_SchemaName,target_cpu_count,default_tablespace,target_tablespace)
                                                
                                                import_print = "Import CMD - \n{}".format(import_nopasswd)
                                                print(import_print)

                                                log.info('Running CMD - {}'.format(import_nopasswd))                                                
                                                import_output = net_connect.send_command(import_cmd,expect_string=r'completed',delay_factor=1000)
                                                
                                                log.info('Import cmd output : \n{}'.format(import_output))

                                ## Checking if source and target schema name is diffrenet ##
                                
                                if (Source_SchemaName != Target_SchemaName):

                                        if (default_tablespace == target_tablespace):
                                        
                                                ##remap schema##
                                                import_cmd = "impdp system/%s@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log PARALLEL=%s remap_schema=%s:%s" %(Target_System_Password,Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt,target_cpu_count,Source_SchemaName,Target_SchemaName)
                               
                                                import_nopasswd = "impdp system/****@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log PARALLEL=%s remap_schema=%s:%s" %(Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt,target_cpu_count,Source_SchemaName,Target_SchemaName)
                                                
                                                import_print = "Import CMD - \n{}".format(import_nopasswd)
                                                print(import_print)
                                                
                                                log.info('Running CMD - {}'.format(import_nopasswd))
                                                import_output = net_connect.send_command(import_cmd,expect_string=r'completed',delay_factor=1000)
                                                
                                                log.info('Import cmd output : \n{}'.format(import_output))

                                        else:
                                        
                                                ##remap tablespace and schema name##
                                                import_cmd = "impdp system/%s@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log PARALLEL=%s remap_schema=%s:%s remap_tablespace=%s:%s table_exists_action=replace exclude=db_link" %(Target_System_Password,Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt,target_cpu_count,Source_SchemaName,Target_SchemaName,default_tablespace,target_tablespace)
                               
                                                import_nopasswd = "impdp system/****@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log PARALLEL=%s remap_schema=%s:%s remap_tablespace=%s:%s table_exists_action=replace exclude=db_link" %(Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt,target_cpu_count,Source_SchemaName,Target_SchemaName,default_tablespace,target_tablespace)
                                                
                                                import_print = "Import CMD - \n{}".format(import_nopasswd)
                                                print(import_print)
                                                
                                                log.info('Running CMD - {}'.format(import_nopasswd))                                                
                                                import_output = net_connect.send_command(import_cmd,expect_string=r'completed',delay_factor=1000)

                                                
                                                log.info('Import cmd output : \n{}'.format(import_output))
                                      
                        else:
                                
                                log.info('Target schema size is less than 10gb.')

                                target_dump_file = "{0}_{1}_{2}.dmp".format(Source_SID,Source_SchemaName,dt)

                                ## Checking if source and target schema name is same ##
                                
                                if (Source_SchemaName == Target_SchemaName):
                                                           
                                        if (default_tablespace == target_tablespace):
                                                
                                                import_cmd = "impdp system/%s@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log full=y" %(Target_System_Password,Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt)
                                                
                                                import_nopasswd = "impdp system/****@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log full=y" %(Target_System_Password,Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt)
                                                
                                                import_print = "Import CMD - \n{}".format(import_nopasswd)
                                                print(import_print)

                                                log.info('Running CMD - {}'.format(import_nopasswd))                                                
                                                
                                                import_output = net_connect.send_command(import_cmd,expect_string=r'completed',delay_factor=1000)
                                                
                                                log.info('Import cmd output : \n{}'.format(import_output))
                                                
                                        else:
                                                
                                                import_cmd = "impdp system/%s@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log remap_tablespace=%s:%s" %(Target_System_Password,Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt,default_tablespace,target_tablespace)
                                                
                                                import_nopasswd = "impdp system/****@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log remap_tablespace=%s:%s" %(Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt,default_tablespace,target_tablespace)
                                                
                                                import_print = "Import CMD - \n{}".format(import_nopasswd)
                                                print(import_print)

                                                log.info('Running CMD - {}'.format(import_nopasswd))                                                
                                                import_output = net_connect.send_command(import_cmd,expect_string=r'completed',delay_factor=1000)
                                                
                                                log.info('Import cmd output : \n{}'.format(import_output))

                                ## Checking if source and target schema name is diffrenet ##
                                
                                if (Source_SchemaName != Target_SchemaName):

                                        if (default_tablespace == target_tablespace):

                                                import_cmd = "impdp system/%s@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log remap_schema=%s:%s" %(Target_System_Password,Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt,Source_SchemaName,Target_SchemaName)
                                                import_nopasswd = "impdp system/****@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log remap_schema=%s:%s" %(Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt,Source_SchemaName,Target_SchemaName)
                                                
                                                import_print = "Import CMD - \n{}".format(import_nopasswd)
                                                print(import_print)
                                                
                                                log.info('Running CMD - {}'.format(import_nopasswd))
                                                import_output = net_connect.send_command(import_cmd,expect_string=r'completed',delay_factor=1000)
                                                
                                                log.info('Import cmd output : \n{}'.format(import_output))

                                        else:
                                                
                                                import_cmd = "impdp system/%s@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log remap_schema=%s:%s remap_tablespace=%s:%s" %(Target_System_Password,Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt,Source_SchemaName,Target_SchemaName,default_tablespace,target_tablespace)
                                                
                                                import_nopasswd = "impdp system/****@%s dumpfile=%s directory=%s logfile=%s_%s_%s.log remap_schema=%s:%s remap_tablespace=%s:%s" %(Target_SID,target_dump_file,target_directory,Target_SID,Target_SchemaName,dt,Source_SchemaName,Target_SchemaName,default_tablespace,target_tablespace)
                                                
                                                import_print = "Import CMD - \n{}".format(import_nopasswd)
                                                print(import_print)

                                                log.info('Running CMD - {}'.format(import_nopasswd))                                                
                                                import_output = net_connect.send_command(import_cmd,expect_string=r'completed',delay_factor=1000)
                                                
                                                log.info('Import cmd output : \n{}'.format(import_output))

                if ("successfully completed" in import_output):
                        log_output = "Schema imported successfully."
                        print(log_output)
                        log.info('Schema imported successfully.')
                elif ("completed" in import_output):
                        log_output = "Schema imported successfully with errors/warnings."                    
                        print(log_output)
                        log.info('Schema imported successfully with errors/warnings.')

                else:

                        log_output = "Schema import failed."                    
                        print(log_output)
                        log.info('Schema import failed.')
                        
                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)
                
        except Exception as err:

                log_output = "Schema import failed. Error - {}".format(err)
                print(log_output)
                log.info('Schema import failed. Error - {}'.format(err))
                
                exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        ####  Ending of Importing Schema To Target Server ####

        print("------------80% Execution Completed------------")
        log.info('------------80% Execution Completed------------')

        #### Post Import Check ####

        log_output = "Post import check."
        print(log_output)
        log.info('Post import check.')

        #### Logging To DB And Checking Version Of Target Server ####

        import_DBVersion = version_check(Target_SID_1,filename,logfile)
        Target_SID = Target_SID_1

        if (import_DBVersion.lower() == "yes"):

                #### Altering Session Target Server ####
                
                alter_session(Target_SID,filename,logfile)

        #### Archieve Log Mode ####
        
        try:

                #### Checking Log Mode Post Import ####

                threshold_schema2 = 10.00

                ## Checking if schema size is more than 10gb or not ##

                if (schemaSize1 >= threshold_schema2):

                        log.info('Target schema size is more than 10gb.')
                        print("Target schema size is more than 10gb.")

                        log.info('Checking DB log mode post import')
                        print("Checking DB log mode post import")

                        logsCheck_cmd = "select name,open_mode from v$database;"

                        log.info('Running CMD - {}'.format(logsCheck_cmd))
                                
                        logsCheck_output = net_connect.send_command_timing(logsCheck_cmd)
                        log.info('CMD Output - {}'.format(logsCheck_output))

                        ## Output formatting ##
                        
                        logsCheck1_output = logsCheck_output.splitlines()
                        logsCheck2_output = logsCheck1_output[3]
                        logsCheck3_output = list(logsCheck2_output.split(" "))
                        perms1_out = logsCheck3_output[2]
                        perms1 = str(perms1_out.lower())
                        perms2_out = logsCheck3_output[3]
                        perms2 = str(perms2_out.lower())
                        
                        if "read" in  perms1 and "write" in perms2:

                                perms_check = "{0} SID is having {1} {2} as open mode.".format(Target_SID,perms1,perms2)
                                print(perms_check)

                        else:

                                perms_check = "{0} SID is having {1} {2} as open mode.".format(Target_SID,perms1,perms2)
                                print(perms_check)
                                
                                log.info('{0} SID is having {1} {2} as open mode.'.format(Target_SID,perms1,perms2))
                                exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

                        ## Checking log mode ##
                                
                        archive_cmd1 = "archive log list;"
                        log.info('Running CMD - {}'.format(archive_cmd1))
                        
                        archive_output1 = net_connect.send_command_timing(archive_cmd1)
                        log.info('CMD Output - {}'.format(archive_output1))

                        ## Output formatting ##
                        
                        archive1_output1 = archive_output1.splitlines()
                        archive2_output1 = archive1_output1[0]
                        
                        if "No Archive Mode" in  archive2_output1:
                                
                                log_output = "DB log mode - No Archieve log mode."                     
                                print(log_output)
                                log.info('DB log mode - No Archieve log mode.')

                                ## Changing log mode ##
                                
                                log.info('Changing DB log mode to Archieve mode.')
                                print("Changing DB log mode to Archieve mode.")

                                print("Shutting down DB.")
                                log.info('Shutting down DB.')
                                
                                shutdown_cmd1 = "shutdown immediate;"
                                log.info('Running CMD - {}'.format(shutdown_cmd1))
                                
                                shutdown_output1 = net_connect.send_command(shutdown_cmd1,expect_string=r'shut down')
                                log.info('CMD Output - {}'.format(shutdown_output1))

                                print("DB starting with mount.")
                                log.info('DB starting with mount.')

                                startup_cmd1 = "startup mount;"
                                log.info('Running CMD - {}'.format(startup_cmd1))
                                
                                startup_output1 = net_connect.send_command(startup_cmd1,expect_string=r'mounted.')
                                log.info('CMD Output - {}'.format(startup_output1))

                                print("Altering DB to archive log mode.")
                                log.info('Altering DB to archive log mode.')

                                alterlog_cmd1 = "alter database archivelog;"
                                log.info('Running CMD - {}'.format(alterlog_cmd1))
                                
                                alterlog_output1 = net_connect.send_command_timing(alterlog_cmd1)
                                log.info('CMD Output - {}'.format(alterlog_output1))

                                print("Altering DB to open.")
                                log.info('Altering DB to open.')

                                open_cmd1 = "alter database open;"
                                log.info('Running CMD - {}'.format(open_cmd1))
                                
                                open_output1 = net_connect.send_command_timing(open_cmd1)
                                log.info('CMD Output - {}'.format(open_output1))

                                logins_cmd1 = "select logins from v$instance;"
                                log.info('Running CMD - {}'.format(logins_cmd1))
                                
                                logins_output1 = net_connect.send_command_timing(logins_cmd1)
                                log.info('CMD Output - {}'.format(logins_output1))

                                ## Output formatting ##
                                
                                logins1_output1 = logins_output1.splitlines()
                                logins2_output1 = logins1_output1[3]

                                if "ALLOWED" in logins2_output1 :

                                        print("DB log mode is archive log mode and logins are allowed.")
                                        log.info('DB log mode is archive log mode and logins are allowed.')

                                else:

                                        print("Unable to change DB log mode to archive or logins to allowed.")
                                        log.info('Unable to change DB log mode to archive or logins to allowed.')
                                        
                                        exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

                        else:

                                print("DB log mode - Archieve log mode.")
                                log.info('DB log mode - Archieve log mode')
                                
        except Exception as err:
                        
                log_output = "Failed to check DB log mode post import. Error - {}".format(err)
                print(log_output)
                log.info('Failed to check DB log mode post import. Error - {}'.format(err))
                
                exit_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)

        #### End of Archieve Log Mode ####

        #### Post Import - Target Server Schema Size Check ####

        log_output = "Post Import - Target server {0} schema size check of {1} SID.".format(Target_HostName,Target_SID)
        print(log_output)
        log.info('Post Import - Target server {0} schema size check of {1} SID.'.format(Target_HostName,Target_SID)) 

        import_schemaSize = Schema_Size(Target_SID,Target_SchemaName,filename,logfile)
        
        import_schemaSize1 = float(import_schemaSize)

        #### Post Import - Target Server Username and Default Tablespace Details Fetch ####

        log_output = "Post Import - Target server {0} username and default tablespace details fetch {1} SID.".format(Target_HostName,Target_SID)
        print(log_output)
        log.info('Post Import - Target server {0} username and default tablespace details fetch of {1} SID.'.format(Target_HostName,Target_SID))

        import_target_uname, import_target_tablespace = username_tablespace(Target_SID,Target_SchemaName,filename,logfile)

        #### Post Import - Target Server Distinct Tablespace Details Fetch ####

        log_output = "Post Import - Target server {0} distinct tablespace details fetch {1} SID.".format(Target_HostName,Target_SID)
        print(log_output)
        log.info('Post Import - Target server {0} distinct tablespace details fetch of {1} SID.'.format(Target_HostName,Target_SID))

        import_targetDistinct = distinct_tablespace(Target_SID,Target_SchemaName,filename,logfile)

        #### Post Import - Target Server Object Type and Count Details Fetch ####

        log_output = "Post Import - Target server {0} object type and count details fetch {1} SID.".format(Target_HostName,Target_SID)
        print(log_output)
        log.info('Post Import - Target server {0} object yype and count details fetch of {1} SID.'.format(Target_HostName,Target_SID))

        import_targetObject = object_type(Target_SID,Target_SchemaName,filename,logfile)

         
        invalid_cmd = "select object_type,object_name from dba_objects where owner='%s' AND STATUS='INVALID';" %(Target_SchemaName)
        log.info('Running CMD - {}.'.format(invalid_cmd))
        invalid_output = net_connect.send_command_timing(invalid_cmd)
        log.info('Invalid status output below :\n{}'.format(invalid_output))        

        #### End of Post Import Check ####

        #### utlrp.sql CMD ####

        final_cmd = "@?/rdbms/admin/utlrp.sql"
        exit_cmd = "exit"
        
        finalOutput1 = "Running CMD - {}.".format(final_cmd)
        print(finalOutput1)

        log.info('Running CMD - {}.'.format(final_cmd))
        
        last_cmd_output = net_connect.send_command(final_cmd,expect_string=r'Function dropped.')
        log.info('Please find "@?/rdbms/admin/utlrp.sql" cmd output below :\n{}'.format(last_cmd_output))
        
        net_connect.send_command_timing(exit_cmd)

        print("------------Execution Completed At Target Server------------")
        log.info('------------Execution Completed At Target Server------------')

        print("------------90% Execution Completed------------")
        log.info('------------90% Execution Completed------------')
        
        if (import_DBVersion.lower() == "yes"):

            Target_SID = PDB1
        mail_function(filename,Source_HostName,Target_HostName,Source_SID,Target_SID,Source_SchemaName,Target_SchemaName,TicketNumber)
        
except Exception as err:
        
        log_output = "Script Failed. Error - {}".format(err)
        print(log_output)
        log.info('Script Failed. Error - {}'.format(err))
        
        print_output = "You can check script output in {} file.".format(filename)
        print(print_output)
        
        print("******************************************************************************************************************************************")
        exit()

finally:
        if (Target_server[-1] != "p"):        
        
            net_connect.disconnect()
                
#### Scripts Ends Here ####
