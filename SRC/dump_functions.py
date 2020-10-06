#!/usr/bin/env python

### The trick for the use of password using subprocess was found here: https://stackoverflow.com/questions/43380273/pg-dump-pg-restore-password-using-python-module-subprocess

import sys
import subprocess
import shlex
import time
from processing_time import start_processing, print_processing_time

def dump_table(host_name,database_name,user_name,database_password,schema_name,table_name,file_path,psql_stdout=False):
    '''Function for saving a Postgresql table in an archive file.
    To save a complete database, use instead the custom function 'dump_db'.
    
    Args:
        host_name (str): Name or IP adress of the serveur on which the PostgreSQL database is installed.
        database_name (str): Name of the database.
        user_name (str): User's name.
        database_password (str): User's password.
        schema_name (str): Name of the schema containing the table.
        table_name (str): Name of the table to be saved.
        file_path (str): Path to the archive file to be created.
        psql_stdout (bool): Default value is False and the standard output will not be printed.
    
    Returns:
        This function has no return value. 
    '''
    try:
        ## Saving current time for processing time management
        begintime = time.time()
        ## pg_dump command arguments
        command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -t {3}.{4} -Fc -f {5}'.format(host_name,database_name,user_name,schema_name,table_name,file_path)
        print("The command executed to be executed is as follow: %s"%command)
        ## Pass the command line instruction through subprocess library
        p = subprocess.Popen(command,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE, encoding='utf8')
        stdout, stderr = p.communicate('{}\n'.format(database_password))
        ## Print standard ouput and error 
        if psql_stdout:
            print("\n\n############ STANDART OUTPUT ############"+"\n"+stdout)
            print("Back-up of table '%s.%s' has successfully been made and stored on the following location '%s'\n"%(schema_name,table_name,file_path))
        if stderr:
            print("\n\n############ STANDART ERROR ############"+"\n"+stderr)
        print(print_processing_time(begintime, "Process achieved in "))
    except:
        sys.exit("ERROR: Back-up of table '%s.%s' failed. Please check."%(schema_name,table_name))

        
def dump_db(host_name,database_name,user_name,database_password,file_path,psql_stdout=False):
    '''Function for saving a complete Postgresql database in an archive file.
    To save a single table, use instead the custom function 'dump_table'.

    Args:
        host_name (str): name or IP adress of the serveur on which the PostgreSQL database is installed 
        database_name (str): name of the database
        user_name (str): user's name
        database_password (str): user's password
        file_path (str): path to the archive file to be created
        psql_stdout (bool): Default value is False and the standard output will not be printed
    
    Returns:
        This function has no return value. 
    '''
    try:
        ## Saving current time for processing time management
        begintime = time.time()
        ## pg_dump command arguments
        command = 'pg_dump -h {0} -d {1} -U {2} -p 5432 -Fc > {3}'.format(host_name,database_name,user_name,file_path)
        print("The command executed to be executed is as follow: %s"%command)
        ## Pass the command line instruction through subprocess library
        p = subprocess.Popen(command,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE, encoding='utf8')
        stdout, stderr = p.communicate('{}\n'.format(database_password))
        ## Print standard ouput and error 
        if psql_stdout:
            print("\n\n############ STANDART OUTPUT ############"+"\n"+stdout)
            print("Back-up of database '%s' has successfully been made and stored on the following location '%s'\n"%(database_name,file_path))
        if stderr:
            print("\n\n############ STANDART ERROR ############"+"\n"+stderr)
        print(print_processing_time(begintime, "Process achieved in "))
    except:
        sys.exit("ERROR: Back-up of database '%s' failed. Please check."%database_name)
        
        
def restore_psql(host_name,database_name,user_name,database_password,file_path,overwrite=True,psql_stdout=False):
    '''Function for restoring a table or a complete database from an archive file.
    
    Args:
        host_name (str): name or IP adress of the serveur on which the PostgreSQL database is installed 
        database_name (str): name of the database
        user_name (str): user's name
        database_password (str): user's password
        file_path (str): path to the archive file to be use for restoration
        overwrite (bool): Option to allow overwrite of existing database object. If set to false and objects already exists, and error will occur. Default value is True.
        psql_stdout (bool): Control the print of standard output. Default value is False and the standard output will not be printed.
    
    Returns:
        This function has no return value. 
    '''
    try:
        ## Saving current time for processing time management
        begintime = time.time()
        if overwrite:
            ## pg_restore command arguments (clean is to allow overwrite of database objects)
            command = "pg_restore --clean -h {0} -d {1} -U {2} '{3}'".format(host_name,database_name,user_name,file_path)
            command = shlex.split(command)
        else:
            ## pg_dump command arguments
            command = "pg_restore -h {0} -d {1} -U {2} '{3}'".format(host_name,database_name,user_name,file_path)
            command = shlex.split(command)
        print("The command executed to be executed is as follow: %s"%' '.join(command))
        ## Pass the command line instruction through subprocess library
        p = subprocess.Popen(command,shell=False,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE, encoding='utf8')
        stdout, stderr = p.communicate('{}\n'.format(database_password))
        ## Print standard ouput and error 
        if psql_stdout:
            print("\n\n############ STANDART OUTPUT ############"+"\n"+stdout)
            print("Restoration of Postgresql database backup file '%s' has successfully been made.\n"%file_path)
        if stderr:
            print("\n\n############ STANDART ERROR ############"+"\n"+stderr)
        print(print_processing_time(begintime, "Process achieved in "))
    except:
        sys.exit("ERROR: Restoration of Postgresql backup file '%s' failed. Please check."%file_path)