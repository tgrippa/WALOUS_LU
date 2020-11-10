#!/usr/bin/env python
"""
WALOUS_LU - Copyright (C) 2020 Université catholique de Louvain (UCLouvain), Belgique
					 Université Libre de Bruxelles (ULB), Belgique 
					 Institut Scientifique de Service Public (ISSeP), Belgique 
					 Service Public de Wallonie (SWP), Belgique 
						 							
	
List of the contributors to the development of WALOUS_LU: see LICENSE file.
Description and complete License: see LICENSE file.
	
This program (WALOUS_LU) is free software: 
you can redistribute it and/or modify it under the terms of the 
GNU General Public License as published by the Free Software 
Foundation, either version 3 of the License, or (at your option) 
any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program (see COPYING file).  If not, 
see <http://www.gnu.org/licenses/>.
"""

import os
import sys
from io import StringIO
import subprocess
import psycopg2
import tempfile
import time
from processing_time import print_processing_time
import platform

def shp2pgsql(data_tuple, schema, connection_dict, from_srid='31370', to_srid='31370', create_opt='-d', 
              psql_stdout=None, quiet=True):
    '''Function to import existing shapefile in PostGIS database using the 'shp2pgsql' command from PostGIS.
    The trick to pass pipe bash command in Python was found here: https://www.bogotobogo.com/python/python_subprocess_module.php.
    Shp2psql cheatsheet is available here: https://www.bostongis.com/pgsql2shp_shp2pgsql_quickguide.bqg.
    
    Args:
        data_tuple (tuple of str): Tuple containing in first position (data_tuple[0]) the name of the table to create in postgres
        and in second position (data_tuple[1]) the path to the shapefile in your computer.
        schema (str): The name of the schema in which to create the table. The schema should already exists because the function do not perform check prior to copy. 
        connexion_param_dict (dict): A dictionnary containing informations for connection to the database. The dictionnary should have the following elements:
        'pg_host' with the server host,  'pg_dbname' with the name of the database, 'pg_user' with the name of the user.
        from_srid (str): The wait ESPG code of the actual coordinate reference system (CRS) of the dataset. Default value is '31370'.
        to_srid (str): The wait ESPG code of the actual coordinate reference system (CRS) of the dataset. Default value is '31370'.
        create_opt (str): The mode of creation corresponding the the flag used in shp2pgsql ('-d', '-a', '-c', '-p'). Default value is '-d' (replace existing table if exists).
        quiet (bool): Either the standard output of the psql command should be displayed (False) or not (True). Default value is True, which reduces significantly 
        the output of the psql command. When set to False, an error "IOPub data rate exceeded" could appear in case of very large dataset.
    
    Returns:
        This function has no return value. 

    To do:
        - Add check if schema exists before trying to import the shapefile.
        - When create_opt='-d', add check if the table exists in the database and if not change the parameter to create_opt='-c'. If table exisits and -d flag, an error will occur.
        - Change the function so that is rely on the use of ogr2ogr in order to be able to import Geopackages.
    '''
    ## Saving current time for processing time management
    begintime_import = time.time()

    # Define popen 1 and popen 2 commands
    p1_cmd = ['shp2pgsql', '-t', '2D', '-s', '%s:%s'%(from_srid,to_srid), 
              create_opt, '-I', data_tuple[1], '%s.%s'%(schema,data_tuple[0])]
    p2_cmd = ['psql', '-h', connection_dict['pg_host'], 
              '-d', connection_dict['pg_dbname'], 
              '-U',connection_dict['pg_user']]
    if quiet:
        p2_cmd.insert(1,'--quiet')
        
    # Print the bash command to be executed using subprocess
    print("Bash command to be executed:")
    print("'%s | %s'"%(' '.join(p1_cmd),' '.join(p2_cmd)))

    # Launch the process using subprocess
    p1 = subprocess.Popen(p1_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf8')
    if psql_stdout:
        p2 = subprocess.Popen(p2_cmd, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf8')
    else:  # If no command output needed - or to avoid "IOPub data rate exceeded" error
        p2 = subprocess.Popen(p2_cmd, stdin=p1.stdout, stdout=None, stderr=subprocess.PIPE, encoding='utf8')
    p1.stdout.close()
    stdout, stderr=p2.communicate()
    
    # Print message
    if psql_stdout:
        print("\n\n############ STANDART OUTPUT ############"+"\n"+stdout)
    if stderr:
        print("\n\n############ STANDART ERROR ############"+"\n"+stderr)
        sys.exit("\n\nERROR: An error occured during the importation of shapefile '%s'. Please check."%data_tuple[1])
    if not stderr:
        print("\n\nShapefile '%s' successfully imported in table '%s.%s' on PostGIS database '%s' hosted on %s "%(data_tuple[1],schema,data_tuple[0],connection_dict['pg_dbname'],connection_dict['pg_host']))
    ## Print processing time
    print(print_processing_time(begintime_import, "\n\nImportation achieved in "))

def ogr2ogr_import(path_to_file, connection_dict, schema=None, table=None, to_srid='31370', create_opt='-overwrite', verbose=False):
    '''Function to import a file (shapefile, geopackage, ..) into a postgresql table using 'ogr2ogr' command from GDAL.

    Args:
        path_to_file (str): Path to the file to be created. The type of format of the file will be interpreted directly from the extension. 
        connection_dict (dict): A dictionnary containing informations for connection to the database. The dictionnary should have the following elements:
        'pg_host' with the server host, 'pg_port' with the server connexion port, 'pg_user' with the name of the user, 'pg_password' with the password of user, 'pg_dbname' with the name of the database.
        schema (str): Name of the schema where to create the table in the database. Default value is None. If provided, the table will be created in the schema indicated by the user, otherwise it will be created in public schema.
        table (str): Name of the table to be created. Default value is None. If provided, the table will be created using the name indicated by the user, otherwise it will be created using the file name.
        to_srid (str): The wait ESPG code of the actual coordinate reference system (CRS) of the dataset. Default value is '31370'.
        create_opt (str): The mode of creation corresponding the the flag used in ogr2ogr (-append, -overwrite or -update). Default value is '-overwrite' (replace existing table if exists).
        verbose (bool): Either the command standard output should be printed or not. Default value is False.
    
    Returns:
        This function has no return value. 

    Example:    
        ogr2ogr_import('/path/to/file/to/be/created', {'pg_host':'myserver.mydomain.be','pg_port':'5432','pg_user':'username','pg_password':'userpassword','pg_dbname':'databasename'},
        schema='myschema', table='mytable', to_srid='31370', create_opt='-overwrite', verbose=False)
    '''       
    
    try:
        ## Saving current time for processing time management
        begintime = time.time()
        ## ogr2ogr command arguments
        if not table:
            table = os.path.split(os.path.splitext(path_to_file)[0])[-1]
        command = [] #Empty list containing ogr_ogr elements    
        # Base command for importation in PosgtreSQL database. -nlt option is used to allow multipolygons. -dim option is used to specify 2d. -lco option is used to force geometry column name. 
        command.append('ogr2ogr -f "PostgreSQL" PG:"host={0} user={1} dbname={2} password={3}" {4} -lco GEOMETRY_NAME=geom -nlt PROMOTE_TO_MULTI -dim XY'\
            .format(connection_dict['pg_host'],connection_dict['pg_user'],connection_dict['pg_dbname'],connection_dict['pg_password'],create_opt))
        if to_srid:  
            # Use -t_srs option to reproject/transform to this SRS on output.
            command.append('-t_srs EPSG:{0}'.format(to_srid,to_srid))  
        if schema:  
            # Use -nln option to assign an alternate name to the new layer
            command.append('-nln {0}.{1}'.format(schema,table))  
        # Add patht to the file to be imported
        command.append('{0}'.format(path_to_file))
            
        ## Pass the command line instruction through subprocess library
        p = subprocess.Popen(" ".join(command),shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE, encoding='utf8')
        stdout, stderr = p.communicate()

        ## Print informations
        if verbose and stdout:
            print("\n\n############ STANDART OUTPUT ############"+"\n"+stdout)
        if stderr:
            print("\n\n############ STANDART ERROR ############"+"\n"+stderr)
        print("The importation of data has successfully been made.")
        print("The command executed is as follow: %s"%" ".join(command))
        print(print_processing_time(begintime, "\n\nProcess achieved in "))
        
    except:
        sys.exit("ERROR: The exportation failed. Please check.")

def import_csv(con, csv, column_definition, schema, table, delimiter=";", null="", add_serial_primary_key=False, overwrite=False):
    '''Function that import a CSV file into PostgreSQL.
    The tricks found here https://stackoverflow.com/a/50034387.
    
    Args:
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or using the custom function "create_PG_connexion" with database connexion parameter.
        csv (str): The path to the .csv file.
        column_definition (list of tuple): A list of tuple with the column name (str) and datatype (str) for the "CREATE TABLE" statement (see example section of the docstring).
        schema (str): The name of the schema.
        table (str): The name of the table.
        delimiter (str): The delimiter character used in the csv file.
        null (str): The character representing null values in the csv file.
        add_serial_primary_key (bool): Either to add (True) a serial primary key to the table during importation or not (False). Default value is False.
        If set to False and duplicates exists on the first column
        overwrite (bool): Either an existing table with the same name should be overwritten (True) or not (False). Default value is False.
    
    Returns:
        This function has no return value. 

    To do:
        - Use the PostgreSQL command 'ALTER TABLE xxxxx ADD COLUMN ID SERIAL PRIMARY KEY;' for adding auto-incremental primary key.
        - Add check if table already exists and sys.exit() in case if overwrite parameter is set to False.
        - Potential improvement: auto check for encoding compatibility with posgresql standards https://docs.postgresql.fr/8.3/multibyte.html.

    Example: 
        import_csv(con=create_PG_connexion(config_parameters), csv='path/to/csv/file.csv', 
        column_definition=[('propertySituationIdf','integer PRIMARY KEY'),('divCad','integer'),('section','varchar')], schema='schema_name',
        table='table_name', delimiter=';', add_serial_primary_key=False, overwrite=False)
    '''
    try:
        ## Saving current time for processing time management
        begintime_copy=time.time()
        
        ## Determine automatically the encoding of the file - ONLY FOR LINUX
        if platform.uname()[0] == 'Linux': 
            cmd = ['file', '-b', '--mime-encoding', csv]
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding='utf8')
            stdout, stderr=p.communicate()
            csv_encoding = stdout.split("\n")[0]

            # If decoding and encoding to UTF-8 is needed
            if csv_encoding.lower() != 'utf-8':
                print('The csv file is encoded in %s. A utf-8 copy will be used for copy in Postgresql.'%csv_encoding.lower())
                # Create a new file that is coded as UTF8 since the original .csv is coded as 'iso-8859-1' which is not 
                # compatible with PostgreSQL
                path,ext = os.path.splitext(csv)
                csv_tmp = os.path.join(tempfile.gettempdir(),'%s_utf8%s'%(os.path.split(path)[-1],ext))
                fin = open(csv_tmp, 'w')
                with open(csv, 'r', encoding=csv_encoding) as f:
                    for row in f:
                        # Write row decode from user specified encoding and encoded to 'utf8'
                        fin.write(row)
                fin.close()
                csv = csv_tmp      
        
        ##
        ## TODO: add management of overwrite option
        ##
        
        # Print
        print("Creating new table copy csv file in the postgresql table")
        # Add serial primary key if needed
        if add_serial_primary_key:
            column_definition.insert(0,('id', 'serial primary key'))
        # Create cursor
        cursor = con.cursor()
        # Create table query
        query="DROP TABLE IF EXISTS %s.%s;"%(schema,table)
        query+="CREATE TABLE %s.%s ("%(schema,table)
        query+=", ".join(['%s %s'%(column_name, column_type) for column_name, column_type in column_definition])
        query+=");"

        # Print the query
        print(query.replace(";",";\n"))
        # Execute the CREATE TABLE query 
        cursor.execute(query)
        # Make the changes to the database persistent
        con.commit()
        
        # Print
        print("Start copy csv file in the postgresql table")  
        # Psycopg2 COPY FROM function 
        with open(csv, 'r') as f:
            next(f)  # Skip the header row.
            #Clean the content of the file - Ensure there is only one newline return (\n) and no carriage return (\r) 
            content = [line.split("\n")[0].replace("\r","") for line in f]
            if add_serial_primary_key:
                content = ['%s,%s'%(i,x) for i,x in enumerate(content,1)]
            content = StringIO('\n'.join(content)) 
            cursor.copy_from(content, '%s.%s'%(schema,table), sep=delimiter, null=null)
        # Make the changes to the database persistent
        con.commit()    
        # Close connection with database
        cursor.close()
        # Remove temporary file if needed (if linux)
        if platform.uname()[0] == 'Linux': 
            if csv_encoding.lower() != 'utf-8':
                os.remove(csv)
        ## Compute processing time and print it
        print(print_processing_time(begintime_copy, "\n\nProcess achieved in "))
        
    except (Exception, psycopg2.Error) as error:
        sys.exit(error)
