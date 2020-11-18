#!/usr/bin/env python
"""
WALOUS_UTS - Copyright (C) <2020> <Service Public de Wallonie (SWP), Belgique,
					          		Institut Scientifique de Service Public (ISSeP), Belgique,
									Université catholique de Louvain (UCLouvain), Belgique,
									Université Libre de Bruxelles (ULB), Belgique>
						 							
	
List of the contributors to the development of WALOUS_UTS: see LICENSE file.
Description and complete License: see LICENSE file.
	
This program (WALOUS_UTS) is free software:
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

import sys
import subprocess
import time
from processing_time import print_processing_time

def ogr2ogr_export(path_to_file, connection_dict, schema=None, table=None, query=None, verbose=False):
    '''Function to export a table or the result of a SQL query in a shapefile using 'ogr2ogr' command from GDAL.

    Args:
        path_to_file (str): Path to the file to be created. The type of format of the file will be interpreted directly from the extension. 
        connection_dict (dict): A dictionnary containing informations for connection to the database. The dictionnary should have the following elements:
        'pg_host' with the server host, 'pg_port' with the server connexion port, 'pg_user' with the name of the user, 'pg_password' with the password of user, 'pg_dbname' with the name of the database.
        schema (str): Name of the schema where the table is located on the database. Default value is None. If provided, the argument 'table' should be provided also and 'query' argument should not be used.
        table (str): Name of the table to be exported. Default value is None. If provided, the argument 'schema' should be provided also and 'query' argument should not be used.
        query (str): A SQL query whose result will be exported. Default value is None. If provided, the argument 'schema' and 'table' should not be provided.
        verbose (bool): Either the command standard output should be printed or not. Default value is False.
    
    Returns:
        This function has no return value. 

    Example: 
        ogr2ogr_export('/path/to/file/to/be/created', {'pg_host':'myserver.mydomain.be','pg_port':'5432','pg_user':'username','pg_password':'userpassword','pg_dbname':'databasename'},
        query="SELECT geom, id, ST_Area(geom) as area FROM schem.table", verbose=False)
    
        ogr2ogr_export('/path/to/file/to/be/created', {'pg_host':'myserver.mydomain.be','pg_port':'5432','pg_user':'username','pg_password':'userpassword','pg_dbname':'databasename'},
        schema='myschema', table='mytable', verbose=False)
    '''
        
    if not query and (not schema or not table):
        sys.exit("ERROR: you should provide either 'table'+'schema' argument OR the query argument.")
    if query and (schema or table):
        sys.exit("ERROR: you should provide either 'table'+'schema' argument OR the query argument.")
    if schema and not table:
        sys.exit("ERROR: table argument should be provided if schema argument is provided.")
    if table and not schema:
        sys.exit("ERROR: schema argument should be provided if table argument is provided.")
    
    try:
        ## Saving current time for processing time management
        begintime = time.time()
        ## ogr2ogr command arguments
        if query:
            command = 'ogr2ogr {0} PG:"host={1} user={2} dbname={3} password={4}" -sql "{5}"'\
            .format(path_to_file,connection_dict['pg_host'],connection_dict['pg_user'],connection_dict['pg_dbname'],connection_dict['pg_password'],query)
        else:
            command = 'ogr2ogr {0} PG:"host={1} user={2} dbname={3} password={4}" {5}.{6}'\
            .format(path_to_file,connection_dict['pg_host'],connection_dict['pg_user'],connection_dict['pg_dbname'],connection_dict['pg_password'],schema, table)

        ## Pass the command line instruction through subprocess library
        p = subprocess.Popen(command,shell=True,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE, encoding='utf8')
        stdout, stderr = p.communicate()

        ## Print informations
        if verbose and stdout:
            print("\n\n############ STANDART OUTPUT ############"+"\n"+stdout)
        if stderr:
            print("\n\n############ STANDART ERROR ############"+"\n"+stderr)
        print("The exportation in shapefile has successfully been made. The file is located here: '%s'\n"%path_to_file)
        print("The command executed is as follow: %s"%command)
        print(print_processing_time(begintime, "\n\nProcess achieved in "))
        
    except:
        sys.exit("ERROR: The exportation failed. Please check.")