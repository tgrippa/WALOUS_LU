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
import psycopg2
import time
from processing_time import print_processing_time


def create_pg_connexion(connexion_param_dict):
    """Create a connexion to a postgresql DB. 
    
    Args:
        connexion_param_dict (dict): A dictionnary containing informations for connection to the database. The dictionnary should have the following elements:
        'pg_host' with the server host, 'pg_port' with the server connexion port, 'pg_user' with the name of the user, 'pg_password' with the password of user, 'pg_dbname' with the name of the database.

    Returns:
        A psycopg2 connexion object.
    """
    try:
        # Connect to postgres database
        return psycopg2.connect(dbname=connexion_param_dict['pg_dbname'], user=connexion_param_dict['pg_user'], password=connexion_param_dict['pg_password'], host=connexion_param_dict['pg_host'])
        
    except (Exception, psycopg2.Error) as error:
        sys.exit(error)


def create_pg_schema(con, schema_name, overwrite=False):
    """Create or replace a schema in posgtgresql.
    
    Args:
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or using the custom function "create_PG_connexion" with database connexion parameter.
        schema_name (str): Name of the schema to be created.
        overwrite (bool): Either the schema should be overwrited if it already exists. Default value is False.

    Returns:
        This function has no return value. 
    """  
    try:
        # Create cursor
        cursor = con.cursor()
        # Check if schema exists
        cursor.execute("SELECT TRUE FROM information_schema.schemata WHERE schema_name = '%s'"%schema_name)
        exists = cursor.fetchone()
        if exists == None:
            cursor.execute('CREATE SCHEMA %s'%schema_name)
        else:
            if overwrite:
                cursor.execute('DROP SCHEMA %s CASCADE'%schema_name)
                cursor.execute('CREATE SCHEMA %s'%schema_name)
            else:
                sys.exit("ERROR: Schema exists and overwrite argument is set to False")
        # Make the changes to the database persistent
        con.commit()
        # Close connection with database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)
        

def grant_user(con, schema_name, user_name):
    """Grant rights for usage and priviledges for a user on a schema in posgtgresql (for all tables, sequences and functions). 
    The trick was found here : https://stackoverflow.com/questions/22483555/give-all-the-permissions-to-a-user-on-a-db
    
    Args:
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or using the custom function "create_PG_connexion" with database connexion parameter.
        schema_name (str): Name of the schema on which to grand the user.
        user_name (str): Name of the user to be granted with rights.

    Returns:
        This function has no return value. 
    """  
    try:
        # Create cursor
        cursor = con.cursor()
        # Give access on the database
        cursor.execute("GRANT USAGE ON SCHEMA {schema} TO {user};".format(schema=schema_name,user=user_name))
        # Give usage on the schema
        cursor.execute("GRANT USAGE ON SCHEMA {schema} TO {user};".format(schema=schema_name,user=user_name))
        # Give all privilege on all tables, sequences and functions for existing files in the schema 
        cursor.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA {schema} TO {user};".format(schema=schema_name,user=user_name))
        cursor.execute("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA {schema} TO {user};".format(schema=schema_name,user=user_name))
        cursor.execute("GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA {schema} TO {user};".format(schema=schema_name,user=user_name))
        # Change default privilege for tables, sequences and functions to be created in future
        cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL PRIVILEGES ON TABLES TO {user};".format(schema=schema_name,user=user_name))
        cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL PRIVILEGES ON SEQUENCES TO {user};".format(schema=schema_name,user=user_name))
        cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT ALL PRIVILEGES ON FUNCTIONS TO {user};".format(schema=schema_name,user=user_name))
        # Make the changes to the database persistent
        con.commit()
        # Close connection with database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)


def create_index(con, schema_name, table_name, index_column, is_geom=False):
    """Function to create/replace index on a table. 
        
    Args:
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or using the custom function "create_PG_connexion" with database connexion parameter.
        schema_name (str): Name of the schema on which to grand the user.
        table (str): Name of the table on which to add index.
        index_column (str): Name of the column on which to add index.
        is_geom (bool): Either the index is created on a geometry column or not. Default value is False.

    Returns:
        This function has no return value. 
    """
    try:
        # Name of the index
        index_name = '{table}_{col}_idx'.format(table=table_name,col=index_column)
        # Create cursor
        cursor = con.cursor()
        # Drop index if exists
        cursor.execute('DROP INDEX IF EXISTS %s'%index_name)
        con.commit()
        if is_geom:
            query = 'CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} USING gist ({col})'
            query = query.format(index=index_name,schema=schema_name,table=table_name,col=index_column)
            cursor.execute(query)
        else: 
            query = 'CREATE INDEX IF NOT EXISTS {index} ON {schema}.{table} ({col})'
            query = query.format(index=index_name,schema=schema_name,table=table_name,col=index_column)
            cursor.execute(query) 
        con.commit()
        # Close connection with database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)

        
def add_serial_pkey(con, schema_name, table_name, column, pkey=True):
    """Function to add a serial column on a table and optinally make define it as primary key. 
        
    Args:
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or using the custom function "create_PG_connexion" with database connexion parameter.
        schema_name (str): Name of the schema on which to grand the user.
        table (str): Name of the table on which to add index.
        column (str): Name of the serial primary key column to be create.
        pkey (bool): Either to use the serial as primary key on the table or not. Default value is True.  

    Returns:
        This function has no return value. 
    """
    try:
        # Create cursor
        cursor = con.cursor()
        # Alter table
        if pkey: 
            query = 'ALTER TABLE {schema}.{table} ADD COLUMN {col} SERIAL PRIMARY KEY'
        else:
            query = 'ALTER TABLE {schema}.{table} ADD COLUMN {col} SERIAL'
        query = query.format(schema=schema_name,table=table_name,col=column)
        cursor.execute(query)
        con.commit()
        # Close connection with database
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)

        
def prop_coverage(con, basemap_schema, basemap_name, basemap_id, overlaymap_schema, overlaymap_name, **kwargs):  
    """Function to compute the proportion of polygonal geometries from 'basemap', e.g. cadastral parcels, covered by polygonal geometries from 'overlaymap', e.g. vacant lands.
    This execution of this function could be extremely slow if the overlaymap is made of multipolygons. If it is the case, please consider using the function 'split_multipolygon_to_singlepolygon' before this one.

    The trick was found here : https://gis.stackexchange.com/questions/222800/how-to-get-the-area-of-two-intersecting-polygons-on-postgis

    Args:
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or using the custom function "create_PG_connexion" with database connexion parameter.
        basemap_schema (str): Name of the schema where the table with reference units is located.
        basemap_name (str): Name of the table with reference units.
        basemap_id (str): Name of column with unique id in the table with reference units.
        overlaymap_schema (str): Name of the schema where the table with ancillary data (overlaying layer) is located.
        overlaymap_name (str): Name of the table with ancillary data (overlaying layer).
        **kwargs: 
            'class_column' (str): Name of the column containing the type of class for overlaying polygons. If provided, the result table will have multiple columns corresponding to each distinct 'class_column' values.
            'select_extra_column'(tupple): Tupple containing as first element, the name of another column to be used in the group by query, and as second element the type of aggregation to be used, e.g., SUM or MIN.

    Returns:
        This function has no return value. 
    """
    try:
        ## Saving current time for processing time management
        begintime = time.time()
        # Get ID of current processus 
        pid = os.getpid()
        # Create cursor
        cursor = con.cursor()
        # Get list of distinct class name if 'class_column' is provided
        if 'class_column' in kwargs:
            distinctlabelquery = "SELECT DISTINCT {_class} FROM {schema}.{overlay} ORDER BY {_class};"
            distinctlabelquery = distinctlabelquery.format(schema=overlaymap_schema,overlay=overlaymap_name,_class=kwargs['class_column'])
            print(distinctlabelquery + "\n")
            cursor.execute(distinctlabelquery)
            distinct_classes = [x[0] for x in cursor.fetchall()]
        # Drop table if exits
        query = "DROP TABLE IF EXISTS {schema}.{table};"
        if 'class_column' not in kwargs:
            query = query.format(schema=overlaymap_schema,table='%s_overlay_%s'%(basemap_name,overlaymap_name))
        else:
            query = query.format(schema=overlaymap_schema,table='tmp_%s'%pid)
        print(query + "\n")
        cursor.execute(query)
        # Subquery 
        if 'class_column' not in kwargs:
            subquery = "SELECT base.{id_} as {id_}_{suffix}, "
            subquery = subquery.format(id_=basemap_id, suffix=overlaymap_name)
        else:
            subquery = "SELECT base.{id_} as {id_}_{suffix}, overlay.{class_column}, "
            subquery = subquery.format(id_=basemap_id, suffix=overlaymap_name, class_column=kwargs['class_column'])
        subquery += "ROUND(CAST(SUM(st_area(st_Intersection(base.geom,overlay.geom))) AS numeric),4) AS {o_name}_area, "
        subquery += "ROUND(CAST(SUM(st_area(st_Intersection(base.geom,overlay.geom))/st_area(base.geom)) AS numeric),4) AS {o_name}_coverage "
        if 'select_extra_column' in kwargs:
            subquery += ", {aggregate}(overlay.{xtra_col}) as {prefix}_{xtra_col} ".format(
                aggregate=kwargs['select_extra_column'][1].upper(), id_=basemap_id,
                xtra_col=kwargs['select_extra_column'][0], prefix=overlaymap_name)
        subquery += "FROM {b_schema}.{b_name} AS base "
        subquery += "JOIN {o_schema}.{o_name} AS overlay ON st_intersects(base.geom,overlay.geom) "
        subquery = subquery.format(id_=basemap_id, suffix=overlaymap_name, 
                                   b_schema=basemap_schema, o_schema=overlaymap_schema, 
                                   b_name=basemap_name, o_name=overlaymap_name)
        subquery += "GROUP BY base.%s"%basemap_id
        if 'class_column' in kwargs:
            subquery += ", overlay.%s"%kwargs['class_column']
        if 'select_extra_column' in kwargs:
            subquery += ", overlay.%s"%kwargs['select_extra_column'][0]   
            
        # Create table query
        query = "CREATE TABLE {schema}.{table} AS ({subquery});"
        if 'class_column' not in kwargs:
            query = query.format(schema=overlaymap_schema, 
                                 table='%s_overlay_%s'%(basemap_name,overlaymap_name), subquery=subquery)
        else:
            query = query.format(schema=overlaymap_schema, 
                                 table='tmp_%s'%pid, subquery=subquery)
        print(query + "\n")
        cursor.execute(query)
        # Update values where unprecise division result may occur
        query = "UPDATE {schema}.{table} "
        if 'class_column' not in kwargs:
            query = query.format(schema=overlaymap_schema, 
                                 table='%s_overlay_%s'%(basemap_name,overlaymap_name), subquery=subquery)
        else:
            query = query.format(schema=overlaymap_schema, 
                                 table='tmp_%s'%pid, subquery=subquery)
        query += "SET {o_name}_coverage = 1.0 WHERE {o_name}_coverage > 1.0;"
        query = query.format(schema=overlaymap_schema,b_name=basemap_name,o_name=overlaymap_name)
        print(query + "\n")
        cursor.execute(query)
        # Make the changes to the database persistent
        con.commit()
        # If multiclass overlay calculation, pivoting table
        if 'class_column' in kwargs:
            # Crosstab query argument
            crosstabquery = "SELECT {id_}_{suffix}, {class_column}, {suffix}_coverage "
            crosstabquery += "FROM {schema}.{table} ORDER  BY 1,2"
            crosstabquery = crosstabquery.format(id_=basemap_id, suffix=overlaymap_name,
                                                 class_column=kwargs['class_column'],
                                                 schema=overlaymap_schema, table='tmp_%s'%pid)
            # Declaration of colums for the pivot table
            columns=["{id_}_{suffix} VARCHAR".format(id_=basemap_id, suffix=overlaymap_name),]
            [columns.append("{suffix}_prop_{_class} NUMERIC".format(suffix=overlaymap_name,_class=_class)) for _class in distinct_classes]
            
            # Queries for pivoting table
            query = "DROP TABLE IF EXISTS {schema}.{table};"
            query = query.format(schema=overlaymap_schema,table='%s_overlay_%s'%(basemap_name,overlaymap_name))
            print(query + "\n")
            cursor.execute(query)
            
            subquery = "SELECT * FROM crosstab('{crosstabquery}', '{distinctlabelquery}') "
            subquery += " AS ct ({columns_declaration})"
            subquery = subquery.format(crosstabquery=crosstabquery, 
                                       distinctlabelquery=distinctlabelquery,
                                       columns_declaration=','.join(columns))
            # Create table query
            query = "CREATE TABLE {schema}.{table} AS ({subquery});"
            query = query.format(schema=overlaymap_schema, 
                                 table='%s_overlay_%s'%(basemap_name,overlaymap_name), 
                                 subquery=subquery)
            print(query + "\n")
            cursor.execute(query)                 
            # Remove temporary table if needed
            query = "DROP TABLE {schema}.{table};"
            query = query.format(schema=overlaymap_schema, 
                                 table='tmp_%s'%pid)
            print(query + "\n")
            cursor.execute(query)   
            # Make the changes to the database persistent
            con.commit()
        # Close connection with database
        cursor.close()
        ## Print processing time
        print(print_processing_time(begintime, "Computation of PropCoverage function achieved in "))
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)
        

def count_points(con, basemap_schema, basemap_name, basemap_id, overlaymap_schema, overlaymap_name, **kwargs):
    """Function to compute the count of points from a 'overlaymap' map, e.g. windturbines, that are inside the polygonal geometries of a 'basemap', e.g. cadastral parcels.
    
    Args:
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or using the custom function "create_PG_connexion" with database connexion parameter.
        basemap_schema (str): Name of the schema where the table with reference units is located.
        basemap_name (str): Name of the table with reference units.
        basemap_id (str): Name of column with unique id in the table with reference units.
        overlaymap_schema (str): Name of the schema where the table with ancillary data (overlaying layer) is located.
        overlaymap_name (str): Name of the table with ancillary data (overlaying layer).
        **kwargs: 
            'class_column' (str): Name of the column containing the type of class for overlaying polygons. If provided, the result table will have multiple columns corresponding to each distinct 'class_column' values.
            'select_extra_column'(tupple): Tupple containing as first element, the name of another column to be used in the group by query, and as second element the type of aggregation to be used, e.g., SUM or MIN.

    Returns:
        This function has no return value. 
    """
    try:
        ## Saving current time for processing time management
        begintime = time.time()    
        # Get ID of current processus 
        pid = os.getpid()
        # Create cursor
        cursor = con.cursor()
        # Get list of distinct class name if 'class_column' is provided
        if 'class_column' in kwargs:
            distinctlabelquery = "SELECT DISTINCT {_class} FROM {schema}.{overlay} ORDER BY {_class};"
            distinctlabelquery = distinctlabelquery.format(schema=overlaymap_schema,overlay=overlaymap_name,_class=kwargs['class_column'])
            print(distinctlabelquery + "\n")
            cursor.execute(distinctlabelquery)
            distinct_classes = [x[0] for x in cursor.fetchall()]
        # Drop table if exits
        query = "DROP TABLE IF EXISTS {schema}.{table};"
        if 'class_column' not in kwargs:
            query = query.format(schema=overlaymap_schema,table='%s_count_%s'%(basemap_name,overlaymap_name))
        else:
            query = query.format(schema=overlaymap_schema,table='tmp_%s'%pid)
        print(query + "\n")
        cursor.execute(query)
        # WITH Query 
        withquery = "WITH tmp AS (SELECT a.*,b.{id_} FROM {o_schema}.{o_name} as a "
        withquery += "JOIN {b_schema}.{b_name} as b ON ST_Intersects(a.geom,b.geom))"
        withquery = withquery.format(id_=basemap_id, b_schema=basemap_schema, b_name=basemap_name,
                                     o_schema=overlaymap_schema, o_name=overlaymap_name)        
        # MAIN query 
        if 'class_column' not in kwargs:
            mainquery = "{with_} SELECT {id_} as {id_}_{o_name}, count(*) as {suffix}_count FROM tmp "
            mainquery = mainquery.format(with_=withquery, id_=basemap_id, 
                                         o_name=overlaymap_name, suffix=overlaymap_name)
        else:
            mainquery = "{with_} SELECT {id_} as {id_}_{o_name}, {_class}, count(*) FROM tmp "
            mainquery = mainquery.format(with_=withquery, 
                                        id_=basemap_id, o_name=overlaymap_name, 
                                        _class=kwargs['class_column'])    
        mainquery += "GROUP BY %s"%basemap_id
        if 'class_column' in kwargs:
            mainquery += ", %s"%kwargs['class_column']          
        # Create table query
        query = "CREATE TABLE {schema}.{table} AS ({mainquery});"
        if 'class_column' not in kwargs:
            query = query.format(schema=overlaymap_schema, 
                                 table='%s_count_%s'%(basemap_name,overlaymap_name), mainquery=mainquery)
        else:
            query = query.format(schema=overlaymap_schema, 
                                 table='tmp_%s'%pid, mainquery=mainquery)
        print(query + "\n")
        cursor.execute(query)
        # Make the changes to the database persistent
        con.commit()
        # If multiclass overlay calculation, pivoting table
        if 'class_column' in kwargs:
            # Crosstab query argument
            crosstabquery = "SELECT {id_}_{suffix}, {class_column}, count "
            crosstabquery += "FROM {schema}.{table} ORDER  BY 1,2"
            crosstabquery = crosstabquery.format(id_=basemap_id, suffix=overlaymap_name,
                                                 class_column=kwargs['class_column'],
                                                 schema=overlaymap_schema, table='tmp_%s'%pid)
            # Declaration of colums for the pivot table
            columns=["{id_}_{suffix} VARCHAR".format(id_=basemap_id, suffix=overlaymap_name),]
            [columns.append("{suffix}_count_{_class} NUMERIC".format(suffix=overlaymap_name,_class=_class)) for _class in distinct_classes]
            # Queries for pivoting table
            query = "DROP TABLE IF EXISTS {schema}.{table};"
            query = query.format(schema=overlaymap_schema,table='%s_count_%s'%(basemap_name,overlaymap_name))
            print(query + "\n")
            cursor.execute(query)
            subquery = "SELECT * FROM crosstab('{crosstabquery}', '{distinctlabelquery}') "
            subquery += " AS ct ({columns_declaration})"
            subquery = subquery.format(crosstabquery=crosstabquery, 
                                       distinctlabelquery=distinctlabelquery,
                                       columns_declaration=','.join(columns))
            # Create table query
            query = "CREATE TABLE {schema}.{table} AS ({subquery});"
            query = query.format(schema=overlaymap_schema, 
                                 table='%s_count_%s'%(basemap_name,overlaymap_name), 
                                 subquery=subquery)
            print(query + "\n")
            cursor.execute(query)                 
            # Remove temporary table if needed
            query = "DROP TABLE {schema}.{table};"
            query = query.format(schema=overlaymap_schema, 
                                 table='tmp_%s'%pid)
            print(query + "\n")
            cursor.execute(query)   
            # Make the changes to the database persistent
            con.commit()
        # Close connection with database
        cursor.close()
        ## Print processing time
        print(print_processing_time(begintime, "Computation of CountPoints function achieved in "))
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)
        

def sum_points(con, basemap_schema, basemap_name, basemap_id, overlaymap_schema, overlaymap_name, overlaymap_sumcolumn):
    """Function to compute the sum of an attribute column of points from a 'overlaymap' map, e.g. windturbines, that are inside the polygonal geometries of a 'basemap', e.g. cadastral parcels.
    
    Args:
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or using the custom function "create_PG_connexion" with database connexion parameter.
        basemap_schema (str): Name of the schema where the table with reference units is located.
        basemap_name (str): Name of the table with reference units.
        basemap_id (str): Name of column with unique id in the table with reference units.
        overlaymap_schema (str): Name of the schema where the table with ancillary data (overlaying layer) is located.
        overlaymap_name (str): Name of the table with ancillary data (overlaying layer).
        overlaymap_sumcolumn (str): Name of the column containing values for which the sum should be computed.

    Returns:
        This function has no return value. 
    """
    try:
        ## Saving current time for processing time management
        begintime = time.time()
        # Create cursor
        cursor = con.cursor()
        # Drop table if exits
        query = "DROP TABLE IF EXISTS {o_schema}.{b_name}_sum_{o_name};"
        query = query.format(o_schema=overlaymap_schema, b_name=basemap_name, o_name=overlaymap_name)
        print(query + "\n")
        cursor.execute(query)
        # Query 
        withquery = "WITH tmp AS (SELECT a.*,b.{id_} FROM {o_schema}.{o_name} as a "
        withquery += "JOIN {b_schema}.{b_name} as b ON ST_Intersects(a.geom,b.geom))"
        withquery = withquery.format(id_=basemap_id, b_schema=basemap_schema, b_name=basemap_name,
                                     o_schema=overlaymap_schema, o_name=overlaymap_name)
        
        mainquery = "{with_} SELECT {id_} as {id_}_{suffix}, sum({sum_col}) as {sum_col}_tot, "
        mainquery += "count(*) as count_{o_name}_points FROM tmp GROUP BY {id_}"
        mainquery = mainquery.format(with_=withquery, id_=basemap_id, suffix=overlaymap_name,
                                     sum_col=overlaymap_sumcolumn, o_name=overlaymap_name)
        # Create table query
        query = "CREATE TABLE {schema}.{b_name}_sum_{o_name} AS ({subquery});"
        query = query.format(schema=overlaymap_schema,b_name=basemap_name,
                             o_name=overlaymap_name, subquery=mainquery)
        print(query + "\n")
        cursor.execute(query)
        # Make the changes to the database persistent
        con.commit()
        # Close connection with database
        cursor.close()
        ## Print processing time
        print(print_processing_time(begintime, "Computation of SumPoints function achieved in "))
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)
        

def get_final_table(con, table_name, join_table_informations):
    """Function to create the final table joining the cadastral parcels with all other tables.
    
    Args:
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or using the custom function "create_PG_connexion" with database connexion parameter.
        table_name (str): Name of the table to be created.
        join_table_informations (list of tupple): A list of tupple containing the informations about the different tables to be jointed together. 
        The first element (str) of each tupple should contain the name of the schema where the table is located, the second element (str) should contain the name 
        of the table to be jointed, and the third element (str) should contain the foreign key to be used with the capakey.
   
    Returns:
        This function has no return value. 
    """
    ## Saving current time for processing time management
    begintime = time.time()   
    # Create cursor
    cursor = con.cursor()
    # Drop table if exits
    query = "DROP TABLE IF EXISTS results.{table_name};".format(table_name=table_name)
    cursor.execute(query)
    print(query + "\n")
    # Query 
    mainquery = "SELECT a.geom, a.capakey, "
    mainquery += ", ".join(["b_%s.*"%i for i,x in enumerate(join_table_informations,1)])  
    mainquery += " FROM agdp.capa AS a "
    for i,table_info in enumerate(join_table_informations,1):
        mainquery += "LEFT JOIN {sche}.{tabl} AS b_{i} ON a.capakey=b_{i}.{z} ".format(sche=table_info[0],tabl=table_info[1],
                                                                          i=i,z=table_info[2])
    # Create table query
    query = "CREATE TABLE results.{create_table} AS ({mainquery});"
    query = query.format(mainquery=mainquery,create_table=table_name)
    cursor.execute(query)
    print(query + "\n")
    # Drop column query
    query = "ALTER TABLE results.{table_name} ".format(table_name=table_name)
    list_of_drop_queries = []
    list_of_drop_queries.append("DROP COLUMN IF EXISTS cat")
    [list_of_drop_queries.append("DROP COLUMN IF EXISTS %s"%x[2]) for x in join_table_informations]
    query += ", ".join(list_of_drop_queries)  
    query += ";"       
    cursor.execute(query)
    print(query + "\n")
    # Make the changes to the database persistent
    con.commit()
    # Close connection with database
    cursor.close()
    ## Print processing time
    print(print_processing_time(begintime, "Creation of final table achieved in "))
    
    