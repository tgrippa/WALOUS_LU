#!/usr/bin/env python

import os
import sys
import psycopg2
import time
from processing_time import print_processing_time

def make_valid(con, schema, table, geomcolumn, geometry_type=3, quiet=False):
    """Function to update invalid geometries in a PostgreSQL/GIS table in order to make them valid using ST_Makevalid() function.
    Idealy, this function should be executed after each importation of raw data in the PostgreSQL/GIS database.

    Args:
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or using the custom function "create_PG_connexion".
        schema (str): Name of the schema where the table is stored.
        table (str): Name of the table whose geometries should be fixed.
        geomcolumn (str): Name of the geometry column.
        geometry_type (str): Type of the geometry. 1 for POINT, 2 for LINESTRING, 3 for POLYGON. Default value is 3.
        quiet (bool): Either the function should print (False) output or not (True). Default value is False.

    Returns:
        This function has no return value. 
    """
    try:
        ## Saving current time for processing time management
        begintime_copy = time.time()
        # Create cursor
        cursor = con.cursor()
        # Update geom with ST_Makevalid function query
        query="UPDATE %s.%s "%(schema,table)
        query+="SET {geom} = St_Multi(St_Collectionextract(St_Makevalid({geom}),3)) ".format(geom=geomcolumn)
        query+="WHERE ST_isvalid({geom}) is False".format(geom=geomcolumn)
        query+=";"
        # Print the query
        if not quiet:
            print(query)
        # Execute the CREATE TABLE query
        cursor.execute(query)
        # Make the changes to the database persistent
        con.commit()
        # Close connection with database
        cursor.close()
        ## Compute processing time and print it
        if not quiet:
            print(print_processing_time(begintime_copy, "Process achieved in "))
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)


def split_multipolygon_to_singlepolygon(con, schema, table, idcolumn, geomcolumn):
    """Function to split multiplygon from a layer into single polygons.
    This function is usefull to split multipolygons which would make the execution of the function 'prop_coverage' extremely slow.

    Args:
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or using the custom function "create_PG_connexion".
        schema (str): Name of the schema where the table is stored.
        table (str): Name of the table containing multipolygons geometries that should be transformed to single polygon.
        geomcolumn (str): Name of the geometry column.

    Returns:
        This function has no return value. 
    """
    try:
        ## Saving current time for processing time management
        begintime_copy=time.time()
        # Get ID of current processus
        pid = os.getpid()
        # Create cursor
        cursor = con.cursor()
        # Update geom with ST_Makevalid function query
        query="DROP TABLE IF EXISTS public.tmp;"
        query+="CREATE TABLE public.tmp AS (SELECT {_id}, (St_dump({geom})).geom as {geom} ".format(_id=idcolumn, geom=geomcolumn)
        query+="FROM {schema}.{table});".format(schema=schema,table=table)
        query+="DROP TABLE IF EXISTS {schema}.{table};".format(schema=schema,table=table)
        query+="CREATE TABLE {schema}.{table} AS (SELECT * FROM public.tmp);".format(schema=schema,table=table)
        query+="DROP TABLE IF EXISTS public.tmp;"
        query+="CREATE INDEX {table}_{geom}_idx ON {schema}.{table} USING gist ({geom});".format(schema=schema,table=table,geom=geomcolumn)
        # Print the query
        print(query)
        # Execute the CREATE TABLE query
        cursor.execute(query)
        # Make the changes to the database persistent
        con.commit()
        # Close connection with database
        cursor.close()
        ## Compute processing time and print it
        print(print_processing_time(begintime_copy, "Process achieved in "))

    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)
