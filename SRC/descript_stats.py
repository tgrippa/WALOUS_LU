#!/usr/bin/env python

import os 
import sys
import psycopg2
import time
from postgres_functions import create_pg_connexion

def get_count_area(config_parameters, schema, table):
    """Function to get values of total number and total area 
    
    Args:
        connexion_param_dict (dict): A dictionnary containing informations for connection to the database. The dictionnary should have the following elements:
        'pg_host' with the server host, 'pg_port' with the server connexion port, 'pg_user' with the name of the user, 'pg_password' with the password of user, 'pg_dbname' with the name of the database.

    Returns:
        A psycopg2 connexion object.
    """
    # DB connexion
    con = create_pg_connexion(config_parameters) # Create connexion
    cursor = con.cursor() # Create cursor
    # SQL query
    query = "SELECT count(*) FROM {0}.{1}"
    cursor.execute(query.format(schema,table))
    total = int(cursor.fetchone()[0]) # fetch the first row
    # SQL query
    query = "SELECT sum(ST_area(geom))/1000000 FROM {0}.{1}"   # Area in squared kilometers
    cursor.execute(query.format(schema,table))
    total_area = float(cursor.fetchone()[0]) # fetch the first row
    # Close connection with database
    cursor.close()
    # Close connexion to postgres database
    con.close()
    # Close connection with database
    cursor.close()
    # Close connexion to postgres database
    con.close()
    # Print information
    print("Total number of records: {0} (100%)".format(total))
    print("Total area: {0} sq. kilometer (100%)".format("{:.2f}".format(total_area)))
    # Returns
    return total, total_area


def descript_stats_proportion(config_parameters, schema, table, total, total_area, where=""):
    """Function to compute proportion (in count and area) of records that correspond to the where condition  
    
    Args:
        connexion_param_dict (dict): A dictionnary containing informations for connection to the database. The dictionnary should have the following elements:
        'pg_host' with the server host, 'pg_port' with the server connexion port, 'pg_user' with the name of the user, 'pg_password' with the password of user, 'pg_dbname' with the name of the database.

    Returns:
        A psycopg2 connexion object.
    """
    ## Queries
    con = create_pg_connexion(config_parameters) # Create connexion
    cursor = con.cursor() # Create cursor
    # Count
    query = "SELECT count(*) FROM {0}.{1} {2}"
    cursor.execute(query.format(schema,table,where))
    count = int(cursor.fetchone()[0]) # fetch the first row
    # Sum area
    query = "SELECT sum(ST_area(geom))/1000000 FROM {0}.{1} {2}"   # Area in squared kilometers
    cursor.execute(query.format(schema,table,where))
    area = float(cursor.fetchone()[0]) # fetch the first row
    # Close connection with database
    cursor.close()
    # Close connexion to postgres database
    con.close()
    # Print information
    prop_count = (count*1.0/total*1.0)
    prop_area = (area*1.0/total_area*1.0)
    print("Count of records: %s (%s)"%(count,"{:.3%}".format(prop_count)))
    print("Sum of area: %s sq. meter (%s)"%("{:.3f}".format(area),"{:.3%}".format(prop_area)))