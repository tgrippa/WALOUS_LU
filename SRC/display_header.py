#!/usr/bin/env python

#from __main__ import *
import sys
import pandas as pd
pd.set_option('display.max_columns', 100)

def display_header(con, schema, table, where=None, order_by=None, row_num=5):
    '''Function that display header of a postgresql table using Pandas library.
    
    Args: 
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or using the custom function "create_PG_connexion" with database connexion parameter.
        schema (str): Name of the schema where the table is located in the postgresql database.
        table (str): Name of the table to be displayed.
        where (str): The 'where' statement of the SQL query (without the 'WHERE' sql keyword). Default:None.
        order_by (str): Name of the column to use for ORDER BY statement of the SQL query (ascending). Default:None.
        row_num (int): The number of row to be displayed.
        
    Returns:
        df (pandas.DataFrame object): The dataframe to be displayed.
    '''
    try:
        # Select on the X first rows of the table
        query = "SELECT * FROM %s.%s "%(schema, table)
        if where:
            query += "WHERE %s "%where
        if order_by:
            query += "ORDER BY %s "%order_by
        query += "LIMIT %s"%row_num
        # Execute query through panda
        df=pd.read_sql(query, con)
        # Return dataframe
        return df
    except:
        sys.exit("ERROR: An error occured when displaying the header of the table. Please check.")
