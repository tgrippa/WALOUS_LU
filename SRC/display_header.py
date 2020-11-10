#!/usr/bin/env python
"""
WALOUS_UTS - Copyright (C) <2020> <Université catholique de Louvain (UCLouvain), Belgique
					 Université Libre de Bruxelles (ULB), Belgique
					 Institut Scientifique de Service Public (ISSeP), Belgique
					 Service Public de Wallonie (SWP), Belgique >
						 							
	
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
