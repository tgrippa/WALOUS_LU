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

#from __main__ import *
import sys
import psycopg2
import time
from processing_time import start_processing, print_processing_time
   
def decision_tree_classification(con, result_table_schema, result_table_name, 
                               stats_table_schema, stats_table_name, 
                               list_rules, colum_label="walousmaj", colum_leaf="rulebased_leaf", grant_user=None):
    '''Function for creation of table with the 'walousmaj' column resulting from a rule-based decision-tree classification.
    This function handle the automated creation of the rule-based query to be used to define the value of 'walousmaj'.

    Args: 
		con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or using the custom function "create_PG_connexion" with database connexion parameter.
		result_table_schema (str): Name of the schema on which the new table with classification results will be created.
		result_table_name (str): Name of the table with classification results to be created.
		stats_table_schema (str): Name of the schema where to find the table containing all statistics used in the classification rules.
		stats_table_name (str): Name of the table containing all statistics used in the classification rules.
		list_rules (list of tupple): List of tuples containing the classification rules.  
		The tuples in the list should be ordrered from the first rule to the last one (hierarchical decision tree). 
		Tuples should be made of two elements. The first element is the 'where' statement of the query and the second element is the label to assign (see example section). 
		colum_label (str): Name of the column that will contain the label resulting from the rule-based classification. Default value is 'walousmaj'. 
		colum_leaf (str): Name of the column that will contain the number of the rule that is used to assign a label in the rule-based classification. Default value is 'rulebased_leaf'.
		grant_user (list of str): List of users who should be granted complete privileges (ALL) on the newly created table. Default value is None and no other user than the current user is granted privileges.

    Returns:
		This function has no return value. 
	
	Example: 
		DecisionTreeClassification(create_PG_connexion(config_parameters), result_table_schema='results', 
		result_table_name=classif_table, stats_table_schema='results', stats_table_name='capa_statistics_wall_a',
		list_rules=[("recypark_count is NOT NULL","'4_3_3'"),("aeroport_coverage > 0.5","'4_1_3'")],
		colum_label="walousmaj", colum_leaf="rulebased_leaf",grant_user=['tais','bbeaumont'])
    '''
    try:
        # Time at starting
        begintime = time.time() 
        # Drop table if exists
        query = 'DROP TABLE IF EXISTS %s.%s;'%(result_table_schema,result_table_name)
        print(query + "\n")
        cursor = con.cursor()
        cursor.execute(query)
        cursor.close()
        con.commit()
        # Create table
        query = 'CREATE TABLE %s.%s AS(SELECT * FROM %s.%s);'%(result_table_schema,result_table_name,stats_table_schema,stats_table_name)
        print(query + "\n")
        cursor = con.cursor()
        cursor.execute(query)
        cursor.close()
        con.commit()
        # Add columns for classification label and rule number
        queries = []
        queries.append('ALTER TABLE %s.%s ADD %s varchar;'%(result_table_schema,result_table_name,colum_label))
        queries.append('ALTER TABLE %s.%s ADD %s integer;'%(result_table_schema,result_table_name,colum_leaf))
        print('\n\n'.join(queries))
        cursor = con.cursor()
        cursor.execute(' '.join(queries))
        cursor.close()
        con.commit()
        # Grant user(s) on the new table
        if grant_user:
            for user in grant_user:
                query = 'GRANT ALL PRIVILEGES ON %s.%s TO %s;'%(result_table_schema,result_table_name,user)
                print(query + "\n")
                cursor = con.cursor()
                cursor.execute(query)
                cursor.close()
                con.commit()
        # Update columns using decision tree hierarchical classification - LABEL column  
        query = 'UPDATE %s.%s SET %s = (CASE '%(result_table_schema,result_table_name,colum_label)
        for when,then in list_rules:
            query += 'WHEN %s THEN %s '%(when,then)
        query += "ELSE '6_6_A' END);"
        print(query + "\n")
        cursor = con.cursor()
        cursor.execute(query)
        cursor.close()
        con.commit()
        # Update columns using decision tree hierarchical classification - RULE NUMBER column  
        query = 'UPDATE %s.%s SET %s = (CASE '%(result_table_schema,result_table_name,colum_leaf)
        for i,rule in enumerate(list_rules,1):
            query += 'WHEN %s THEN %s '%(rule[0],i)
        query += "ELSE %s END);"%int(len(list_rules)+1)
        print(query + "\n")
        cursor = con.cursor()
        cursor.execute(query)
        cursor.close()
        con.commit()
        # Close connection with database
        cursor.close()
        ## Print processing time
        print(print_processing_time(begintime, "Classification and creation of result table achieved in "))
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)
        
