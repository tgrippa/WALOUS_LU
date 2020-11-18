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
import psycopg2
import time
from processing_time import print_processing_time
  
    
def add_column_postclass_rulenumber(con, result_table_schema, result_table_name):
    '''Function to add column for saving the postclassification rule number.
    
    Args: 
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or 
        using the custom function "create_PG_connexion" with database connexion parameters.
        result_table_schema (str): Name of the schema where the table is located.
        result_table_name (str): Name of the table on which the column should be created.
    
    Returns:
        This function has no return value. 
    '''
    try:
        # Time at starting
        begintime = time.time() 
        # Create cursor
        cursor = con.cursor()
        # Update table
        query="ALTER TABLE %s.%s ADD postclas_rule integer;"%(result_table_schema,result_table_name)
        print(query + '\n')
        cursor.execute(query)
        con.commit()
        # Close connection with database
        cursor.close()
        ## Print processing time
        print(print_processing_time(begintime, "Add column to the table in "))
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error) 
        
        
def postclassif_residentialgardens_1(con, result_table_schema, result_table_name, 
                                     postclassif_rule=1, colum_label="walousmaj"):
    '''Function to fix systematic misclassification of residential gardens.
    Postclassification of residential gardens is made in two steps. This function is the first step.
    The rule implement is as follows: all cadastral parcels in urban areas smaller than 2500 sq.m and classified 
    as '1_1' or '1_1_1' and not having all_hilucs items like '1_1_1_%' are reclassified as '5_1'.
    
    Args: 
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or 
        using the custom function "create_PG_connexion" with database connexion parameters.
        result_table_schema (str): Name of the schema where the table is located.
        result_table_name (str): Name of the table on which the column should be created.
        postclassif_rule (int): The number of the rule in the postclassification process.
        colum_label (str): The name of the attribute containing the classification results. Default value is "walousmaj".
        
    Returns:
        This function has no return value. 
    '''
    try:
        # Time at starting
        begintime = time.time() 
        # Create cursor
        cursor = con.cursor()
        # Update table
        query="UPDATE %s.%s "%(result_table_schema,result_table_name)
        query+="SET %s = '5_1', postclas_rule = %s "%(colum_label,postclassif_rule)
        query+="WHERE capakey IN (SELECT DISTINCT a.capakey FROM %s.%s AS a "%(result_table_schema,result_table_name)
        query+="JOIN (SELECT geom FROM %s.%s WHERE %s = '5_1') AS b "%(result_table_schema,result_table_name,colum_label)
        query+="ON ST_Touches(a.geom, b.geom) "
        query+="WHERE a.%s IN ('1_1','1_1_1') "%colum_label
        query+="AND ST_Area(a.geom) < 2500 AND a.rnpp_200m_mode >= 2 "
        query+="AND NOT EXISTS (SELECT 1 FROM unnest(a.all_hilucs) AS c WHERE c LIKE '1_1_1_%'))"
        print(query + ';\n')
        cursor.execute(query)
        con.commit()
        # Close connection with database
        cursor.close()
        ## Print processing time
        print(print_processing_time(begintime, "Reclassification of residential garden (step 1/2) achieved in "))
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error) 
        
        
def postclassif_residentialgardens_2(con, result_table_schema, result_table_name, 
                                     postclassif_rule=2, colum_label="walousmaj"):
    '''Function to fix systematic misclassification of residential gardens.
    Postclassification of residential gardens is made in two steps. This function is the second and last step.
    The rule implement is as follows: all cadastral parcels in urban areas, classified as '1_1' and having only
    one source of information available coming from the cadastral nature, are reclassified as '5_1' if they
    touch another parcel classified as residential.
    
    Args: 
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or 
        using the custom function "create_PG_connexion" with database connexion parameters.
        result_table_schema (str): Name of the schema where the table is located.
        result_table_name (str): Name of the table on which the column should be created.
        postclassif_rule (int): The number of the rule in the postclassification process.
        colum_label (str): The name of the attribute containing the classification results. Default value is "walousmaj".
        
    Returns:
        This function has no return value. 
    ''' 
    try:
        # Time at starting
        begintime = time.time() 
        # Create cursor
        cursor = con.cursor()
        # Update table
        query="UPDATE %s.%s "%(result_table_schema,result_table_name)
        query+="SET %s = '5_1', postclas_rule = %s "%(colum_label,postclassif_rule)
        query+="WHERE capakey IN (SELECT DISTINCT a.capakey FROM %s.%s AS a "%(result_table_schema,result_table_name)
        query+="JOIN (SELECT geom FROM %s.%s WHERE %s = '5_1') AS b "%(result_table_schema,result_table_name,colum_label)
        query+="ON ST_Touches(a.geom, b.geom) "
        query+="WHERE Cardinality(a.all_hilucs) = 1 "
        query+="AND a.nat_lu_maj = '1_1' AND a.%s = '1_1' "%colum_label
        query+="AND a.rnpp_200m_mode >= 2)"
        print(query + ';\n')
        cursor.execute(query)
        con.commit()
        # Close connection with database
        cursor.close()
        ## Print processing time
        print(print_processing_time(begintime, "Reclassification of residential garden (step 2/2) achieved in "))
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)    
        
def subdivide_residential_density(con, result_table_schema, result_table_name, colum_label="walousmaj"):
    '''Function to refine residential classes, by updating the classes '5_1' and '5_2' according to the 
    population density in their neighbourhood. 
    
    Args: 
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or 
        using the custom function "create_PG_connexion" with database connexion parameters.
        result_table_schema (str): Name of the schema where the table is located.
        result_table_name (str): Name of the table on which the column should be created.
        colum_label (str): The name of the attribute containing the classification results. Default value is "walousmaj".
        
    Returns:
        This function has no return value. 
    ''' 
    try:
        # Time at starting
        begintime = time.time() 
        # Create cursor
        cursor = con.cursor()
        # Case when then else end query
        case_query = "CASE WHEN %s = '5_1' THEN CASE "%colum_label
        case_query += "WHEN rnpp_200m_mode <= 1 THEN '5_1_D' "
        case_query += "WHEN rnpp_200m_mode = 2 THEN '5_1_C' "
        case_query += "WHEN rnpp_200m_mode = 3 THEN '5_1_B' "
        case_query += "WHEN rnpp_200m_mode = 4 THEN '5_1_A' "
        case_query += "ELSE '5_1' END "
        case_query += "WHEN %s = '5_2' THEN CASE "%colum_label
        case_query += "WHEN rnpp_200m_mode <= 1 THEN '5_2_D' "
        case_query += "WHEN rnpp_200m_mode = 2 THEN '5_2_C' "
        case_query += "WHEN rnpp_200m_mode = 3 THEN '5_2_B' "
        case_query += "WHEN rnpp_200m_mode = 4 THEN '5_2_A' "
        case_query += "ELSE '5_2' END "
        case_query += "ELSE %s END"%colum_label 

        # Update table - Land use class attribute
        query="UPDATE %s.%s "%(result_table_schema,result_table_name)
        query+="SET %s = (%s)"%(colum_label,case_query)
        print(query+";\n")
        cursor.execute(query)
        con.commit()
        
        # Close connection with database
        cursor.close()
        ## Print processing time
        print(print_processing_time(begintime, "Updating of residential classes in neighbourhood density classes achieved in "))
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)
    
def create_cusw_table(con, schema, input_table_name, cusw_table_name):
    '''Function to create table "cusw" that will be the one shared with end users. 
    
    Args: 
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or 
        using the custom function "create_PG_connexion" with database connexion parameters.
        schema (str): Name of the schema where the table with classification results is located.
        input_table_name (str): Name of the table with classification results.
        cusw_table_name (str): The name of the table to be created and dedicated to be shared with end-users.
        
    Returns:
        This function has no return value. 
    ''' 
    try:
        # Time at starting
        begintime = time.time() 
        # Create cursor
        cursor = con.cursor()
        # Drop table if exists
        query = 'DROP TABLE IF EXISTS %s.%s;'%(schema,cusw_table_name)
        print(query + "\n")
        cursor = con.cursor()
        cursor.execute(query)
        con.commit()
        cursor.close()
        # List of columns to be selected for cusw table
        selectcolumns = ['geom', 'capakey', 'lc_mode','lc_prop_1','lc_prop_2', 'lc_prop_3', 'lc_prop_4', 
                         'lc_prop_5', 'lc_prop_6', 'lc_prop_7', 'lc_prop_8', 'lc_prop_9', 'lc_prop_80', 
                         'lc_prop_90', 'all_hilucs', 'walousmaj', 'rulebased_leaf', 'postclas_rule']
        # Create table
        query = 'CREATE TABLE %s.%s AS(SELECT %s FROM %s.%s);'%(schema,cusw_table_name,
                                                                ','.join(selectcolumns),
                                                                schema,input_table_name)
        print(query + "\n")
        cursor = con.cursor()
        cursor.execute(query)
        con.commit()
        cursor.close()
        ## Print processing time
        print(print_processing_time(begintime, "Creation of table '%s' achieved in "%cusw_table_name))
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)
        
        
def create_walousmaj_levels(con, result_table_schema, result_table_name, colum_label="walousmaj"):
    '''Function to add new attribute columns with the classifiation results code at each level (from 
    level 1 to level 4). 
    
    Args: 
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or 
        using the custom function "create_PG_connexion" with database connexion parameters.
        result_table_schema (str): Name of the schema where the table is located.
        result_table_name (str): Name of the table on which the column should be created.
        colum_label (str): The name of the attribute containing the classification results. Default value is "walousmaj".
        
    Returns:
        This function has no return value. 
    ''' 
    try:
        # Time at starting
        begintime = time.time() 
        # Create cursor
        cursor = con.cursor()
        # Add columns
        queries = []
        queries.append("ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s_l1 character varying"%(result_table_schema,result_table_name,colum_label))
        queries.append("ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s_l2 character varying"%(result_table_schema,result_table_name,colum_label))
        queries.append("ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s_l3 character varying"%(result_table_schema,result_table_name,colum_label))
        queries.append("ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s_l4 character varying"%(result_table_schema,result_table_name,colum_label))
        print(';\n'.join(queries)+';\n')
        cursor.execute(';\n'.join(queries))
        con.commit()

        # Update table
        update = "UPDATE {s}.{t} SET {p}_l1 = LEFT({p},1);"
        query = update.format(s=result_table_schema,t=result_table_name,p=colum_label)
        print(query)
        cursor.execute(query)
        con.commit()
        update = "UPDATE {s}.{t} SET {p}_l2 = LEFT({p},3) WHERE LENGTH({p}) >= 3;"
        query = update.format(s=result_table_schema,t=result_table_name,p=colum_label)
        print(query)
        cursor.execute(query)
        con.commit()
        update = "UPDATE {s}.{t} SET {p}_l3 = LEFT({p},5) WHERE LENGTH({p}) >= 5;"
        query = update.format(s=result_table_schema,t=result_table_name,p=colum_label)
        print(query)
        cursor.execute(query)
        con.commit()
        update = "UPDATE {s}.{t} SET {p}_l4 = LEFT({p},7) WHERE LENGTH({p}) >= 7;"
        query = update.format(s=result_table_schema,t=result_table_name,p=colum_label)
        print(query)
        cursor.execute(query)
        con.commit()
        
        # Close connection with database
        cursor.close()
        ## Print processing time
        print(print_processing_time(begintime, "Creation of columns for 'walousmaj' for different levels achieved in "))
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)

        
def create_hilucs_landuse_1(con, result_table_schema, result_table_name, 
                            cl_truncate, cl_lookup, colum_label="hilucslanduse_1"):
    '''Function to add a new attribute column with the classification compliant with scenario 1 of
    INSPIRE HILUCS data specification. This function takes as input some lists in parameters that allow ensuring the
    newly created attribute is compliant with INSPIRE HILUCS scheme.
    
    Args: 
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or 
        using the custom function "create_PG_connexion" with database connexion parameters.
        result_table_schema (str): Name of the schema where the table is located.
        result_table_name (str): Name of the table on which the column should be created.
        cl_truncate (list of str): Codes of the actual legend scheme that should be truncated of one level scheme to be 
        compliant with the INSPIRE HILUCS scheme. 
        cl_lookup (list of tuple of str): Correspondance between codes of the actual legend scheme and their 
        corresponding code in the INSPIRE HILUCS scheme.
        colum_label (str): The name of the attribute to be created. Default value is "hilucslanduse_1".
        
    Returns:
        This function has no return value. 
    ''' 
    try:
        # Time at starting
        begintime = time.time() 
        # Create cursor
        cursor = con.cursor()    
        ##### HilucsLandUse #####
        ## This column will contain only the majority class contain in "walousmaj" but in its in the Hilucs legend, while 
        ## Classes without an existing higher level hilucs class will be changed, e.g. 5_1_A or 1_1_1_A 
        #########################
        # Add column or replace it if exists
        query = 'ALTER TABLE %s.%s DROP COLUMN IF EXISTS %s'%(result_table_schema,result_table_name,colum_label)
        print(query+";\n")
        cursor.execute(query)
        query = 'ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s text'%(result_table_schema,result_table_name,colum_label)
        print(query+";\n")
        cursor.execute(query)
        con.commit()
        # Case when then else end query
        case_query = "CASE "
        if cl_truncate:
            for cl in cl_truncate: # Classes that need to be cuted from one level of detail off, e.g. class 1_1_1_A -> 1_1_1
                case_query += "WHEN walousmaj = '%s' THEN '%s' "%(cl,'_'.join(cl.split('_')[:-1]))
        if cl_lookup:
            for cl_walousmaj, cl_inspire in cl_lookup: # Classes that need to be converted, e.g. 7_1 -> 6_3_1
                case_query += "WHEN walousmaj = '%s' THEN '%s' "%(cl_walousmaj,cl_inspire)
        case_query += "ELSE walousmaj END "
        # Update HilucsLandUse with only classes that exist in the Hilucs legend
        query = "UPDATE %s.%s "%(result_table_schema,result_table_name)
        query += "SET %s = (%s) "%(colum_label,case_query)
        print(query+";\n")
        cursor.execute(query)
        con.commit()
        # Close connection with database
        cursor.close()
        ## Print processing time
        print(print_processing_time(begintime, "Creation of INSPIRE compliant 'HilucsLanduse' column achieved in "))
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)

        
def create_hilucs_landuse_2(con, result_table_schema, result_table_name, cl_ignore, cl_truncate, 
                            cl_lookup, cl_remove, colum_label="hilucslanduse_2"):
    '''Function to add a new attribute column with the classification compliant with scenario 2 of
    INSPIRE HILUCS data specification. This function takes as input some lists in parameters that allow ensuring the
    newly created attribute is compliant with INSPIRE HILUCS scheme.
    The trick for the distinct value in the array was found here: https://stackoverflow.com/a/57813770/8013239
    The trick for pattern/wildcard searching in array content was found here: https://stackoverflow.com/a/55480601/8013239 
    
    Args: 
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or 
        using the custom function "create_PG_connexion" with database connexion parameters.
        result_table_schema (str): Name of the schema where the table is located.
        result_table_name (str): Name of the table on which the column should be created.
        cl_ignore (list of str): Codes of the actual legend scheme that should be ignored when creating the 
        INSPIRE HILUCS compliant attribute. 
        cl_truncate (list of str): Codes of the actual legend scheme that should be truncated of one level scheme to be 
        compliant with the INSPIRE HILUCS scheme. 
        cl_lookup (list of tuple of str): Correspondance between codes of the actual legend scheme and their 
        corresponding code in the INSPIRE HILUCS scheme.
        cl_remove (list of str): Codes of the actual legend scheme that should not be present in the newly created
        attribute, if another more detailed sub-level class is present in the all_hilucs attribute.
        colum_label (str): The name of the attribute to be created. Default value is "hilucslanduse_2".
        
    Returns:
        This function has no return value. 
    ''' 
    try:
        # Time at starting
        begintime = time.time() 
        # Create cursor
        cursor = con.cursor()
        
        ##### tmp_walousmaj_allhilucs #####
        ## This column is temporary and used in the creation of the column HilucsLandUse
        ########################
        # Add column or replace it if exists
        query = 'ALTER TABLE %s.%s DROP COLUMN IF EXISTS tmp_walousmaj_allhilucs'%(result_table_schema,result_table_name)
        print(query+";\n")
        cursor.execute(query)
        query = 'ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS tmp_walousmaj_allhilucs text[]'%(result_table_schema,result_table_name)
        print(query+";\n")
        cursor.execute(query)
        con.commit()
        # Update column
        query = "UPDATE %s.%s "%(result_table_schema,result_table_name)
        query += "SET tmp_walousmaj_allhilucs = (array_prepend(walousmaj::text,all_hilucs))"
        print(query+";\n")
        cursor.execute(query)
        con.commit()
        
        ##### HilucsLandUse #####
        ## This column will contain only classes that exist in the Hilucs legend, while 
        # preserving the order of the original array 'tmp_column_walousmaj_allhilucs'
        ## Classes without any correspondance such as '8_8' will be removed
        ## Classes without an existing higher level hilucs class will be changed, e.g. 5_1_A or 1_1_1_A
        ## Classes only existing in walousmaj but having a correspondance in INSPIRE HILUCS will be converted such as 7_1 -> 6_3_1
        ## In case of coexistence of redundant classes such as '1_1' and '1_1_1', the higher level class will be removed 
        #########################
        # Add column or replace it if exists
        query = 'ALTER TABLE %s.%s DROP COLUMN IF EXISTS %s'%(result_table_schema,result_table_name,colum_label)
        print(query+";\n")
        cursor.execute(query)
        query = 'ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s text[]'%(result_table_schema,result_table_name,colum_label)
        print(query+";\n")
        cursor.execute(query)
        con.commit()
        # Case when then else end query
        case_query = "ARRAY (SELECT CASE "
        if cl_truncate:
            for cl in cl_truncate: # Classes that need to be cuted from one level of detail off, e.g. class 1_1_1_A become 1_1_1
                case_query += "WHEN v = '%s' THEN '%s' "%(cl,'_'.join(cl.split('_')[:-1]))
        if cl_lookup:
            for cl_walousmaj, cl_inspire in cl_lookup: # Classes that need to be converted, e.g. 7_1 -> 6_3_1
                case_query += "WHEN v = '%s' THEN '%s' "%(cl_walousmaj,cl_inspire)
        case_query += "ELSE v END "
        case_query += "FROM unnest(tmp_walousmaj_allhilucs) WITH ORDINALITY t(v,ord) "
        if cl_ignore: 
            case_query += "WHERE v NOT IN ('%s') "%"','".join(cl_ignore)
        case_query += "GROUP BY 1 ORDER BY min(ord))"
        # Update HilucsLandUse with array containing only classes that exist in the Hilucs legend
        query = "UPDATE %s.%s "%(result_table_schema,result_table_name)
        query += "SET %s = (%s) "%(colum_label,case_query)
        print(query+";\n")
        cursor.execute(query)
        con.commit()
        # Update HilucsLandUse array to remove classes when sublevels more precise classes exists in the array.
        subquery = "array_remove({col},'{cl}') WHERE '{cl}' = ANY({col}) AND EXISTS (SELECT 1 FROM unnest({col}) AS a WHERE a LIKE '{cl}_%')"
        for cl in cl_remove:
            query = "UPDATE %s.%s "%(result_table_schema,result_table_name)
            query += "SET %s = %s "%(colum_label,subquery.format(col=colum_label, cl=cl))
            print(query+";\n")
            cursor.execute(query)
        con.commit()
        ###### tmp_column_walousmaj_allhilucs     
        # Remove column not needed anymore
        query = 'ALTER TABLE %s.%s DROP COLUMN IF EXISTS tmp_walousmaj_allhilucs'%(result_table_schema,result_table_name)
        print(query+";\n")
        cursor.execute(query)
        con.commit()
        # Close connection with database
        cursor.close()
        ## Print processing time
        print(print_processing_time(begintime, "Creation of INSPIRE compliant 'HilucsLanduse' column achieved in "))
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)
    
        
def create_cuswall_table(con, result_schema, result_table, uncad_schema, uncad_table, output_table='cusw2018_all'):
    '''Function to create a new table containing cadastred and uncadastred spaces together. 
    
    Args: 
        con (psycopg2 connection object): Psycopg connection object generated using psycopg2.connect() or 
        using the custom function "create_PG_connexion" with database connexion parameters.
        result_schema (str): Name of the schema where the 'cusw' table is located.
        result_table (str): Name of the 'cusw' table.
        uncad_schema (str): Name of the schema where the table with uncadastred geometries is located.
        uncad_table (str): Name of the table with uncadastred geometries.
        output_table (str): Name of the table with uncadastred geometries. Default value is 'cusw2018_all'.
        
    Returns:
        This function has no return value. 
    ''' 
    try:
        # Time at starting
        begintime = time.time() 
        # Create cursor
        cursor = con.cursor()
        # Create new table as a copy of result table
        queries = []
        queries.append("DROP TABLE IF EXISTS %s.%s"%(result_schema,output_table))
        queries.append("CREATE TABLE IF NOT EXISTS %s.%s AS (SELECT * FROM %s.%s)"%(result_schema,output_table,result_schema,result_table))
        print(';\n'.join(queries)+';\n')
        cursor.execute(';\n'.join(queries))
        con.commit()
        # Add column to store uncadastred geometries ID and INSERT query
        queries = []
        queries.append("ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS uncadastr_id integer"%(result_schema,output_table))
        queries.append("INSERT INTO %s.%s(geom,lc_mode,lc_prop_1,lc_prop_2,lc_prop_3,lc_prop_4,lc_prop_5,lc_prop_6,lc_prop_7,lc_prop_8,lc_prop_9,lc_prop_80,lc_prop_90,walousmaj,hilucslanduse_1,hilucslanduse_2,walousmaj_l1,walousmaj_l2,walousmaj_l3,walousmaj_l4,uncadastr_id) SELECT ST_Multi(ST_CollectionExtract(geom,3)) \
        as geom,lc_mode,lc_prop_1,lc_prop_2,lc_prop_3,lc_prop_4,lc_prop_5,lc_prop_6,lc_prop_7,lc_prop_8,lc_prop_9,lc_prop_80,lc_prop_90,walousmaj,hilucslanduse_1,hilucslanduse_2,walousmaj_l1,walousmaj_l2,walousmaj_l3,walousmaj_l4, uncadastr_id FROM %s.%s"%(result_schema,output_table,uncad_schema,uncad_table))
        print(';\n'.join(queries)+';\n')
        cursor.execute(';\n'.join(queries))
        con.commit()
        # Close connection with database
        cursor.close()
        ## Print processing time
        print(print_processing_time(begintime, "Creation of table '%s.%s' combining all geometries \
        (cadastred and uncadastred ares) achieved in "%(result_schema,output_table)))
    except (Exception, psycopg2.DatabaseError) as error:
        sys.exit(error)
        
        