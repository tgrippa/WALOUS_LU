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

import os
import grass.script as gscript
import grass.script.setup as gsetup

def rzonalclasses_sql_insert(table_name, header, value_dict, overwrite=True):
    """Function to import the csv resulting from the execution of r.zonal.classes as a table in a GRASSDATA (GRASS GIS) 

    Args:
        table_name (str): Name of the table to be created in GRASS GIS".
        header (list of str): List with the name of columns corresponding to the first row of the csv.
        value_dict (dict of list of str): Dictionary with the cat of the polygon as key and the values of the rest of the columns in a list of str.
        overwrite (bool): Option to overwrite or not the existing table.

    Returns:
        This function has no return value. 
    """
    sql_query = gscript.tempfile()
    fsql = open(sql_query, 'w')
    fsql.write('BEGIN TRANSACTION;\n')
    if gscript.db_table_exist(table_name):
        if overwrite:
            fsql.write('DROP TABLE %s;\n' % table_name)
        else:
            gscript.fatal(_("Table %s already exists. Use 'overwrite=True' to overwrite" % table_name))
    create_statement = 'CREATE TABLE ' + table_name + ' (cat int PRIMARY KEY);\n'
    fsql.write(create_statement)
    for col in header[1:]:
        if col.split('_')[-1] == 'mode':  # lc_mode column should be integer
            addcol_statement = 'ALTER TABLE %s ADD COLUMN %s integer;\n' % (table_name, col)
        else: # Proportions columns should be double precision
            addcol_statement = 'ALTER TABLE %s ADD COLUMN %s double precision;\n' % (table_name, col)
        fsql.write(addcol_statement)
    for key in value_dict:
            insert_statement = 'INSERT INTO %s VALUES (%s, %s);\n' % (table_name, key, ','.join([str(x) for x in value_dict[key]]))
            fsql.write(insert_statement)
    fsql.write('END TRANSACTION;')
    fsql.close()
    gscript.run_command('db.execute', input=sql_query, quiet=True)