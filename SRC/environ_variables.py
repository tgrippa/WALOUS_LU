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

import os,sys
from __main__ import config_parameters

def setup_environmental_variables():
    '''Function for setting the environment variables for GRASS GIS and PostgreSQL.
    
    Returns:
        This function has no return value. 
        
    Reference:
        Documentation is available here: https://grass.osgeo.org/grass64/manuals/variables.html.
    ''' 
    # Check is environmental variables exists and create them if not exists (declare as empty).
    if not 'PYTHONPATH' in os.environ:
        os.environ['PYTHONPATH']=''
    if not 'LD_LIBRARY_PATH' in os.environ:
        os.environ['LD_LIBRARY_PATH']=''
        
    # Set environmental variables
    os.environ['GISBASE'] = config_parameters['GISBASE']
    os.environ['PATH'] += os.pathsep + os.path.join(os.environ['GISBASE'],'bin')
    os.environ['PATH'] += os.pathsep + os.path.join(os.environ['GISBASE'],'script')
    os.environ['PATH'] += os.pathsep + os.path.join(os.environ['GISBASE'],'lib')
    os.environ['PYTHONPATH'] += os.pathsep + os.path.join(os.environ['GISBASE'],'etc','python')
    os.environ['PYTHONPATH'] += os.pathsep + os.path.join(os.environ['GISBASE'],'etc','python','grass')
    os.environ['PYTHONPATH'] += os.pathsep + os.path.join(os.environ['GISBASE'],'etc','python','grass','script')
    os.environ['PYTHONLIB'] = config_parameters['PYTHONLIB']
    os.environ['LD_LIBRARY_PATH'] += os.pathsep + os.path.join(os.environ['GISBASE'],'lib')
    os.environ['GIS_LOCK'] = '$$'
    os.environ['GISRC'] = os.path.join(os.environ['HOME'],'.grass7','rc')  ## Guess will only works for LINUX

    ## Define GRASS-Python environment
    sys.path.append(os.path.join(os.environ['GISBASE'],'etc','python'))
    
    # Set environmental variables for Postgresql
    os.environ['PGHOST'] = config_parameters['pg_host']
    os.environ['PGPORT'] = config_parameters['pg_port']
    os.environ['PGUSER'] = config_parameters['pg_user']
    os.environ['PGPASSWORD'] = config_parameters['pg_password']
    os.environ['PGDATABASE'] = config_parameters['pg_dbname']

def print_environmental_variables():
    '''Function that print the current environmental variables of your computer.
    
    Returns:
        This function has no return value. 
    '''
    ## Display the current defined environment variables
    for key in os.environ.keys():
        print("%s = %s \t" % (key,os.environ[key]))