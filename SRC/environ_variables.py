#!/usr/bin/env python

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