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

import os
import grass.script as gscript
import grass.script.setup as gsetup
import shutil ## Import library for file copying 
from __main__ import *

def check_gisdb(gisdb_path):
    '''Function to check if a GRASS GIS 'GRASSDATA' folder exists and to create it if not exists.
    
    Args:
       gisdb_path (str): Path to the GRASS GIS 'GRASSDATA' folder.
    
    Returns:
       str: A message in string format. 
    '''
    ## Automatic creation of GRASSDATA folder
    if os.path.exists(gisdb_path):
        return "GRASSDATA folder already exist" 
    else: 
        os.makedirs(gisdb_path) 
        return "GRASSDATA folder created in '%s'"%gisdb_path
        
def check_location(gisdb_path,location_name,epsg):
    '''Function to check if a GRASS GIS location exists and to create it if not exists.
    
    Args:
       gisdb_path (str): Path to the GRASS GIS 'GRASSDATA' folder.
       location_name (str): Name of the GRASS GIS location.
       epsg (str): The EPSG number corresponding to the Coordinate Reference System (CRS) of the GRASS GIS location.

    Returns:
       str: A message in string format. 
    '''
    ## Automatic creation of GRASS location is doesn't exist
    if os.path.exists(os.path.join(gisdb_path,location_name)):
        return "Location '%s' already exist"%location_name
    else : 
        gscript.core.create_location(gisdb_path, location_name, epsg=epsg, overwrite=False)
        return "Location '%s' created"%location_name
    
def check_mapset(gisdb_path,location_name,mapset_name):
    '''Function to check if a GRASS GIS mapset exists and to create it if not exists.
    
    Args:
       gisdb_path (str): Path to the GRASS GIS 'GRASSDATA' folder.
       location_name (str): Name of the GRASS GIS location.
       mapset_name (str): Name of the GRASS GIS mapset.

    Returns:
       str: A message in string format. 
    '''
    ### Automatic creation of GRASS GIS mapsets
    if os.path.exists(os.path.join(gisdb_path,location_name,"PERMANENT")):  # Check if PERMANENT mapset exists because it is needed
        if not os.path.exists(os.path.join(gisdb_path,location_name,"PERMANENT",'WIND')): # Check if PERMANENT mapset exists because it is needed
            return "WARNING: 'PERMANENT' mapset already exist, but a 'WIND' file is missing. Please solve this issue."
        else: 
            if not os.path.exists(os.path.join(gisdb_path,location_name,mapset_name)):
                os.makedirs(os.path.join(gisdb_path,location_name,mapset_name))
                shutil.copy(os.path.join(gisdb_path,location_name,'PERMANENT','WIND'),os.path.join(gisdb_path,location_name,mapset_name,'WIND'))
                return "'%s' mapset created in location '%s'"%(mapset_name,location_name)
            else:
                return "'%s' mapset already exists in location '%s'"%(mapset_name,location_name)
    else:
        return "WARNING: 'PERMANENT' mapset do not exist. Please solve this issue."

def working_mapset(gisdb_path,location_name,mapset_name):
    '''Function to launch a GRASS GIS working session in a specific mapset.
    
    Args:
       gisdb_path (str): Path to the GRASS GIS 'GRASSDATA' folder.
       location_name (str): Name of the GRASS GIS location.
       mapset_name (str): Name of the GRASS GIS mapset.

    Returns:
       str: A message in string format. 
    '''
    ## Launch GRASS GIS working session in the mapset
    if os.path.exists(os.path.join(gisdb_path,location_name,mapset_name)):
        gsetup.init(os.environ['GISBASE'], gisdb_path,location_name,mapset_name)
        return "You are now working in mapset '%s/%s'"%(location_name,mapset_name)
    else: 
        return "'%s' mapset doesn't exists in '%s'"%(mapset_name,gisdb_path)
        
def launch_mapset(mapset):
    '''Function to launch a GRASS GIS working session in a specific mapset and handle creation of this mapset even if it does not existis initially.
    
    Args:
       mapset_name (str): name of the GRASS GIS mapset.

    Returns:
       str: A message in string format. 
    '''
    #Declare empty list that will contain the messages to return
    return_message = []
    # Check if the location exists and create it if not, with the CRS defined by the epsg code 
    return_message.append(check_location(config_parameters["gisdb"],config_parameters['location'],config_parameters["locationepsg"]))
    # Check if mapset exists
    return_message.append(check_mapset(config_parameters["gisdb"],config_parameters['location'],mapset))
    # Change the current working GRASS GIS session mapset
    return_message.append(working_mapset(config_parameters["gisdb"],config_parameters['location'],mapset))
    # Return
    return return_message