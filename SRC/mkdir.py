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

import os

def check_create_dir(path):
	'''Function that check if a directory exists and create it if not exists.
    
    Args:
	   path (str): Path to the folder.
    
	Returns:
       This function has no returns. 
    '''
	if os.path.exists(path):
		print("The folder '%s' already exists"%path)
	else: 
		os.makedirs(path) 
		print("The folder '%s' has been created"%path)
		
