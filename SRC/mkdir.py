#!/usr/bin/env python

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
		
