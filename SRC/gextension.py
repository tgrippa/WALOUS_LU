#!/usr/bin/env python

import grass.script as gscript

def check_install_addon(addon):
    '''Function for checking if a GRASS GIS add-on is installed and to install it if not.
    
    Args:
       addon (str): Name of the GRASS GIS add-on.
    
    Returns:
       This function has no returns. 
    '''
    if addon not in gscript.parse_command('g.extension', flags="a"):
        gscript.run_command('g.extension', extension="%s"%addon)
        print("%s have been installed on your computer"%addon)
    else: print("%s is already installed on your computer"%addon)
