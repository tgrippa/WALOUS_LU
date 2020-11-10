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

import string
import random

def random_layer_name(prefix='tmp', length=6, chars=string.ascii_uppercase + string.digits):
    '''Function that return a random name for a temporary layer. 
    
    Args:
    prefix (str): The prefix of the string to be created. Default value is 'tmp'.
    length (int): The length of the random random part of the string to be created. Default value is 6.
    chars (str): The chain of character on wich to randomly pick elements. Default value 
    is string.ascii_uppercase + string.digits which correspond to all ASCII character in uppercase and all digits. 
    
    Returns:
        str value with the random string. 
    '''    
    return prefix + ''.join(random.choice(chars) for _ in range(length))


