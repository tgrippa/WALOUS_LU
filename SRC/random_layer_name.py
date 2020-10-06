#!/usr/bin/env python

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


