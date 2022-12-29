'''
Created on Dec 23, 2022

@author: leo
'''
from pprint import pprint

from .config import VERB

def trace(msg, msg_level=1, pretty=False, verb_level=VERB):
    if msg_level <= verb_level: 
        if pretty:
            pprint(msg)
        else:
            print(msg)

