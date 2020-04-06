import os
import sys
'''
import inspect
_cf = inspect.currentframe()
_cf = inspect.getfile(_cf)
_cf = os.path.abspath(_cf)
_cf = os.path.dirname(_cf)
'''
def add_context(arg=None) :
    if arg is None :
       arg = os.path.dirname(__file__)
       arg = os.path.join(arg, '../bin')
       arg = os.path.abspath(arg)
    sys.path.insert(0, arg)

add_context()
