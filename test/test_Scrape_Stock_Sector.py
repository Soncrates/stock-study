#!/usr/bin/python

import logging
import sys
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import context

from libDecorators import log_on_exception, singleton
from libDebug import trace
import cmd_Scrape_Stock_Sector as TEST

def get_globals(*largs) :
    ret = {}
    for name in largs :
        value = globals().get(name,None)
        if value is None :
           continue
        ret[name] = value
    return ret

@singleton
class T() :
    var_names = ['ini_files','file_list','DMYT']
    def __init__(self) :
        values = get_globals(*T.var_names)
        self.__dict__.update(**values)

class TemplateTest(unittest.TestCase):

    #@unittest.skip("demonstrating skipping")
    def test_03_(self) :
        a, b = TEST.YAHOO.extract(T().DMYT)
        logging.info(a)
        logging.info(b)
         

if __name__ == '__main__' :

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   DMYT = ['DMYT','DMYT+', 'DMYT=','_']

   unittest.main()
