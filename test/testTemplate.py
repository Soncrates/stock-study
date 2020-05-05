#!/usr/bin/python

import logging
import sys
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import context

from libDecorators import log_on_exception, singleton
from libDebug import trace

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
    var_names = ['ini_files','file_list']
    def __init__(self) :
        values = get_globals(*T.var_names)
        self.__dict__.update(**values)

class TemplateTest(unittest.TestCase):

    def setUp(self, delete=False):
        print("setup")
    def tearDown(self, delete=False):
        print('tearDown')
    @unittest.skipIf(sys.platform.startswith("win"), "requires Windows")
    def test_01_(self) :
        print((1,sys.platform))
    @unittest.skipUnless(sys.platform.startswith("linux"), "requires Linux")
    def test_02_(self) :
        print((2,sys.platform))
    @unittest.skip("demonstrating skipping")
    def test_03_(self) :
        print ("skip")
    @unittest.expectedFailure
    def test_fail(self):
        self.assertEqual(1, 0, "broken")

if __name__ == '__main__' :

   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')

   unittest.main()
