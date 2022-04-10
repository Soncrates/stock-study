#!/usr/bin/python

import logging
import sys
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import context

from libCommon import find_subset, LOG_FORMAT_TEST, find_files
from libDecorators import singleton
#from libDebug import trace,cpu

@singleton
class T() :
    var_names = ['ini_files','file_list']
    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*T.var_names))

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
   logging.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=logging.INFO)

   ini_list = '{pwd_parent}/local/*ini'.format(**vars(env))
   ini_list = find_files(ini_list)
   file_list = '{pwd_parent}/local/historical_prices/*pkl'.format(**vars(env))
   file_list = find_files(file_list)

   unittest.main()
