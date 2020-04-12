#!/usr/bin/python

import logging
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import context

from libCommon import CSV as TEST
from libUtils import log_on_exception
from libDebug import trace

class TemplateTest(unittest.TestCase):

    def test_01_(self) :
        target = 'test_csv'
        target = globals().get(target,None)
        counter = 15
        for a in TEST.rows(target[0]) :
            logging.debug(a)
            counter -= 1
            if counter == 0 :
               break
    def test_02_(self) :
        target = 'test_csv'
        target = globals().get(target,None)
        counter = 15
        for a in TEST.to_dict(target[0]) :
            logging.debug(a)
            counter -= 1
            if counter == 0 :
               break
if __name__ == '__main__' :

   import sys
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.DEBUG)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   test_csv = env.list_filenames('testConfig/test.csv')

   unittest.main()

