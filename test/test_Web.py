#!/usr/bin/python

import logging
import sys
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import context

from libUtils import log_on_exception
from libDebug import trace
from libWeb import YAHOO_PROFILE
from libWeb import FINANCEMODELLING_PROFILE, FINANCEMODELLING_STOCK_LIST, FINANCEMODELLING_INDEX

class TemplateTest(unittest.TestCase):

    def test_02_stock_list(self) :
        ret_list = FINANCEMODELLING_STOCK_LIST.get()
        for i,ret in enumerate(ret_list) :
            logging.info ((i, ret))
    def test_03_profile(self) :
        target = 'stock_list'
        stock_list = globals().get(target,[])
        for i, stock in enumerate(stock_list) :
            logging.info ((i,stock))
            ret = YAHOO_PROFILE.get(stock)
            logging.info ((i,ret))
    def test_04_profile(self) :
        target = 'stock_list'
        stock_list = globals().get(target,[])
        for i, stock in enumerate(stock_list) :
            logging.info ((i,stock))
            ret = FINANCEMODELLING_PROFILE.get(stock)
            logging.info ((i,ret))
    def test_05_index(self) :
        for index in FINANCEMODELLING_INDEX.get() :
            logging.info (index)

if __name__ == '__main__' :

   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   ini_list = env.list_filenames('local/*ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')
   stock_list = ['AAPL','GOOG','SPY', 'SRCpA','SRC-A', 'SRC$A', 'SRCA']

   unittest.main()
   #main()
   

