#!/usr/bin/python

import logging
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import pandas as pd
import context

from libUtils import log_on_exception
from libDebug import trace
from cmd_Scrape_Stock import init as TEST_INIT, action as TEST

class T() :
    _data_list = None
    _name = 'Fund Symbol'
    @classmethod
    def _init(cls) :
        if not (cls._data_list is None) :
           return cls._data_list
        target = 'ticker_list'
        ticker_list = globals().get(target,[])

        names, stock_list, etf_list, alias, stock_names, etf_names = TEST_INIT()
        the_list = sorted(names.keys())
        key_list = filter(lambda stock : stock in ticker_list, the_list)
        key_list = list(key_list)
        value_list = map(lambda stock : names[stock], key_list)
        value_list = list(value_list)
        cls._data_list = dict(zip(key_list,value_list))
        return cls._data_list
    @classmethod
    def extract(cls) :
        return cls._init()

class TemplateTest(unittest.TestCase):

    def test_01_(self) :
        target = 'data_store'
        data_store = globals().get(target,[])
        target = 'ticker_list'
        ticker_list = globals().get(target,[])
        stock_list = T.extract()
        logging.debug(stock_list)
        ret,transpose = TEST(data_store,stock_list)
        logging.info(ret)
        ret = pd.DataFrame(ret).T
        logging.info(ret)
        logging.info(transpose)

if __name__ == '__main__' :

   import sys
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   ini_list = env.list_filenames('local/*ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')
   data_store = '../local/historical_prices'
   ticker_list = ['SYY','SBUX','CHTR','AAPL','ELS','XPO','BASI','FNV','COST','HE']
   unittest.main()

