#!/usr/bin/python

import logging
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import pandas as pd
import context
from context import test_stock_data_store, test_stock_ticker_list

from libUtils import log_on_exception
from libDebug import trace
from cmd_Scrape_Tickers import init as TEST_INIT, LOAD as TEST

class T() :
    _data_list = None
    _name = 'Fund Symbol'
    @classmethod
    def _init(cls) :
        if not (cls._data_list is None) :
           return cls._data_list
        target = 'test_stock_ticker_list'
        ticker_list = globals().get(target,[])

        fund_list,stock_list, etf_list, alias = TEST_INIT()
        fund_name_list = map(lambda fund : fund.get(cls._name,None), fund_list)
        fund_name_list = list(fund_name_list)

        the_list = sorted(stock_list + fund_name_list + etf_list)
        logging.info(len(the_list))
        key_list = filter(lambda stock : stock in ticker_list, the_list)
        cls._data_list = list(key_list)
        return cls._data_list
    @classmethod
    def extract(cls) :
        return cls._init()

class TemplateTest(unittest.TestCase):

    def test_01_(self) :
        target = 'test_stock_data_store'
        data_store = globals().get(target,[])
        target = 'test_stock_ticker_list'
        ticker_list = globals().get(target,[])
        stock_list = T.extract()
        logging.debug(stock_list)
        TEST.robust(data_store,stock_list)

if __name__ == '__main__' :

   import sys
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   unittest.main()

