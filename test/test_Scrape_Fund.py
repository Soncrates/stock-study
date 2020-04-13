#!/usr/bin/python

import logging
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import pandas as pd
import context
from context import test_fund_data_store, test_fund_ticker_list

from libUtils import log_on_exception
from libDebug import trace
from libNASDAQ import NASDAQ
from cmd_Scrape_Fund import action as TEST

class T() :
    _data_list = None
    _name = 'Fund Symbol'
    @classmethod
    def _init(cls) :
        if not (cls._data_list is None) :
           return cls._data_list
        target = 'test_fund_ticker_list'
        ticker_list = globals().get(target,[])
        fund_list = NASDAQ.init().fund_list()
        ret = filter(lambda fund : fund[cls._name] in  ticker_list, fund_list)
        cls._data_list = list(ret)
        return cls._data_list
    @classmethod
    def extract(cls) :
        for i, fund in enumerate(cls._init()) :
            ticker = fund.get(cls._name, 'Unknown')
            yield ticker, fund

class TemplateTest(unittest.TestCase):

    def test_01_(self) :
        target = 'test_fund_data_store'
        data_store = globals().get(target,[])
        target = 'test_fund_ticker_list'
        ticker_list = globals().get(target,[])
        fund_list = []
        for ticker, fund in T.extract() :
            fund_list.append(fund)

        ret,transpose = TEST(data_store,fund_list)
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

   unittest.main()

