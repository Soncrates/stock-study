#!/usr/bin/python

import logging
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import context

from libCommon import log_on_exception
from libDebug import trace
from libFinance import STOCK_TIMESERIES, TRANSFORM_DAILY, HELPER

class TemplateTest(unittest.TestCase):

    _data_list = None
    @classmethod 
    def _init(cls) :
        if not (cls._data_list is None) :
           return cls._data_list
        target = 'reader'
        reader = globals().get(target,None)
        target = 'stock_list'
        stock_list = globals().get(target,[])
        cls._data_list = map(lambda stock : reader.extract_from_yahoo(stock), stock_list)
        cls._data_list = list(cls._data_list)
        return cls._data_list
    @classmethod 
    def extract(cls) :
        target = 'stock_list'
        stock_list = globals().get(target,[])
        for i, stock in enumerate(stock_list) :
            yield stock, cls._init()[i]

    def test_01_(self) :
        for stock, ret in TemplateTest.extract() :
            logging.debug (stock)
            logging.debug (ret.describe().round(2))

    def test_02_(self) :
        for stock, ret in TemplateTest.extract() :
            logging.debug (stock)
            daily = TRANSFORM_DAILY.enrich(ret)
            logging.debug (daily.describe().round(2))
    def test_03_(self) :
        for stock, ret in TemplateTest.extract() :
            logging.debug (stock)
            daily = HELPER.findDailyReturns(ret)
            logging.debug (daily.describe().round(2))
    def test_04_(self) :
        for stock, ret in TemplateTest.extract() :
            logging.debug (stock)
            daily = HELPER.findDailyReturns(ret)
            _risk, _returns = HELPER.findRiskAndReturn(daily)
            msg = [_returns,_risk,_returns/_risk]
            msg = dict(zip(['returns','risk','sharpe'],msg))
            logging.debug(msg)
    def test_05_(self) :
        for stock, ret in TemplateTest.extract() :
            logging.debug (stock)
            daily = HELPER.findDailyReturns(ret)
            _risk, _returns = HELPER.findRiskAndReturn(daily, period=HELPER.YEAR)
            msg = [_returns,_risk,_returns/_risk]
            msg = dict(zip(['returns','risk','sharpe'],msg))
            logging.debug(msg)

if __name__ == '__main__' :

   import sys
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   end = "2019-06-01"
   reader = STOCK_TIMESERIES.init(end=end)
   stock_list = ['AAPL','IBM']
   stock_list = ['IBM','AAPL','^GSPC']

   unittest.main()

