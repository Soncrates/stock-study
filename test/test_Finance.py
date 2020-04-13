#!/usr/bin/python

import logging
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import pandas as pd
import context
from context import test_finance_end, test_finance_stock_list

from libUtils import log_on_exception
from libDebug import trace, cpu
from libFinance import STOCK_TIMESERIES, HELPER
from libFinance import TRANSFORM_DAILY as TEST_DAILY
from libFinance import TRANSFORM_SHARPE as TEST_SHARPE
from libFinance import TRANSFORM_CAGR as TEST_CAGR
from libFinance import TRANSFORM_DRAWDOWN as TEST_DRAWDOWN
from libFinance import TRANSFORM_BACKGROUND as TEST_BACKGROUND

class T() :
    _data_list = None
    @classmethod 
    def _init(cls) :
        if not (cls._data_list is None) :
           return cls._data_list
        target = 'test_finance_end'
        end = globals().get(target,None)
        reader = STOCK_TIMESERIES.init(end=end)
        target = 'test_finance_stock_list'
        stock_list = globals().get(target,[])
        cls._data_list = map(lambda stock : reader.extract_from_yahoo(stock), stock_list)
        cls._data_list = list(cls._data_list)
        return cls._data_list
    @classmethod 
    def extract(cls) :
        target = 'test_finance_stock_list'
        stock_list = globals().get(target,[])
        for i, stock in enumerate(stock_list) :
            yield stock, cls._init()[i].copy()

class TEST_01(unittest.TestCase):

    def test_01_(self) :
        for stock, ret in T.extract() :
            logging.info (stock)
            logging.info (ret.describe().round(2))
            daily = HELPER.findDailyReturns(ret)
            logging.info (daily.describe().round(2))

    def test_03_(self) :
        msg = {}
        for stock, ret in T.extract() :
            daily = HELPER.findDailyReturns(ret)
            _risk, _returns = HELPER.findRiskAndReturn(daily)
            value = [_returns,_risk,_returns/_risk]
            value = dict(zip(['returns','risk','sharpe'],value))
            msg[stock] = value
        msg = pd.DataFrame(msg).T
        logging.info(msg)
    def test_04_(self) :
        msg = {}
        for stock, ret in T.extract() :
            daily = HELPER.findDailyReturns(ret)
            _risk, _returns = HELPER.findRiskAndReturn(daily, period=HELPER.YEAR)
            value = [_returns,_risk,_returns/_risk]
            value = dict(zip(['returns','risk','sharpe'],value))
            msg[stock] = value
        msg = pd.DataFrame(msg).T
        logging.info(msg)
    def test_05_(self) :
        msg = {}
        for stock, ret in T.extract() :
            value_1 = ret['Adj Close'].pct_change() + 1
            _risk, _returns = HELPER.findRiskAndReturn(value_1, period=HELPER.YEAR)
            value_1 = [_returns,_risk,_returns/_risk]
            value_1 = dict(zip(['returns','risk','sharpe'],value_1))
            value_2 = ret['Adj Close'].pct_change()
            _risk, _returns = HELPER.findRiskAndReturn(value_2, period=HELPER.YEAR)
            value_2 = [_returns,_risk,_returns/_risk]
            value_2 = dict(zip(['returns','risk','sharpe'],value_2))
            msg[stock] = value_2
            msg[stock + "+ 1"] = value_1
        msg = pd.DataFrame(msg)
        logging.info(msg.T)

class TEST_02_DAILY(unittest.TestCase):
    def test_01_daily_find(self) :
        msg = {}
        for stock, ret in T.extract() :
            value = TEST_DAILY.find(ret['Adj Close'])
            logging.debug (value)
            msg[stock] = value
        msg = pd.DataFrame(msg)
        logging.info(msg)
class TEST_03_SHARPE(unittest.TestCase):
    def test_02_SHARPE_annualize(self) :
        msg = {}
        msg['t1'] = TEST_SHARPE.annualize(.10,.20,10)
        msg['t2'] = TEST_SHARPE.annualize(.10,.20,30)
        msg['t3'] = TEST_SHARPE.annualize(.15,.20,10)
        msg['t4'] = TEST_SHARPE.annualize(.15,.20,30)
        msg = pd.DataFrame(msg)
        msg.rename(index={0:'risk', 1: 'return'},inplace=True)
        msg = msg.T
        logging.info(msg)
    def test_03_SHARPE_extract(self) :
        msg = {}
        for stock, ret in T.extract() :
            daily = TEST_DAILY.find(ret['Adj Close'])
            #data, risk_free_rate, period, span, size = TEST_SHARPE.validate(ret['Adj Close'])
            msg[stock] = TEST_SHARPE.extractRR(daily)
            msg[stock+"Rolling 30"] = TEST_SHARPE.extractRR(daily,span=30)
            msg[stock+"Rolling year"] = TEST_SHARPE.extractRR(daily,span=HELPER.YEAR)
            msg[stock+"Rolling 2 year"] = TEST_SHARPE.extractRR(daily,span=2*HELPER.YEAR)
        msg = pd.DataFrame(msg)
        msg.rename(index={0:'risk', 1: 'return'},inplace=True)
        msg = msg.T
    def test_04_SHARPE_find(self) :
        msg = {}
        for stock, ret in T.extract() :
            data, risk_free_rate, period, span, size = TEST_SHARPE.validate(ret['Adj Close'])
            value = TEST_SHARPE.find(data, risk_free_rate, period, span, size)
            logging.debug (value)
            msg[stock] = value
        msg = pd.DataFrame(msg)
        logging.info(msg.T)
    def test_05_SHARPE_enrich(self) :
        msg = None
        for stock, ret in T.extract() :
            msg = TEST_SHARPE.enrich(ret['Adj Close'], stock, msg)
        logging.info(msg)
class TEST_04_CAGR(unittest.TestCase):
    def test_01_CAGR_find(self) :
        msg = {}
        for stock, ret in T.extract() :
            growth, periods = TEST_CAGR.validate(ret['Adj Close'])
            value = TEST_CAGR.find(growth, periods)
            logging.debug (value)
            msg[stock] = value
        msg = pd.DataFrame(msg)
        logging.info(msg.T)
    def test_02_CAGR_enrich(self) :
        msg = None
        for stock, ret in T.extract() :
            msg = TEST_CAGR.enrich(ret['Adj Close'], stock, msg)
        logging.info(msg)
class TEST_05_DRAWDOWN(unittest.TestCase):
    def test_01_DRAWDOWN_find(self) :
        msg = {}
        for stock, ret in T.extract() :
            ret = TEST_DRAWDOWN.validate(ret['Adj Close'])
            value = TEST_DRAWDOWN.find(ret)
            logging.debug (value)
            msg[stock] = value
        msg = pd.DataFrame(msg)
        logging.info(msg.T)
    def test_02_DRAWDOWN_enrich(self) :
        msg = None
        for stock, ret in T.extract() :
            msg = TEST_DRAWDOWN.enrich(ret['Adj Close'], stock, msg)
        logging.info(msg)
class TEST_06_BACKGROUND(unittest.TestCase):
    def test_01_BACKGROUND(self) :
        msg = {}
        for stock, ret in T.extract() :
            msg[stock] = TEST_BACKGROUND.find(ret)
        logging.info(msg)
        msg = pd.DataFrame(msg).T
        logging.info(msg)
    def test_02_BACKGROUND(self) :
        msg = None
        for stock, ret in T.extract() :
            msg = TEST_BACKGROUND.enrich(ret, stock, msg)
        logging.info(msg)

if __name__ == '__main__' :

   import sys
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   #end = "2019-06-01"
   #reader = STOCK_TIMESERIES.init(end=end)
   #stock_list = ['AAPL','IBM']
   #stock_list = ['IBM','AAPL','^GSPC']

   unittest.main()

