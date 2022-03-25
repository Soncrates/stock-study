#!/usr/bin/python

import logging
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import pandas as pd
import context
from context import test_fund_data_store, test_fund_ticker_list

from libDecorators import log_on_exception,singleton
from libDebug import trace
from libNASDAQ import NASDAQ
import cmd_Scrape_Fund as TEST

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
    var_names = ['test_fund_ticker_list', 'test_fund_data_store']
    def __init__(self) :
        values = get_globals(*T.var_names)
        self.__dict__.update(**values)
        fund_list = TEST.get_tickers()
        key_list = filter(lambda fund :  fund in self.test_fund_ticker_list, fund_list)
        values = map(lambda key : fund_list[key], key_list)
        self.fund_list = dict(zip(key_list,values))

class TestScrapeStock(unittest.TestCase):

    def test_01_(self) :
        ret,transpose = TEST.action(T().test_fund_data_store,T().fund_list)
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
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.DEBUG)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   unittest.main()

