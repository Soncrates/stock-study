#!/usr/bin/python

import logging
import sys
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import pandas as pd
import context

from libDecorators import exit_on_exception, log_on_exception, singleton
from libDebug import trace
from cmd_Method05 import SUB_ACTOR as TEST_1
from cmd_Method05 import ACTOR as TEST_2

def get_globals(*largs) :
    ret = {}
    for name in largs :
        value = globals().get(name,None)
        if value is None :
           raise ValueError('{} not in {}'.format(name,sorted(globals())))
        ret[name] = value
    return ret

@singleton
@exit_on_exception
class T() :
    var_names = ['ini_list','price_list','data','columns_drop', 'threshold','portfolio_iterations','sector_cap','prices']
    def __init__(self) :
        values = get_globals(*T.var_names)
        self.__dict__.update(**values)
        self.test_01 = TEST_1(self.portfolio_iterations,self.threshold,self.columns_drop)
        self_test_02 = TEST_2(self.price_list,self.prices,self.sector_cap)

class TemplateTest(unittest.TestCase):

    def setUp(self, delete=False):
        print("setup")
    def tearDown(self, delete=False):
        print('tearDown')
    def test_01(self):
        logging.info(T().data)
        avg = T().data.drop(labels=T().columns_drop)
        avg = T().test_01.find_average(avg)
        logging.info(avg)
    @unittest.skip(" ")
    def test_02(self):
        avg = T().data.drop(labels=T().columns_drop)
        avg = T().test_01.find_average(avg)
        test = None
        test = TEST_1.portfolio(T().prices,avg,T().portfolio_iterations*5,test)
        logging.info(test)
if __name__ == '__main__' :

   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*ini')
   price_list = env.list_filenames('local/historical_*/*pkl')
   prices = 'Adj Close'
   floats_in_summary = ['CAGR', 'GROWTH', 'LEN', 'RISK', 'SHARPE','RETURNS']

   data = {'portfolio_6506': {'APD': 0.1855, 'FNV': 0.2135, 'NEU': 0.0997, 'SHW': 0.2285, 'WDFC': 0.2729, 'returns': 0.2117, 'risk': 0.1703, 'sharpe': 1.1261}
  , 'portfolio_3762': {'APD': 0.0895, 'FNV': 0.2284, 'NEU': 0.1037, 'SHW': 0.3117, 'WDFC': 0.2667, 'returns': 0.2169, 'risk': 0.1715, 'sharpe': 1.1476}
  , 'portfolio_6167': {'APD': 0.0, 'FNV': 0.2277, 'NEU': 0.107, 'SHW': 0.3801, 'WDFC': 0.2852, 'returns': 0.2211, 'risk': 0.1744, 'sharpe': 1.1534}}

   data = pd.DataFrame(data)
   drop = ['returns','risk','sharpe']
   threshold = 0.20
   sector_cap = 8
   portfolio_iterations = 500
   columns_drop = ['returns','risk','sharpe']

   unittest.main()
