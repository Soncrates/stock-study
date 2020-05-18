#!/usr/bin/python

import sys,logging
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import pandas as pd
import context
from context import test_stock_ticker_list, test_stock_data_store

from libDecorators import singleton
from libDebug import debug_object
import cmd_Scrape_Stock as TEST

def get_globals(*largs) :
    logging.info("Valid globals : {}".format(sorted(globals().keys())))
    ret = {}
    for name in largs :
        value = globals().get(name,None)
        if value is None :
           continue
        ret[name] = value
    return ret

@singleton
class T() :
    var_names = ['test_stock_ticker_list','test_stock_data_store']
    def __init__(self) :
        values = get_globals(*T.var_names)
        self.__dict__.update(**values)
        total, background = TEST.get_tickers()
        ret = filter(lambda stock : stock in self.test_stock_ticker_list, total)
        self.ticker_list = sorted(list(set(ret)))
        values = map(lambda ticker : background[ticker], self.ticker_list)
        self.data = dict(zip(self.ticker_list,values))
        debug_object(self)

class TemplateTest(unittest.TestCase):

    def test_01_(self) :
        ret,transpose = TEST.action(T().test_stock_data_store,T().ticker_list, T().data)
        logging.info(ret)
        ret = pd.DataFrame(ret).T
        logging.info(ret)
        logging.info(transpose)

if __name__ == '__main__' :

   import sys
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   unittest.main()

