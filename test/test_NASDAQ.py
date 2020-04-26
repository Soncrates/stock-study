#!/usr/bin/python

import logging
import sys
import pandas as pd
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import context

from libUtils import log_on_exception
from libDebug import trace

from libNASDAQ import NASDAQ as TEST

def to_dict(target,*entry_list) :
    logging.info(entry_list[0])
    ret = {}
    for entry in entry_list :
        key = entry.pop(target,target)
        ret[key] = dict(zip(entry,entry.values()))
        if len(ret) == 1 :
           logging.info(ret)
    return ret
def to_pandas(**kwargs) :
    _columns = list(kwargs[list(kwargs.keys())[0]].keys())
    logging.info(_columns)
    ret = pd.DataFrame.from_dict(kwargs, orient='index',columns=_columns)
    return ret

test_columns_list = ['Security Name', 'Market Category', 'Financial Status', 'Round Lot Size', 'ETF', 'NextShares']
test_columns_list = ['Market Category', 'Round Lot Size', 'ETF', 'NextShares']
test_columns_traded = ['Nasdaq Traded', 'Security Name', 'Listing Exchange', 'Market Category', 'ETF', 'Round Lot Size', 'Financial Status', 'CQS Symbol', 'NASDAQ Symbol', 'NextShares']
test_columns_traded = ['Nasdaq Traded', 'ETF', 'Round Lot Size', 'NextShares']
test_columns_other =  ['ACT Symbol', 'Security Name', 'Exchange', 'CQS Symbol', 'ETF', 'Round Lot Size', 'NASDAQ Symbol']
test_columns_funds =  ['Type', 'Category']
test_columns_bonds =  ['Security Name', 'Financial Status']
test_columns_bonds =  ['Financial Status']
test_columns_participants =  ['MP Type', 'Name', 'Location', 'Telephone', 'NASDAQ Member', 'FINRA Member', 'NASDAQ BX Member', 'PSX Participant']
test_columns_participants =  ['MP Type', 'NASDAQ Member', 'FINRA Member', 'NASDAQ BX Member', 'PSX Participant']

class TemplateTest(unittest.TestCase):

    #@unittest.skip("demonstrating skipping")
    def test_03_(self) :
        test = TEST.init()
        test()
    def test_04_(self) :
        test = TEST.init()
        a,raw = test.listed()
        logging.info(type(raw))
        #a = a[::50]
        a = to_dict('Symbol',*a)
        a = to_pandas(**a)
        logging.info(a.filter(items=test_columns_list))
        logging.info(a.filter(items=test_columns_list).describe())
        logging.info(list(a.columns))
    def test_05_(self) :
        test = TEST.init()
        a,raw = test.traded()
        a = to_dict('Symbol',*a)
        a = to_pandas(**a)
        logging.info(a.filter(items=test_columns_traded))
        logging.info(a.filter(items=test_columns_traded).describe())
        logging.info(list(a.columns))
    def test_06_(self) :
        test = TEST.init()
        a,raw = test.other()
        a = to_dict('NASDAQ Symbol',*a)
        a = to_pandas(**a)
        logging.info(a.filter(items=test_columns_other))
        logging.info(a.filter(items=test_columns_other).describe())
        logging.info(list(a.columns))
    def test_07_(self) :
        test = TEST.init()
        a,raw = test.funds()
        a = to_dict('Fund Symbol',*a)
        a = to_pandas(**a)
        logging.info(a.filter(items=test_columns_funds))
        logging.info(a.filter(items=test_columns_funds).describe())
        logging.info(list(a.columns))
    def test_08_(self) :
        test = TEST.init()
        a,raw = test.bonds()
        a = to_dict('Symbol',*a)
        a = to_pandas(**a)
        logging.info(a.filter(items=test_columns_bonds))
        logging.info(a.filter(items=test_columns_bonds).describe())
        logging.info(list(a.columns))
    def test_09_(self) :
        test = TEST.init()
        a,raw = test.participants()
        a = to_dict('MPID',*a)
        a = to_pandas(**a)
        logging.info(a.filter(items=test_columns_participants))
        logging.info(a.filter(items=test_columns_participants).describe())
        logging.info(list(a.columns))
    @unittest.expectedFailure
    def test_fail(self):
        self.assertEqual(1, 0, "broken")

if __name__ == '__main__' :

   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   unittest.main()
