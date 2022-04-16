#!/usr/bin/python

import logging
import unittest
import context

from libCommon import LOG_FORMAT_TEST, find_subset
from libCommon import FTP
from libNASDAQ import NASDAQ as TEST
from libBusinessLogic import NASDAQ_EXTRACT as FINAL

test_columns_list = ['Security Name', 'Market Category', 'Financial Status', 'Round Lot Size', 'ETF', 'NextShares']
test_columns_list = ['Market Category', 'Round Lot Size', 'ETF', 'NextShares']
test_columns_traded = ['Nasdaq Traded', 'Security Name', 'Listing Exchange', 'Market Category', 'ETF', 'Round Lot Size', 'Financial Status', 'CQS Symbol', 'NASDAQ Symbol', 'NextShares']
test_columns_traded = ['Nasdaq Traded', 'ETF', 'Round Lot Size', 'NextShares']
test_columns_other =  ['ACT Symbol', 'Security Name', 'Exchange', 'CQS Symbol', 'ETF', 'Round Lot Size', 'NASDAQ Symbol']
test_columns_other =  ['Exchange', 'ETF', 'Round Lot Size']
test_columns_funds =  ['Type', 'Category']
test_columns_bonds =  ['Security Name', 'Financial Status']
test_columns_bonds =  ['Financial Status']
test_columns_participants =  ['MP Type', 'Name', 'Location', 'Telephone', 'NASDAQ Member', 'FINRA Member', 'NASDAQ BX Member', 'PSX Participant']
test_columns_participants =  ['MP Type', 'NASDAQ Member', 'FINRA Member', 'NASDAQ BX Member', 'PSX Participant']

class TEST_NASDAQ(unittest.TestCase):

    def test_Total(self) :
        fund_list,stock_list, etf_list, alias = FINAL()
        
    #@unittest.skip("demonstrating skipping")
    def test_01_(self) :
        test = TEST.init()
        msg = { key for key in vars(test) if key.startswith("__")==False}
        logging.info(msg)
        msg = { key for key in dir(test) if key.startswith("__")==False}
        logging.info(msg)
        logging.info(type(test))
        logging.info(type(test.ftp))
        logging.info(vars(test.ftp))
        ret = FTP.LIST(test.ftp, pwd = 'symboldirectory')
        ret = set(ret)
        file_list = set(TEST.file_list)
        lhs = ret - file_list
        rhs = file_list - ret
        if len(lhs) > 0 :
              logging.warn(lhs)
        if len(rhs) > 0 :
              logging.warn(rhs)
        for i, name in enumerate(test.file_list) :
              logging.info((i, name))
        logging.info(ret)

    def test_04_(self) :
        test = TEST.init()
        a,raw = test.extract_listed()
        logging.info(a.filter(items=test_columns_list))
        logging.info(a.filter(items=test_columns_list).describe())
        logging.info(list(a.columns))

    def test_05_(self) :
        test = TEST.init()
        a,raw = test.extract_traded_list()
        logging.info(a.filter(items=test_columns_traded))
        logging.info(a.filter(items=test_columns_traded).describe())
        logging.info(list(a.columns))

    def test_06_(self) :
        test = TEST.init()
        a,raw = test.extract_other_list()
        logging.info(a.filter(items=test_columns_other))
        logging.info(a.filter(items=test_columns_other).describe())
        logging.info(list(a.columns))

    def test_07_(self) :
        test = TEST.init()
        a,raw = test.extract_fund_list()
        logging.info(a.filter(items=test_columns_funds))
        logging.info(a.filter(items=test_columns_funds).describe())
        logging.info(list(a.columns))

    def test_08_(self) :
        test = TEST.init()
        a,raw = test.extract_bond_list()
        logging.info(a.filter(items=test_columns_bonds))
        logging.info(a.filter(items=test_columns_bonds).describe())
        logging.info(list(a.columns))

    def test_09_(self) :
        test = TEST.init()
        a,raw = test.extract_participant_list()
        logging.info(a.filter(items=test_columns_participants))
        logging.info(a.filter(items=test_columns_participants).describe())
        logging.info(list(a.columns))

    def test_10_(self) :
        test = TEST.init()
        stock, etf, alias = test.extract_stock_list()
        logging.info(list(stock.columns))
        logging.info(list(etf.columns))
        logging.info(list(alias.columns))
    #@unittest.expectedFailure        

if __name__ == '__main__' :

   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   logging.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=logging.DEBUG)
   #logging.basicConfig(stream=sys.stdout, format=LOG_FORMAT_TEST, level=logging.DEBUG)

   unittest.main()

# -*- coding: utf-8 -*-

