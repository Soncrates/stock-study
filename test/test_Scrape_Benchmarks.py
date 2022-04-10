# -*- coding: utf-8 -*-
"""
Created on Sat Apr  9 22:44:36 2022

@author: emers
"""

#!/usr/bin/python

import logging as log
import unittest
import context
import pandas as pd

from libCommon import find_subset, LOG_FORMAT_TEST,find_files
from libDecorators import singleton

from cmd_Scrape_Benchmarks import prep

from libBusinessLogic import YAHOO_SCRAPER, TRANSFORM_TICKER, BASE_PANDAS_FINANCE
from libFinance import TRANSFORM_BACKGROUND
#from libDebug import trace,cpu

@singleton
class T() :
    var_names = ['ini_files','file_list','config_file_list','benchmarks','omit_list','data_store','scraper']
    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*T.var_names))

class TestBenchmarks(unittest.TestCase):

    def test_03(self) :
        benchmark_list = prep(T().config_file_list , T().benchmarks,T().omit_list )
        ticker_list = list(benchmark_list.values())
        log.debug(benchmark_list)
        log.debug(ticker_list)
    def test_04(self) :
        benchmark_list = prep(T().config_file_list , T().benchmarks,T().omit_list )
        ticker_list = list(benchmark_list.values())
        BASE_PANDAS_FINANCE.SAVE(T().data_store, *ticker_list, **T().scraper)
    def test_05(self) :
        benchmark_list = prep(T().config_file_list , T().benchmarks,T().omit_list )
        ticker_list = list(benchmark_list.values())
        ret = {}
        for ticker, data in BASE_PANDAS_FINANCE.LOAD(T().data_store, *ticker_list) :
            ret[ticker] = TRANSFORM_BACKGROUND.find(data)
            log.info((ticker,ret[ticker]))
        ret = pd.DataFrame(ret)
        log.debug(ret)
    def test_06(self) :
        benchmark_list = prep(T().config_file_list , T().benchmarks,T().omit_list )
        ticker_list = list(benchmark_list.values())
        ret = {}
        for ticker, data in BASE_PANDAS_FINANCE.LOAD(T().data_store, *ticker_list) :
            ret[ticker] = TRANSFORM_BACKGROUND.find(data)
        ret = pd.DataFrame(ret)
        ret = TRANSFORM_TICKER.enhance(ret,benchmark_list)
        log.debug(ret)

if __name__ == '__main__' :

   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=log.DEBUG)

   data_store  = '{pwd_parent}/local/historical_prices'.format(**vars(env))

   ini_list = '{pwd_parent}/local/*ini'.format(**vars(env))
   ini_list = find_files(ini_list)
   config_file_list = [ filename for filename in ini_list if 'benchmark' in filename ]

   file_list = '{pwd_parent}/local/historical_prices/*pkl'.format(**vars(env))
   file_list = find_files(file_list)

   benchmarks = ['Index']
   omit_list = ['ACT Symbol', 'CQS Symbol', 'alias', 'unknown']

   scraper = YAHOO_SCRAPER.pandas()

   unittest.main()

