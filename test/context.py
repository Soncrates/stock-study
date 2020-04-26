from collections import Counter
import os
import sys
import re
import logging
'''
import inspect
_cf = inspect.currentframe()
_cf = inspect.getfile(_cf)
_cf = os.path.abspath(_cf)
_cf = os.path.dirname(_cf)
'''
def add_context(arg=None) :
    if arg is None :
       arg = os.path.dirname(__file__)
       arg = os.path.join(arg, '../bin')
       arg = os.path.abspath(arg)
    sys.path.insert(0, arg)

add_context()

log_file = 'testResults/tests.log'
log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'

test_csv = ['testConfig/test.csv']

test_ftp_server = 'ftp.nasdaqtrader.com'
test_ftp_file = '/symboldirectory/mfundslist.txt'
test_ftp_dir = 'symboldirectory'

test_finance_end = "2019-06-01"
test_finance_stock_list = ['AAPL','IBM']
test_finance_stock_list = ['IBM','AAPL','^GSPC']

test_stock_data_store = '../local/historical_prices'
test_stock_ticker_list = ['SYY','SBUX','CHTR','AAPL','ELS','XPO','BASI','FNV','COST','HE']

test_fund_data_store = '../local/historical_prices_fund'
test_fund_ticker_list = ['VBIAX','FBALX','FTBFX','YCGEX','VWELX','FCNTX','FSDIX','PRSVX','TRBCX','IAAAAX']

test_ini_input_file = ['testConfig/testRead.ini']
test_ini_output_file = 'testResults/testWrite.ini'
test_ini_output_data = { 'stanza 1 simple' : {'string' : 'I am a string', 'string with single quote' : "I don't like single quotes", 'int0' : 0, 'int2' : 2, 'int negative' : -2}
            , 'stanza 2 complex' : { 'list' : ['a','b','c']
                , 'dictionary' : { 'letters' : ['a','b','c']
                    , 'numbers' : [0,1,2]
                    , 'single quote' : "I don't like single quotes"}
                }
            , 'stanza 3 special cases' : { '%eix' : -2
               , 'GQ=F' : -2
               , '^SNP500' : 3
               }
            }

test_pdf_save_file = 'testResults/hello.pdf'

test_plot_dir = 'testResults'
test_plot_stock_list = ["SPY", "HD", "PYPL", "SBUX", "UNH", "WEC"]

