#!/usr/bin/python

import logging
import sys
import unittest
#import numpy.testing as np_test
#import pandas.util.testing as pd_test
import context

from libDecorators import log_on_exception, singleton
from libDebug import trace
from libUtils import WEB

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
    var_names = ['test_stock_ticker_list','test_stock_data_store','stock_list', 'yahoo']
    def __init__(self) :
        values = get_globals(*T.var_names)
        self.__dict__.update(**values)
        url_list = map(lambda ticker : self.yahoo.format(ticker), self.stock_list)
        self.yahoo_backgrounds = list(url_list)

class TemplateTest(unittest.TestCase):

    def test_02_text(self) :
        for url in T().yahoo_backgrounds :
            logging.info(url)
            ret = WEB.get_text(url)
            logging.debug(ret[0:50])
    def test_04_content(self) :
        for url in T().yahoo_backgrounds :
            logging.info(url)
            ret = WEB.get_content(url)
            if ret is None :
               ret = WEB.get_content(url)
            soup = WEB.format_as_soup(ret)
            span_list = soup.body.findAll('span')
            data = []
            for span in span_list :
                data.append(span.text)
            while True :
                if data[0] == 'Sector' :
                   break
                if data[0] == 'Category' :
                   break
                data = data[1:]
                if len(data) == 0 :
                   return {}
            key_list = data[0::2]
            value_list = data[1::2]
            logging.debug(data)
            logging.debug(key_list)
            logging.debug(value_list)
            data = dict(zip(key_list,value_list))
            data = { key:value for (key,value) in data.items() if len(key) > 0 and key[0] != '1'}
            data = { key:value for (key,value) in data.items() if key != data[key]}
            data = { key:value for (key,value) in data.items() if data[key]!='N/A'}
            logging.info(sorted(data.keys()))
            for i, key in enumerate(sorted(data)) :
                logging.info((i,key,data[key]))

    def test_05_index(self) :
        pass

if __name__ == '__main__' :

   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   stock_list = ['AAPL','GOOG','SPY', 'SRCpA','SRC-A', 'SRC$A', 'SRCA']
   yahoo = "https://finance.yahoo.com/quote/{0}/profile?p={0}"

   unittest.main()
