#!/usr/bin/env python

import math
import logging
import pandas as pd

from libCommon import INI, log_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libSharpe import HELPER as MONTECARLO

from libDebug import trace, cpu
from cmd_Method03_step01 import HELPER_THIRDS, HELPER_HALVES
'''
Method 02 step 01 - Divide and Conquer

1) Partition stocks by Sector (enumerated)
2) Partition stocks by Risk into 3 (scaled)
3) Partition stocks by Sharpe into 3 (scaled)

Each sector is now divided into 9 groups :
0_0 : low risk, low sharpe
0_1 : low risk, medium sharpe
0_2 : low risk, high sharpe
1_0 : medium risk, low sharpe
1_1 : medium risk, medium sharpe
1_2 : medium risk, high sharpe
2_0 : high risk, low sharpe
2_1 : high risk, medium sharpe
2_2 : high risk, high sharpe

Write results and basic statistics data about each sub section into ini file
'''
class HELPER() :

    @classmethod
    def transform(cls, key, data) :
        stock_list = data.T.columns.values
        stock_list = sorted(list(stock_list))
        ret = { key + "_stocks" : stock_list } 

        column_list = ['mean', 'std', 'min', 'max']

        temp = data.describe()['sharpe'] 
        value_list = map(lambda key : round(temp[key],2), column_list)
        key_list = map(lambda k : '{}_sharpe_{}'.format(key,k), column_list)
        temp = dict(zip(key_list,value_list))
        ret.update(temp)

        temp = data.describe()['risk'] 
        value_list = map(lambda key : round(temp[key],2), column_list)
        key_list = map(lambda k : '{}_risk_{}'.format(key,k), column_list)
        temp = dict(zip(key_list,value_list))
        ret.update(temp)
        return ret

    @classmethod
    def _debug(cls, data) :
        for v in data.values() :
            if isinstance(v,dict) :
              yield sorted(data)
              break
            else :
              yield sorted(v.T.columns.values)

def _prep() :
    target = 'ini_list'
    ini_list = globals().get(target,[])
    logging.info("loading results {}".format(ini_list))
    ret = {}
    for path, section, key, value in INI.loadList(*ini_list) :
        key = key.replace(section+"_","")
        ret[key] = value
        target = "stocks"
        if target not in key : 
            continue
        logging.debug(key)
        prefix = key.replace(target,"")
        _ret = {}
        for key in sorted(ret) :
            new = key.replace(prefix,"")
            _ret[new] = ret[key]
        target = 'sharpe_max'
        v = _ret.get(target,["0.0"])
        v = float(v[0])
        if v < 1.0 :
           ret = {}
           continue
        logging.info((section,_ret))
        target = 'stocks'
        yield section, _ret.get(target,[])
        ret = {}
def prep() :
    ret = {}
    for section, stocks in _prep() :
        if section not in ret :
           ret[section] = []
        ret[section] = ret[section] + stocks
    for section in ret :
        value = sorted(ret[section])
        yield section, value

def round_values(**data) :
    key_list = data.keys()
    value_list = map(lambda x : data[x], key_list)
    value_list = map(lambda x : round(x,2), value_list)
    ret = dict(zip(key_list,value_list))
    return ret

def load(value_list) :
    target = "file_list"
    file_list = globals().get(target,[])
    ret = {}
    for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
        if len(data) < 7*FINANCE.YEAR :
           logging.info("{} of length {} rejected for being less than {}".format(name,len(data),7*FINANCE.YEAR))
           continue
        data = MONTECARLO.find(data['Adj Close'], period=FINANCE.YEAR)
        #filter stocks that have less than a year
        sharpe = data.get('sharpe',0)
        if sharpe == 0 :
           continue
        #filter stocks that have negative returns
        returns = data.get('returns',0)
        if returns <= 0 : 
           logging.info("{} of returns {} rejected for being unprofitable".format(name,returns))
           continue
        msg = round_values(**data)
        logging.info((name, msg))
        ret[name] = data
    return ret

def rename_keys(sector, **data) : 
    key_list = sorted(data)
    value_list = map(lambda x : data[x], key_list)
    key_list = map(lambda x : "{}_{}".format(sector,x), key_list)
    ret = dict(zip(key_list,value_list))
    return ret

def action() : 
    for sector, stocks in prep() :
        logging.info((sector,len(stocks),stocks))
        stock_list = load(stocks)
        if len(stocks) <= 12 :
           yield sector, {'{}_stocks'.format(sector) : stocks} 
           continue
        ret = {}
        partition = HELPER_HALVES.partition
        if len(stock_list) > 40 :
           partition = HELPER_THIRDS.partition
        logging.info(partition)
        for key, data in partition(stock_list) :
            logging.warn(len(data))
            results = HELPER.transform(key,data)
            results = rename_keys(sector, **results)
            ret.update(results)
        yield sector, ret

@log_exception
@trace
def main(save_file) : 
    ret = INI.init()
    for key, value in action() :
        logging.debug(value)
        INI.write_section(ret,key,**value)
    ret.write(open(save_file, 'w'))
    logging.info("results saved to {}".format(save_file))

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   file_list = env.list_filenames('local/historical_prices/*pkl')
   save_file = "{}/local/method03_step03.ini".format(env.pwd_parent)
   ini_list = env.list_filenames('local/*.ini')
   ini_list = filter(lambda x : 'method03' in x, ini_list)
   ini_list = filter(lambda x : 'step02' in x, ini_list)

   main(save_file)
