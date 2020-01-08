#!/usr/bin/env python

import math
import logging
import pandas as pd

from libCommon import INI, log_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libSharpe import HELPER as MONTECARLO

from libDebug import trace, cpu
from cmd_Method03_step01 import HELPER_THIRDS, HELPER_HALVES, HELPER
from cmd_Method03_step02 import HELPER2
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

def _prep(*ini_list) :
    ret = {}
    target = "stocks"
    for path, section, key, value in INI.loadList(*ini_list) :
        key = key.replace(section+"_","")
        ret[key] = value
        if target not in key :
            continue
        logging.debug(key)
        prefix = key.replace(target,"")
        ret = HELPER2.transform(prefix,ret)
        if HELPER2.sharpe_cap(ret) :
           ret = {}
           continue
        logging.info((section,ret))
        yield section, ret.get(target,[])
        ret = {}

def prep() :
    target = 'ini_list'
    ini_list = globals().get(target,[])
    logging.info("loading results {}".format(ini_list))
    ret = {}
    for section, stocks in _prep(*ini_list) :
        if section not in ret :
           ret[section] = []
        ret[section] = ret[section] + stocks
    for section in ret :
        value = sorted(ret[section])
        yield section, value

def load(value_list) :
    target = "file_list"
    file_list = globals().get(target,[])
    ret = {}
    for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
        flag, data = HELPER.is_stock_invalid(name, data)
        if flag :
            continue
        ret[name] = data
        msg = HELPER.round_values(**data)
        logging.info((name, msg))
    return ret

def _partition(*stock_list) :
    flag = len(stock_list)
    if flag > 40 :
       return HELPER_THIRDS.partition
    elif flag > 17 :
       return HELPER_HALVES.partition
    return None


def _action(stock_list) :
    partition = _partition(*stock_list)
    if partition is None :
       yield "0", stock_list
       return
    for key, data in partition(stock_list) :
        _stock_list = data.T.columns.values
        _stock_list = load(_stock_list)
        for _key, _data in _action(_stock_list) :
            _key = "{}_{}".format(key,_key)
            yield _key, _data

def action() :
    for sector, _stock_list in prep() :
        logging.debug(sector)
        logging.debug(_stock_list)
        stock_list = load(_stock_list)

        ret = {}
        for key, data in _action(stock_list) :
            logging.info(len(data))
            data = load(data)
            data = pd.DataFrame(data).T
            results = HELPER.transform(key,data)
            results = HELPER.rename_keys(sector, **results)
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
