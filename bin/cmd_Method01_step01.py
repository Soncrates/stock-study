#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI, combinations, log_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libSharpe import BIN, HELPER
from libNasdaq import getByNasdaq
from libDebug import trace

'''
Parition stocks by Sector Industry Fund
Filter each Partition to top 8 performers based on Sharpe and Risk
'''

@log_exception
@trace
def main(file_list, ini_list) :

    Sector, Industry, Category, FundFamily = getByNasdaq(*ini_list)
    Sector_Top = {}
    Industry_Top = {}
    Fund_Top = {}

    for key, top_columns, top_data in find(file_list, **Sector) :
        Sector_Top[key] = sorted(top_columns)
    for key, top_columns, top_data in find(file_list, **Industry) :
        Industry_Top[key] = sorted(top_columns)
    for key, top_columns, top_data in find(file_list, **Category) :
        Fund_Top[key] = sorted(top_columns)
    return Sector_Top, Industry_Top, Fund_Top

def find(file_list, **kwargs) :
    for key in sorted(kwargs.keys()) :
        value_list = sorted(kwargs[key])
        ret = load(file_list, value_list)
        logging.info(key)
        if len(ret) == 0 : continue
        ret = pd.DataFrame(ret).T
        parition_list = make_partition(ret)
        for partition in parition_list :
            stock_list, ret = _reduce(partition)
            print stock_list
            print ret
        yield key, stock_list, ret

def load(file_list, value_list) :
    ret = {}
    for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
        data = HELPER.find(data['Adj Close'], period=FINANCE.YEAR) 
        #filter stocks that have less than a year
        sharpe = data.get('sharpe',0)
        if sharpe == 0 : continue
        #filter stocks that have negative returns
        returns = data.get('returns',0)
        if returns <= 0 : continue
        key_list = data.keys()
        value_list = map(lambda x : data[x], key_list)
        value_list = map(lambda x : round(x,2), value_list)
        msg = dict(zip(key_list,value_list))
        logging.info((name, msg))
        ret[name] = data
    return ret

def make_partition(ret) :
    bin_list = BIN.ascending(ret,'len')
    ret_list = []
    for partition in bin_list :
        _len =  partition.describe()['len']
        if _len['mean'] - _len['min'] < _len['std']:
           ret_list.append(partition)
           continue
        ret_list += BIN.ascending(partition,'len')
    return ret_list

def _reduce(ret) :
    _len = len(ret)
    size = int(_len*.1)
    # filter riskier
    ret = ret.sort_values(['risk']).head(_len - size)
    # filter low performers
    ret = ret.sort_values(['returns']).tail(_len - size - size)
    # screen top players
    ret = ret.round(2)
    ret = ret.sort_values(['sharpe','returns']).tail(8)
    logging.info(ret)
    return list(ret.T.columns), ret

if __name__ == '__main__' :

   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')
   Sector_Top, Industry_Top, Fund_Top = main(file_list,ini_list)
   
   config = INI.init()
   INI.write_section(config,'Sector',**Sector_Top)
   INI.write_section(config,'Industry',**Industry_Top)
   INI.write_section(config,'Fund',**Fund_Top)
   stock_ini = "{}/local/method01_step01_sharpe.ini".format(env.pwd_parent)
   config.write(open(stock_ini, 'w'))

