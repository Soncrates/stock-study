#!/usr/bin/env python

import math
import logging
import sys
import pandas as pd

from libCommon import ENVIRONMENT, INI, log_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libSharpe import HELPER as MONTECARLO

from libDebug import trace

'''
Method02 Step 2 - Reduction

Analysis from step 01 revealed most promising is 0_2 for each sector.

1) load results from step 01 (ini file)
2) filter results to 0_2 for each sector
3) If the stock count is less than 20 return
4) Else, filter by risk, then filter by sharpe until the stock count is 20 or less

Write results and basic statistics data about each section into ini file
'''
def prep(*ini_list) :
    target_sector = 'target_sector'
    target_sector = globals().get(target_sector,'_')
    risk = {}
    sharpe = {}
    stock_list = {}
    for path, section, key, value in INI.loadList(*ini_list) :
        if target_sector in key : pass
        else : continue
        if section not in risk :
           risk[section] = {}
           sharpe[section] = {}
        if key.endswith(target_sector) :
           key = key.replace(section,'')
           key = key.replace(target_sector,'')
           stock_list[section] = value
        else :
           key = key.replace(section,'')
           key = key.replace(target_sector,'')
           if 'risk' in key :
              key = key.replace('risk_','')
              key = key.replace('_','')
              risk[section][key] = value[0]
           if 'sharpe' in key :
              key = key.replace('sharpe_','')
              key = key.replace('_','')
              sharpe[section][key] = value[0]
    for key in risk :
        yield key, stock_list[key], risk[key], sharpe[key]

def load(file_list, value_list) :
    ret = {}
    for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
        data = MONTECARLO.find(data['Adj Close'], period=FINANCE.YEAR)
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

class HELPER() :
    @classmethod
    def filterByRisk(cls, data,risk) :
        size = len(data)
        logging.debug(data.describe())
        logging.debug(risk)
        ret = data[data.risk < risk]
        _size = len(ret)
        logging.debug(size)
        logging.debug(_size)
        if _size == 0 :
            return data
        return ret

    @classmethod
    def filterBySharpe(cls, data,sharpe) :
        size = len(data)
        logging.debug(data.describe())
        logging.debug(sharpe)
        ret = data[data.sharpe > sharpe]
        _size = len(ret)
        logging.debug(size)
        logging.debug(_size)
        if _size == 0 :
            return data
        return ret

    @classmethod
    def transformMeta(cls, **meta) :
        mean = float(meta.get('mean',0))
        std = float(meta.get('std',1))
        ret = [mean,mean+std,mean+std+std]
        logging.debug(ret)
        return ret

    @classmethod
    def filterStock(cls, data,risk,sharpe,cap=5) : 
        size = len(data.T)
        if size <= cap : return data

        risk = cls.transformMeta(**risk)[::-1]
        sharpe = cls.transformMeta(**sharpe)
        ret = data.T
        for i, value in enumerate(risk) :
           _d01 = cls.filterByRisk(ret, risk[i]) 
           size = len(_d01)
           if size <= cap : return _d01.T
           _d02 = cls.filterBySharpe(_d01, sharpe[i])
           size = len(_d02)
           if size <= cap : return _d02.T
           ret = _d02
        size = len(ret)
        if size <= cap : return ret.T
        return ret.T

    @classmethod
    def transform(cls, key, data) :
        stock_list = data.columns.values
        stock_list = sorted(list(stock_list))
        ret = { key : stock_list } 

        column_list = ['mean', 'std', 'min', 'max']

        temp = data.T.describe()['sharpe'] 
        value_list = map(lambda key : round(temp[key],2), column_list)
        key_list = map(lambda k : '{}_sharpe_{}'.format(key,k), column_list)
        temp = dict(zip(key_list,value_list))
        ret.update(temp)

        temp = data.T.describe()['risk'] 
        value_list = map(lambda key : round(temp[key],2), column_list)
        key_list = map(lambda k : '{}_risk_{}'.format(key,k), column_list)
        temp = dict(zip(key_list,value_list))
        ret.update(temp)
        return ret

def action(file_list, ini_list) : 
    for sector, _stock_list, risk, sharpe in prep(*ini_list) :
        logging.debug(sector)
        logging.debug(_stock_list)
        logging.debug(risk)
        logging.debug(sharpe)
        stock_list = load(file_list,_stock_list)
        stock_list = pd.DataFrame(stock_list)
        stock_list = HELPER.filterStock(stock_list,risk,sharpe,cap=20)
        results = HELPER.transform(sector,stock_list)
        yield sector, results

@log_exception
@trace
def main(file_list, ini_list,save_file) : 
    logging.info("loading results {}".format(ini_list))
    ret = INI.init()
    for key, value in action(file_list, ini_list) :
        logging.info(value)
        INI.write_section(ret,key,**value)
    ret.write(open(save_file, 'w'))
    logging.info("Results saved to {}".format(save_file))

if __name__ == '__main__' :
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   ini_list = env.list_filenames('local/method02*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')
   save_file = "{}/local/method02_step02.ini".format(env.pwd_parent)
   target_sector = '_0_2'
   #target_sector = '_1_2'
   ini_list = filter(lambda x : "step01" in x, ini_list)

   main(file_list,ini_list,save_file)
