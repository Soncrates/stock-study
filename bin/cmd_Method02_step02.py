#!/usr/bin/python

import math
import logging
import sys
import pandas as pd

from libCommon import ENVIRONMENT, INI, STOCK_TIMESERIES, log_exception
from libSharpe import HELPER
from libMonteCarlo import MonteCarlo

from libDebug import trace

'''
Method02 Step 2 - Reduction

Analysis fom step 01 revealed most promising is 0_2 for each sector.


'''
def prep(*ini_list) :
    background_list = filter(lambda x : "method02_step01" in x, ini_list)
    risk = {}
    sharpe = {}
    stock_list = {}
    for path, section, key, value in INI.loadList(*background_list) :
        if '_0_2' in key : pass
        else : continue
        if section not in risk :
           risk[section] = {}
           sharpe[section] = {}
        if key.endswith('_0_2') :
           key = key.replace(section,'')
           key = key.replace('_0_2_','')
           stock_list[section] = value
        else :
           key = key.replace(section,'')
           key = key.replace('_0_2_','')
           if 'risk' in key :
              key = key.replace('risk_','')
              risk[section][key] = value[0]
           if 'sharpe' in key :
              key = key.replace('sharpe_','')
              sharpe[section][key] = value[0]
    for key in risk :
        yield key, stock_list[key], risk[key], sharpe[key]

def load(file_list, value_list) :
    annual = MonteCarlo.YEAR()
    ret = {}
    for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
        data = HELPER.find(data['Adj Close'], period=annual.period)
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

def filterByRisk(data,risk) :
    size = len(data)
    logging.debug(data.describe())
    logging.debug(risk)
    ret = data[data.risk < risk]
    _size = len(ret)
    if _size == 0 :
        return data
    return ret

def filterBySharpe(data,sharpe) :
    size = len(data)
    logging.debug(data.describe())
    logging.debug(sharpe)
    ret = data[data.sharpe > sharpe]
    _size = len(ret)
    if _size == 0 :
        return data
    return ret

def transformMeta(**meta) :
    mean = float(meta.get('mean',0))
    std = float(meta.get('std',1))
    ret = [mean,mean+std,mean+std+std]
    logging.debug(ret)
    return ret

def filterStock(data,risk,sharpe,cap=5) : 
    size = len(data.T)
    if size <= cap : return data

    risk = transformMeta(**risk)[::-1]
    sharpe = transformMeta(**sharpe)
    ret = data.T
    for i, value in enumerate(risk) :
       _d01 = filterByRisk(ret, risk[i]) 
       size = len(_d01)
       if size <= cap : return _d01.T
       _d02 = filterBySharpe(_d01, sharpe[i])
       size = len(_d02)
       if size <= cap : return _d02.T
       ret = _d02
    size = len(ret)
    if size <= cap : return ret.T
    return ret.T

def transform(key, data) :
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
        stock_list = filterStock(stock_list,risk,sharpe,cap=20)
        results = transform(sector,stock_list)
        yield sector, results

@log_exception
def main(file_list, ini_list) : 
    env = ENVIRONMENT()
    ret = INI.init()
    for key, value in action(file_list, ini_list) :
        logging.info(value)
        INI.write_section(ret,key,**value)
    stock_ini = "{}/local/method02_step02.ini".format(env.pwd_parent)
    ret.write(open(stock_ini, 'w'))

if __name__ == '__main__' :
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')

   main(file_list,ini_list)
