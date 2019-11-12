#!/usr/bin/python

import math
import logging
import sys
import pandas as pd

from libCommon import ENVIRONMENT, INI, STOCK_TIMESERIES, combinations, log_exception
from libSharpe import HELPER
from libMonteCarlo import MonteCarlo

from libDebug import trace

'''
Method02 Step 3 - Monte Carlo


1) load results from step 02 (ini file)
2) Calculate Monte Carlo per sector

Write results and basic statistics data about each section into ini file
'''
def prep(*ini_list) :
    risk = {}
    sharpe = {}
    stock_list = {}
    for path, section, key, value in INI.loadList(*ini_list) :
        if section not in risk :
           risk[section] = {}
           sharpe[section] = {}
        if section == key :
           stock_list[key] = value
        key = key.replace(section,'')
        if 'risk' in key :
           key = key.replace('risk_','')
           key = key.replace('_','')
           risk[section][key] = value[0]
        elif 'sharpe' in key :
           key = key.replace('sharpe_','')
           key = key.replace('_','')
           sharpe[section][key] = value[0]
    for key in risk :
        yield key, stock_list[key], risk[key], sharpe[key]

def load(file_list, stock_list) :
    name_list = []
    data_list = pd.DataFrame()
    if len(stock_list) == 0 :
       return name_list, data_list

    for name, stock in STOCK_TIMESERIES.read(file_list, stock_list) :
        try :
            data_list[name] = stock['Adj Close']
            name_list.append(name)
        except Exception as e : logging.error(e, exc_info=True)
        finally : pass
    return name_list, data_list

def process(d) :
    #locate position of portfolio with highest Sharpe Ratio
    _sharpe = d['sharpe'].idxmax()
    #locate positon of portfolio with minimum risk
    _risk = d['risk'].idxmin()
    return d.iloc[_sharpe], d.iloc[_risk]

def filterByRisk(data,risk) :
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

def filterBySharpe(data,sharpe) :
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

def calculateMonteCarlo(stock_list,data_list) :
    meta = ['returns', 'risk', 'sharpe']
    stock_list = filter(lambda x : x not in meta, stock_list)
    annual = MonteCarlo.YEAR()
    max_sharpe, min_dev = annual(stock_list,data_list,5)
    logging.debug((max_sharpe, min_dev))
    yield max_sharpe, min_dev

def calculateMonteCarlo(stock_list,data_list) :
    _size = len(stock_list)
    default_combo = 10
    default_montecarlo = 5000
    if _size < default_combo :
       default_combo = _size - 1
    elif _size >= 18  :
       default_combo = 14
       default_montecarlo = 500
    elif _size >= 15  :
       default_combo = 12
       default_montecarlo = 1000
    elif _size > 12  :
       default_montecarlo = 2000

    annual = MonteCarlo.YEAR()
    ret = pd.DataFrame()
    for subset in combinations(stock_list,default_combo) :
        max_sharpe, min_dev = annual(subset,data_list,default_montecarlo)
        ret = ret.append(max_sharpe)
        ret = ret.append(min_dev)
        size = len(ret)
        if size > 10000 :
           min_risk = ret.sort_values(['risk']).head(100)
           max_sharpe = ret.sort_values(['sharpe']).tail(100)
           ret = pd.DataFrame()
           ret = ret.append(min_risk)
           ret = ret.append(max_sharpe)
    if len(ret) > 100 :
       min_risk = ret.sort_values(['risk']).head(50)
       max_sharpe = ret.sort_values(['sharpe']).tail(50)
       ret = pd.DataFrame()
       ret = ret.append(min_risk)
       ret = ret.append(max_sharpe)
    yield ret

def action(file_list, ini_list) : 
    for sector, _stock_list, risk, sharpe in prep(*ini_list) :
        logging.info(sector)
        logging.info(_stock_list)
        logging.debug(risk)
        logging.debug(sharpe)
        stock_list, data_list = load(file_list,_stock_list)
        for results in calculateMonteCarlo(stock_list,data_list) :
            columns = results.columns.values
            _i = 0
            for index, row in results.iterrows() :
                row = row.dropna(axis = 0, how ='any') 
                row = row.to_dict()
                yield "{}_{}".format(sector,_i), row
                _i += 1

@log_exception
def main(file_list, ini_list,save_file) : 
    ret = INI.init()
    for key, value in action(file_list, ini_list) :
        logging.info(value)
        INI.write_section(ret,key,**value)
    ret.write(open(save_file, 'w'))

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
   save_file = "{}/local/method02_step03.ini".format(env.pwd_parent)
   ini_list = filter(lambda x : "method02_step02" in x, ini_list)

   main(file_list,ini_list,save_file)
