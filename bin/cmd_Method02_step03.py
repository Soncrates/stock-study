#!/usr/bin/env python

import math
import logging
import sys
import pandas as pd

from libCommon import ENVIRONMENT, INI, combinations, log_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libSharpe import PORTFOLIO

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
           value = sorted(list(set(value)))
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

class HELPER() :
    @classmethod
    def listPortfolio(cls, stock_list,data_list) :
        meta = ['returns', 'risk', 'sharpe']
        stock_list = filter(lambda x : x not in meta, stock_list)
        max_sharp, min_dev = PORTFOLIO.find(data_list, stocks=stock_list, portfolios=5, period=FINANCE.YEAR)
        logging.debug((max_sharpe, min_dev))
        yield max_sharpe, min_dev

    @classmethod
    def validateParams(cls, stock_list) :
        _size = len(stock_list)
        default_combo = 10
        num_portfolios = 5000
        if _size < default_combo :
           default_combo = _size - 1
        elif _size >= 20  :
           default_combo = _size - 4
           num_portfolios = 100
        elif _size >= 18  :
           default_combo = _size - 3
           num_portfolios = 500
        elif _size >= 15  :
           default_combo = _size - 2
           num_portfolios = 1000
        elif _size > 12  :
           num_portfolios = 2000
        logging.info(('stock count', _size, 'combos', default_combo, 'num_portfolios',num_portfolios))
        return default_combo, num_portfolios

    @classmethod
    def listPortfolio(cls, stock_list,data_list) :
        default_combo, num_portfolios = cls.validateParams(stock_list)
        ret = pd.DataFrame()
        for subset in combinations(stock_list,default_combo) :
            max_sharpe, min_dev = PORTFOLIO.find(data_list, stocks=subset, portfolios=num_portfolios, period=FINANCE.YEAR)
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
        for results in HELPER.listPortfolio(stock_list,data_list) :
            columns = results.columns.values
            _i = 0
            for index, row in results.iterrows() :
                row = row.dropna(axis = 0, how ='any') 
                row = row.to_dict()
                yield "{}_{}".format(sector,_i), row
                _i += 1

@log_exception
@trace
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
   save_file = "{}/local/method02_step03.ini".format(env.pwd_parent)

   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/method02*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')
   ini_list = filter(lambda x : "step02" in x, ini_list)

   main(file_list,ini_list,save_file)
