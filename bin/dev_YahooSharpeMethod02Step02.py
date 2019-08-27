#!/usr/bin/python

import logging
import pandas as pd
from libCommon import INI, STOCK_TIMESERIES, combinations
from libNasdaq import getByNasdaq
from libMonteCarlo import MonteCarlo

'''
    WARNING : in development
    Use MonteCarlo method to take subset of stocks and funds (by type)
    Determine which are risky (high profit) and which are safe (low risk)
'''
def prep(target, *ini_list) :
    Stock = {}
    Fund = {}
    ini_list = filter(lambda x : "sharpe_nasdaq" in x , ini_list)
    ini_list = filter(lambda x : "fact" not in x , ini_list)
    ini_list = filter(lambda x : target in x , ini_list)
    logging.debug(ini_list)
    for path, section, key, stock_list in INI.loadList(*ini_list) :
        if section == 'Sector' : config = Stock
        elif section == 'Industry' : config = Stock
        elif section == 'Fund' : config = Fund
        else : continue
        config[key] = stock_list
    Stock = reduce(lambda a, b : a + b, Stock.values())
    Stock = list(set(Stock))
    Fund = reduce(lambda a, b : a + b, Fund.values())
    Fund = list(set(Fund))
    return sorted(Stock), sorted(Fund)

def enrich(*ini_list) :
    ret = {}
    ini_list = filter(lambda x : "background" in x , ini_list)
    ini_list = filter(lambda x : "sector" not in x , ini_list)
    ini_list = filter(lambda x : "industry" not in x , ini_list)
    for path, section, key, stock_list in INI.loadList(*ini_list) :
        if section == 'Sector' : pass
        elif section == 'Industry' : pass
        else : continue
        for stock in stock_list :
            if stock not in ret : ret[stock] = {}
            ret[stock][section] = key
    return ret

def main(file_list, ini_list) :
    local_enrich = enrich(*ini_list)

    high_stock_list, high_fund_list = prep("high", *ini_list)
    name_list, data_list = calculateMonteCarlo(file_list, high_stock_list)
    stock_list = data_list.columns
    for stock in stock_list :
        msg = "{}, {}".format(stock, local_enrich[stock])
        logging.info(msg)

    logging.debug( data_list.head(5))
    logging.debug( data_list.tail(5))

    sharpe_high, dev_high = _main(high_stock_list, data_list)

    balanced_stock_list, fund_list = prep("balanced", *ini_list)
    name_list, data_list = calculateMonteCarlo(file_list, balanced_stock_list)
    sharpe_balanced, dev_balanced = _main(balanced_stock_list, data_list)

    safe_stock_list, fund_list = prep("safe", *ini_list)
    name_list, data_list = calculateMonteCarlo(file_list, safe_stock_list)
    sharpe_safe, dev_safe = _main(safe_stock_list, data_list)

    hybrid = high_stock_list + balanced_stock_list
    name_list, data_list = calculateMonteCarlo(file_list, hybrid)
    sharpe_high_hybrid, dev_high_hybrid = _main(hybrid, data_list)

    hybrid = safe_stock_list + balanced_stock_list
    name_list, data_list = calculateMonteCarlo(file_list, hybrid)
    sharpe_safe_hybrid, dev_safe_hybrid = _main(hybrid, data_list)

    values = [sharpe_high,dev_high,sharpe_balanced,dev_balanced,sharpe_safe,dev_safe,sharpe_high_hybrid,dev_high_hybrid,sharpe_safe_hybrid, dev_safe_hybrid]
    keys = ["sharpe_high","risk_high","sharpe_balanced","risk_balanced","sharpe_safe","risk_safe", "sharpe_high_hybrid", "risk_high_hybrid", "sharpe_safe_hybrid", "risk_safe_hybrid"]
    ini = dict(zip(keys,values))
    return ini

def _main(stock_list, data_list) :
    for max_sharpe, min_dev in _calculateMonteCarlo(stock_list, data_list) : pass
    target = max_sharpe
    ret, flag = _reduceMonteCarlo(target) 
    while flag :
       subset = sorted(list(ret.T.columns)) 
       for target, dummy  in _calculateMonteCarlo(subset, data_list) : pass
       ret, flag = _reduceMonteCarlo(target) 
    subset = sorted(list(ret.T.columns)) 
    logging.info(subset)
    for target, dummy  in _calculateMonteCarlo(subset, data_list) : pass
    max_sharpe = target

    target = min_dev
    ret, flag = _reduceMonteCarlo(target) 
    while flag :
       subset = sorted(list(ret.T.columns)) 
       for dummy, target in _calculateMonteCarlo(subset, data_list) : pass
       ret, flag = _reduceMonteCarlo(target) 
    subset = sorted(list(ret.T.columns)) 
    logging.info(subset)
    for dummy, target in _calculateMonteCarlo(subset, data_list) : pass
    min_dev = target
    return max_sharpe, min_dev

def _filterMonteCarlo(**kwargs) :
    meta = ['ret','stdev', 'sharpe']
    set_meta = set(meta)
    for key in kwargs.keys() :
        ret = pd.DataFrame()
        for value in kwargs[key] :
            p = pd.DataFrame(value).T
            ret = ret.append(p)
        ret = ret.drop(columns=list(meta))
        mean = _reduceMonteCarlo(ret)
        yield key, list(mean.columns)

def _reduceMonteCarlo(ret) :
    _len = len(ret)-3
    size = int(_len*.1) + 1
    ret = pd.DataFrame(ret)
    ret.columns = ['values']
    ret = ret.sort_values(['values']).tail(_len - size)
    flag_small = ret.sort_values(['values']).values[0][0]
    flag_small = round(flag_small,2)
    flag = len(ret) > 23 or flag_small < 0.02
    msg = "len {} small {} flag {}".format(len(ret),flag_small, flag)
    logging.info( msg )
    return ret, flag

def calculateMonteCarlo(file_list, stock_list) :
    name_list = []
    data_list = pd.DataFrame() 
    for name, stock in STOCK_TIMESERIES.read(file_list, stock_list) :
        try :
            data_list[name] = stock['Adj Close']
            name_list.append(name)
        except Exception as e : logging.error(e, exc_info=True)
        finally : pass
    return name_list, data_list
def calculateMonteCarlo(file_list, stock_list) :
    name_list,data_list = STOCK_TIMESERIES.read_all(file_list, stock_list)
    data_list = STOCK_TIMESERIES.flatten('Adj Close',data_list)
    data_list = data_list.fillna(0)
    logging.debug(data_list)
    return name_list, data_list

def _calculateMonteCarlo(stock_list,data_list) :
    annual = MonteCarlo.YEAR()
    for subset in combinations(stock_list,4) : 
        max_sharp, min_dev = annual(subset,data_list,1000) 
        yield max_sharp, min_dev
def _calculateMonteCarlo(stock_list,data_list) :
    annual = MonteCarlo.YEAR()
    #max_sharpe, min_dev = annual(stock_list,data_list,20000) 
    max_sharpe, min_dev = annual(stock_list,data_list,1000) 
    #print max_sharpe, min_dev
    yield max_sharpe, min_dev

if __name__ == '__main__' :

   from glob import glob
   import os,sys
   from libCommon import TIMER

   pwd = os.getcwd()

   dir = pwd.replace('bin','log')
   name = sys.argv[0].split('.')[0]
   log_filename = '{}/{}.log'.format(dir,name)
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.DEBUG)

   local = pwd.replace('bin','local')
   ini_list = glob('{}/*.ini'.format(local))
   file_list = glob('{}/historical_prices/*pkl'.format(local))

   elapsed = TIMER.init()
   logging.info("Started {}".format(name))
   ini = main(file_list,ini_list)
   logging.info("Finished {} Elapsed time {} ".format(name,elapsed()))

   config = INI.init()
   for key in ini.keys() :
       values = ini[key]
       if not isinstance(values,dict) :
          values = values.to_dict()
       INI.write_section(config,key,**values)
   stock_ini = "{}/sharpe_diversified_portfolios.ini".format(local)
   config.write(open(stock_ini, 'w'))
