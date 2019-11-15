#!/usr/bin/python

import pandas as pd
from libCommon import INI, combinations
from libFinance import STOCK_TIMESERIES
from libMonteCarlo import MonteCarlo

'''
    WARNING : in development
    Use MonteCarlo method to take subset of stocks and funds (by type)
    Determine which are risky (high profit) and which are safe (low risk)
'''
def prep(target, *ini_list) :
    Stock = {}
    Fund = {}
    for path, section, key, stock_list in INI.loadList(*ini_list) :
        if target not in path : continue
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
    for path, section, key, stock_list in INI.loadList(*ini_list) :
        if 'background' not in path : continue
        if section == 'Sector' : pass
        elif section == 'Industry' : pass
        else : continue
        for stock in stock_list :
            if stock not in ret : ret[stock] = {}
            ret[stock][section] = key
    return ret

def main(file_list, ini_list) :
    local_enrich = enrich(*ini_list)

    risky_stock_list, risky_fund_list = prep("nasdaq_risky", *ini_list)
    name_list, data_list = calculateMonteCarlo(file_list, risky_stock_list)
    stock_list = data_list.columns
    for stock in stock_list :
        print stock, local_enrich[stock]

    print data_list.head(5)
    print data_list.tail(5)
    return

    sharpe_risky, dev_risky = _main(risky_stock_list, data_list)

    balanced_stock_list, fund_list = prep("nasdaq_balanced", *ini_list)
    name_list, data_list = calculateMonteCarlo(file_list, balanced_stock_list)
    sharpe_balanced, dev_balanced = _main(balanced_stock_list, data_list)

    stock_list, fund_list = prep("nasdaq_safe", *ini_list)
    name_list, data_list = calculateMonteCarlo(file_list, stock_list)
    sharpe_safe, dev_safe = _main(stock_list, data_list)


    hybrid = risky_stock_list + balanced_stock_list
    name_list, data_list = calculateMonteCarlo(file_list, hybrid)
    sharpe_hybrid, dev_hybrid = _main(hybrid, data_list)

    values = [sharpe_risky,dev_risky,sharpe_balanced,dev_balanced,sharpe_safe,dev_safe,sharpe_hybrid,dev_hybrid]
    keys = ["sharpe_risk","dev_risk","sharpe_balanced","dev_balanced","sharpe_safe","dev_safe", "sharpe_hybrid", "dev_hybrid"]
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
    for target, dummy  in _calculateMonteCarlo(subset, data_list) : pass
    max_sharpe = target

    target = min_dev
    ret, flag = _reduceMonteCarlo(target) 
    while flag :
       subset = sorted(list(ret.T.columns)) 
       for dummy, target in _calculateMonteCarlo(subset, data_list) : pass
       ret, flag = _reduceMonteCarlo(target) 
    subset = sorted(list(ret.T.columns)) 
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
    print len(ret), flag_small, flag
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
   pwd = pwd.replace('bin','local')
   ini_list = glob('{}/*.ini'.format(pwd))
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))

   elapsed = TIMER.init()
   ini = main(file_list,ini_list)
   for v in ini.values() : print v
   print "Elapsed time {} ".format(elapsed())
   config = INI.init()
   for key in ini.keys() :
       values = ini[key]
       if not isinstance(values,dict) :
          values = values.to_dict()
       INI.write_section(config,key,**values)
   stock_ini = "{}/nasdaq_portfolios.ini".format(pwd)
   config.write(open(stock_ini, 'w'))
