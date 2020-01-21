#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI, combinations, exit_on_exception
from libFinance import STOCK_TIMESERIES
from libSharpe import PORTFOLIO

from libDebug import trace
'''
    Use MonteCarlo method to find best portfolio for risky, balanced, and not risky
'''
def prep(target, *ini_list) :
    Stock = {}
    Fund = {}
    ini_list = filter(lambda x : target in x, ini_list)
    for path, section, key, stock_list in INI.loadList(*ini_list) :
        if section == 'Sector' : config = Stock
        elif section == 'Industry' : config = Stock
        elif section == 'Fund' : config = Fund
        else : continue
        config[key] = stock_list
    stock_list = Stock.values()
    if len(stock_list) > 0 :
       Stock = reduce(lambda a, b : a + b, stock_list)
       Stock = list(set(Stock))
    fund_list = Fund.values()
    if len(fund_list) > 0 :
       Fund = reduce(lambda a, b : a + b, fund_list)
       Fund = list(set(Fund))
    return sorted(Stock), sorted(Fund)

def enrich(*ini_list) :
    ret = {}
    ini_list = filter(lambda x : 'background' in x, ini_list)
    for path, section, key, stock_list in INI.loadList(*ini_list) :
        if section == 'Sector' : pass
        elif section == 'Industry' : pass
        else : continue
        for stock in stock_list :
            if stock not in ret : ret[stock] = {}
            ret[stock][section] = key
    return ret

@exit_on_exception
@trace
def main(file_list, ini_list) :
    local_enrich = enrich(*ini_list)

    risky_stock_list, risky_fund_list = prep("risky", *ini_list)
    name_list, data_list = load(file_list, risky_stock_list)
    sharpe_risky, dev_risky = find(risky_stock_list, data_list)
    #stock_list = data_list.columns
    #for stock in stock_list :
    #    logging.info( (stock, local_enrich[stock]))

    logging.info( data_list.head(5) )
    logging.info( data_list.tail(5) )

    balanced_stock_list, fund_list = prep("balanced", *ini_list)
    name_list, data_list = load(file_list, balanced_stock_list)
    sharpe_balanced, dev_balanced = find(balanced_stock_list, data_list)

    stock_list, fund_list = prep("safe", *ini_list)
    name_list, data_list = load(file_list, stock_list)
    sharpe_safe, dev_safe = find(stock_list, data_list)

    hybrid = risky_stock_list + balanced_stock_list
    name_list, data_list = load(file_list, hybrid)
    sharpe_hybrid, dev_hybrid = find(hybrid, data_list)

    values = [sharpe_risky,dev_risky,sharpe_balanced,dev_balanced,sharpe_safe,dev_safe,sharpe_hybrid,dev_hybrid]
    keys = ["sharpe_risk","dev_risk","sharpe_balanced","dev_balanced","sharpe_safe","dev_safe", "sharpe_hybrid", "dev_hybrid"]
    ini = dict(zip(keys,values))
    return ini

def find(stock_list, data_list) :
    logging.debug(sorted(stock_list))
    for max_sharpe, min_dev in process(stock_list, data_list) : pass
    target = max_sharpe
    ret, flag = lambdaReduce(target) 
    while flag :
       subset = sorted(list(ret.T.columns)) 
       if len(subset) == 0 : break
       for target, dummy  in process(subset, data_list) : pass
       ret, flag = lambdaReduce(target) 
    subset = sorted(list(ret.T.columns)) 
    for target, dummy  in process(subset, data_list) : pass
    max_sharpe = target

    target = min_dev
    ret, flag = lambdaReduce(target) 
    while flag :
       subset = sorted(list(ret.T.columns)) 
       if len(subset) == 0 : break
       for dummy, target in process(subset, data_list) : pass
       ret, flag = lambdaReduce(target) 
    subset = sorted(list(ret.T.columns)) 
    for dummy, target in process(subset, data_list) : pass
    min_dev = target
    return max_sharpe, min_dev

def lambdaFilter(**kwargs) :
    meta = ['returns','risk', 'sharpe']
    set_meta = set(meta)
    for key in kwargs.keys() :
        ret = pd.DataFrame()
        for value in kwargs[key] :
            p = pd.DataFrame(value).T
            ret = ret.append(p)
        ret = ret.drop(columns=list(meta))
        mean = lambdaReduce(ret)
        yield key, list(mean.columns)

def lambdaReduce(ret) :
    if len(ret) == 0 :
       return ret, False
    _len = len(ret)-3
    size = int(_len*.1) + 1
    ret = pd.DataFrame(ret)
    ret.columns = ['values']
    ret = ret.sort_values(['values']).tail(_len - size)
    flag_small = ret.sort_values(['values']).values[0][0]
    flag_small = round(flag_small,2)
    flag = len(ret) > 23 or flag_small < 0.02
    logging.info((len(ret), flag_small, flag))
    return ret, flag

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
'''
def load(file_list, stock_list) :
    name_list,data_list = STOCK_TIMESERIES.read_all(file_list, stock_list)
    data_list = STOCK_TIMESERIES.flatten('Adj Close',data_list)
    data_list = data_list.fillna(0)
    return name_list, data_list
'''
def process(stock_list,data_list) :
    for subset in combinations(stock_list,4) : 
        max_sharp, min_dev = PORTFOLIO.find(data_list, stocks=stock_list, portfolios=1000, period=FINANCE.YEAR)
        yield max_sharp, min_dev
def process(stock_list,data_list) :
    meta = ['returns', 'risk', 'sharpe']
    stock_list = filter(lambda x : x not in meta, stock_list)
    max_sharp, min_dev = PORTFOLIO.find(data_list, stocks=stock_list, portfolios=25000, period=FINANCE.YEAR)
    logging.debug((max_sharpe, min_dev))
    yield max_sharpe, min_dev

if __name__ == '__main__' :

   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/method01_step02*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')
   save_file = "{}/local/method01_step03_sharpe_portfolios.ini".format(env.pwd_parent)

   ini = main(file_list,ini_list)

   config = INI.init()
   for key in ini.keys() :
       values = ini[key]
       if not isinstance(values,dict) :
          values = values.to_dict()
       INI.write_section(config,key,**values)
   config.write(open(save_file, 'w'))
