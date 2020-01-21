#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI, combinations, exit_on_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libSharpe import PORTFOLIO

from libDebug import trace
'''
    Use MonteCarlo method to take subset of stocks and funds (by type)
    Determine which are risky (high profit) and which are safe (low risk)
'''
def prep(*ini_list) :
    Sector = {}
    Industry = {}
    Category = {}
    for path, section, key, stock in INI.loadList(*ini_list) :
        if section == 'Sector' : config = Sector
        elif section == 'Industry' : config = Industry
        elif section == 'Fund' : config = Category
        else : continue
        config[key] = stock
    return Sector, Industry, Category

@exit_on_exception
@trace
def main(file_list, ini_list) :
    Sector, Industry, Category = prep(*ini_list)

    ret_sect, dev_sect, ret_sect_list, dev_sect_list = lambdaBin(file_list, Sector)
    ret_ind, dev_ind, ret_ind_list, dev_int_list = lambdaBin(file_list, Industry)
    ret_cat, dev_cat, ret_cat_list, dev_cat_list = lambdaBin(file_list, Category)
    
    risky_data = { 'Sector' : {}, 'Industry' : {}, 'Fund' : {} }
    balanced_data = { 'Sector' : {}, 'Industry' : {}, 'Fund' : {} }
    safe_data = { 'Sector' : {}, 'Industry' : {}, 'Fund' : {} }
    target = 'Sector'
    for key, risky, balanced, safe in  lambdaSort(ret_sect, dev_sect) :
        risky_data[target][key] = risky
        balanced_data[target][key] = balanced
        safe_data[target][key] = safe
        logging.info( "Sector {} risky : {} balanced : {} safe : {}".format(key,risky,balanced,safe))
    target = 'Fund'
    for key, risky, balanced, safe in  lambdaSort(ret_cat, dev_cat) :
        risky_data[target][key] = risky
        balanced_data[target][key] = balanced
        safe_data[target][key] = safe
        logging.info("Fund {} risky : {} balanced : {} safe : {}".format(key,risky,balanced,safe))
    target = 'Industry'
    for key, risky, balanced, safe in  lambdaSort(ret_ind, dev_ind) :
        risky_data[target][key] = risky
        balanced_data[target][key] = balanced
        safe_data[target][key] = safe
        logging.info("Industry {} risky : {} balanced : {} safe : {}".format(key,risky,balanced,safe))
    return risky_data, balanced_data, safe_data

def lambdaSort(ret_good, dev_good) :
    risky_data = {}
    for key, stock_list in lambdaFilter(**ret_good) :
        risky_data[key] = stock_list
    safe_data = {}
    for key, stock_list in lambdaFilter(**dev_good) :
        safe_data[key] = stock_list
    key_list = risky_data.keys() + safe_data.keys()
    key_list = list(set(key_list))
    for key in key_list :
        risky = set(risky_data.get(key,[]))
        safe = set(safe_data.get(key,[]))
        balanced = risky.intersection(safe)
        risky = risky - balanced
        safe = safe - balanced
        yield key, list(risky), list(balanced), list(safe)

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
    #size = int(len(ret)*.9)
    #if size < 20 : size = 20
    #ret = ret.sort_values(['risk','sharpe']).head(size)
    ret = ret.fillna(0)
    ret = ret.mean()
    ret = pd.DataFrame(ret)
    ret.columns = ['mean']
    ret = ret[(ret['mean'] > 0.05)]
    ret = ret.T
    return ret

def lambdaBin(file_list, kwargs) :
    logging.debug(kwargs)
    ret_max = {}
    ret_3 = {}
    ret_2 = {}
    ret_min = {}
    dev_max = {}
    dev_3 = {}
    dev_2 = {}
    dev_min = {}
    for key in kwargs.keys() :
        stock_list = sorted(kwargs[key])
        logging.info((key, stock_list))
        name_list, data_list = load(file_list, stock_list)
        for max_sharpe, min_dev in process(name_list, data_list) :
            cur_sharpe = ret_min
            cur_dev = dev_min
            if max_sharpe['sharpe'] > 2.5 :
               cur_sharpe = ret_max
            elif max_sharpe['sharpe'] > 2 :
               cur_sharpe = ret_3
            elif max_sharpe['sharpe'] > 1 :
               cur_sharpe = ret_2
            if min_dev['risk'] < 0.15 :
               cur_dev = dev_max
            elif min_dev['risk'] < 0.25 :
               cur_dev = dev_3
            elif min_dev['risk'] < 0.5 :
               cur_dev = dev_2

            if key not in cur_sharpe.keys() : 
               cur_sharpe[key] = []
            if key not in cur_dev.keys() : 
               cur_dev[key] = []

            cur_sharpe[key].append(max_sharpe)
            cur_dev[key].append(min_dev)
    returns_list = [ret_max, ret_3, ret_2, ret_min]
    devs_list = [dev_max, dev_3, dev_2, dev_min]
    returns_list = filter(lambda x : len(x) > 0, returns_list)
    devs_list = filter(lambda x : len(x) > 0, devs_list)
    logging.info(returns_list[0])
    logging.info(len(returns_list[0]))
    logging.info(len(returns_list))
    logging.info(devs_list[0])
    return returns_list[0], devs_list[0], returns_list, devs_list

def load(file_list, stock_list) :
    name_list = []
    data_list = pd.DataFrame() 
    for name, stock in STOCK_TIMESERIES.read(file_list, stock_list) :
        try :
            data_list[name] = stock['Adj Close']
            name_list.append(name)
        except Exception as e : logging.error(e, exc_info=True)
        finally : pass
    return name_list, data_list
def process(stock_list,data_list) :
    for subset in combinations(stock_list,4) : 
        max_sharp, min_dev = PORTFOLIO.find(data_list, stocks=stock_list, portfolios=1000, period=FINANCE.YEAR)
        yield max_sharp, min_dev
def process(stock_list,data_list) :
    max_sharp, min_dev = PORTFOLIO.find(data_list, stocks=stock_list, portfolios=25000, period=FINANCE.YEAR)
    logging.debug((max_sharp, min_dev))
    yield max_sharp, min_dev

if __name__ == '__main__' :

   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/method01*.ini')
   ini_list = filter(lambda x : 'step01' in x, ini_list)
   file_list = env.list_filenames('local/historical_prices/*pkl')

   risky_data, balanced_data, safe_data = main(file_list,ini_list)

   config = INI.init()
   for key in risky_data.keys() :
       values = risky_data[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/local/method01_step02_sharpe_risky.ini".format(env.pwd_parent)
   config.write(open(stock_ini, 'w'))

   config = INI.init()
   for key in balanced_data.keys() :
       values = balanced_data[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/local/method01_step02_sharpe_balanced.ini".format(env.pwd_parent)
   config.write(open(stock_ini, 'w'))

   config = INI.init()
   for key in safe_data.keys() :
       values = safe_data[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/local/method01_step02_sharpe_safe.ini".format(env.pwd_parent)
   config.write(open(stock_ini, 'w'))
