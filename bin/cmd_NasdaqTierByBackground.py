#!/usr/bin/python

import logging
import pandas as pd
from libCommon import INI, STOCK_TIMESERIES, combinations
from libNasdaq import getByNasdaq
from libMonteCarlo import MonteCarlo

'''
  There are over 6 thousand stocks on the nasdaq.
  Full analysis would take weeks
  Calculating simple sharpe ratio, 
     filtering out returns below a certain threshold, 
     filtering out risk above a certain threshold
  then reduce the list to the top 8 by sharpe.
  repeat for every subset of sector, industry, fund category
'''
def main(file_list, ini_list) :

    Sector, Industry, Category, FundFamily = getByNasdaq(*ini_list)

    high_data = {}
    balanced_data = {}
    safe_data = {}
    high_fact = {}
    balanced_fact = {}
    safe_fact = {}

    target = "Sector"
    for high_stocks, high_facts, balanced_stocks, balanced_facts, safe_stocks, safe_facts in find(file_list, **Sector) :
        high_data[target] = high_stocks
        high_fact[target] = high_facts
        balanced_data[target] = balanced_stocks
        balanced_fact[target] = balanced_facts
        safe_data[target] = safe_stocks
        safe_fact[target] = safe_facts
    target = "Industry"
    for high_stocks, high_facts, balanced_stocks, balanced_facts, safe_stocks, safe_facts in find(file_list, **Industry) :
        high_data[target] = high_stocks
        high_fact[target] = high_facts
        balanced_data[target] = balanced_stocks
        balanced_fact[target] = balanced_facts
        safe_data[target] = safe_stocks
        safe_fact[target] = safe_facts
    target = "Fund"
    for high_stocks, high_facts, balanced_stocks, balanced_facts, safe_stocks, safe_facts in find(file_list, **Category) :
        high_data[target] = high_stocks
        high_fact[target] = high_facts
        balanced_data[target] = balanced_stocks
        balanced_fact[target] = balanced_facts
        safe_data[target] = safe_stocks
        safe_fact[target] = safe_facts
    return high_data, high_fact, balanced_data, balanced_fact, safe_data, safe_fact

def find(file_list, **kwargs) :
    high_data = {}
    balanced_data = {}
    safe_data = {}
    high_fact = {}
    balanced_fact = {}
    safe_fact = {}
    for key, high_stocks, high_facts, balanced_stocks, balanced_facts, safe_stocks, safe_facts in _find(file_list, **kwargs) :
        high_data[key] = sorted(high_stocks)
        if len(high_stocks) > 0 :
           high_fact[key] = high_facts
           logging.info("high {} {} {}".format(key, sorted(high_stocks), high_facts))
        balanced_data[key] = sorted(balanced_stocks)
        if len(balanced_stocks) > 0 :
           balanced_fact[key] = balanced_facts
           logging.info("balanced {} {} {}".format(key, sorted(balanced_stocks), balanced_facts))
        safe_data[key] = sorted(safe_stocks)
        if len(safe_stocks) > 0 :
           safe_fact[key] = safe_facts
           logging.info("safe {} {} {}".format(key, sorted(safe_stocks), safe_facts))
        yield high_data, high_fact, balanced_data, balanced_fact, safe_data, safe_fact

def _find(file_list, **kwargs) :
    fact_mean = "mean [ sharpe : {sharpe}, returns : {returns}, risk : {risk} ]"
    fact_stddev = "stddev [ sharpe : {sharpe}, returns : {returns}, risk : {risk} ]"
    for key in sorted(kwargs.keys()) :
        value_list = sorted(kwargs[key])
        ret = _calculateSharpe(file_list, value_list)
        if len(ret) == 0 : continue
        ret = pd.DataFrame(ret).T
        high, balanced, safe =  findTier(ret,20) 
        high_stocks = list(high.T.columns)
        facts = high.mean().round(2)
        high_mean = fact_mean.format(**facts)
        facts = high.std().round(2)
        high_dev = fact_stddev.format(**facts)
        high_facts = high_mean + "," + high_dev
        balanced_stocks = []
        if balanced is not None :
           balanced_stocks = list(balanced.T.columns)
           facts = balanced.mean().round(2)
           balanced_mean = fact_mean.format(**facts)
           facts = balanced.std().round(2)
           balanced_dev = fact_stddev.format(**facts)
           balanced_facts = balanced_mean + "," + balanced_dev
        safe_stocks = list(safe.T.columns)
        facts = safe.mean().round(2)
        safe_mean = fact_mean.format(**facts)
        facts = safe.std().round(2)
        safe_dev = fact_stddev.format(**facts)
        safe_facts = safe_mean + "," + safe_dev
        yield key, high_stocks, high_facts, balanced_stocks, balanced_facts, safe_stocks, safe_facts

def findTier(ret,size) :
    desc = ret.describe()
    _len =  desc['len']['50%']
    ret = ret[ ret['len'] >= _len ]
    ret = ret[ ret['returns'] > 0 ]
    high = _filterSharpe(ret,size)
    safe = _filterSafe(ret,size)
    high_stock = set(high.T.columns)
    safe_stock = set(safe.T.columns)
    balanced_stock = safe_stock.intersection(high_stock)
    if len(balanced_stock) == 0 :
       return high, None, safe
    
    logging.info(balanced_stock)
    drop = safe_stock - balanced_stock
    balanced = safe.T.drop(columns=list(drop)).T
    safe = safe.T.drop(columns=list(balanced_stock)).T
    high = high.T.drop(columns=list(balanced_stock)).T
    logging.debug(high)
    logging.debug(high.describe())
    logging.debug(balanced)
    logging.debug(balanced.describe())
    logging.debug(safe)
    logging.debug(safe.describe())
    return high, balanced, safe

def _filterSharpe(ret,size) :
    while len(ret) > size : 
          desc = ret.describe()
          sharpe =  desc['sharpe']['75%']
          temp = ret[ret['sharpe'] >= sharpe]
          if len(temp) == 0 : break
          logging.debug(temp.sort_values(['returns']))
          ret = temp
    return ret
def _filterSafe(ret,size) :
    while len(ret) > size : 
          desc = ret.describe()
          risk =  desc['risk']['25%']
          temp = ret[ret['risk'] <= risk]
          if len(temp) == 0 : break
          logging.debug(temp.sort_values(['risk']))
          ret = temp
    return ret

def _calculateSharpe(file_list, value_list) :
    annual = MonteCarlo.YEAR()
    ret = {}
    for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
        logging.debug(name)
        data = annual.findSharpe(data['Adj Close'], risk_free_rate=0) 
        #filter stocks that have less than a year
        sharpe = data.get('sharpe', 0)
        if sharpe == 0 : continue
        ret[name] = data
    return ret

if __name__ == '__main__' :

   from glob import glob
   import os,sys,time

   pwd = os.getcwd()

   dir = pwd.replace('bin','log')
   name = sys.argv[0].split('.')[0]
   log_filename = '{}/{}.log'.format(dir,name)
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   local = pwd.replace('bin','local')
   ini_list = glob('{}/*.ini'.format(local))
   file_list = glob('{}/historical_prices/*pkl'.format(local))

   start = time.time()
   logging.info("started {}".format(name))
   high_data, high_fact, balanced_data, balanced_fact, safe_data, safe_fact = main(file_list,ini_list)
   end = time.time()
   elapsed = end - start
   logging.info("finished {} elapsed time : {} seconds".format(name,elapsed))
 
   config = INI.init()
   for key in high_data.keys() :
       values = high_data[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/nasdaq_high.ini".format(local)
   config.write(open(stock_ini, 'w'))

   config = INI.init()
   for key in balanced_data.keys() :
       values = balanced_data[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/nasdaq_balanced.ini".format(local)
   config.write(open(stock_ini, 'w'))

   config = INI.init()
   for key in safe_data.keys() :
       values = safe_data[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/nasdaq_safe.ini".format(local)
   config.write(open(stock_ini, 'w'))

   config = INI.init()
   for key in high_fact.keys() :
       values = high_fact[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/nasdaq_high_fact.ini".format(local)
   config.write(open(stock_ini, 'w'))

   config = INI.init()
   for key in balanced_fact.keys() :
       values = balanced_fact[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/nasdaq_balanced_fact.ini".format(local)
   config.write(open(stock_ini, 'w'))

   config = INI.init()
   for key in safe_fact.keys() :
       values = safe_fact[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/nasdaq_safe_fact.ini".format(local)
   config.write(open(stock_ini, 'w'))

