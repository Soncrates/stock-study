#!/usr/bin/python

import logging
import pandas as pd
from libSharpe import RISK, SHARPE, RETURNS, BIN
from libCommon import INI, STOCK_TIMESERIES, combinations
from libNasdaq import getByNasdaq
from libMonteCarlo import MonteCarlo

'''
WARNING : in development

Partition stocks by Sector, Industry, and Fund Category
Partition by High and possibly risky performance, non risky performance, and balanced performance
balanced is set theory union of risky and not risky
balanced is removed from risky and not risky
montecarlo is executed on each partition
'''
def main(file_list, ini_list) :
    try :
        return _main(file_list, ini_list)
    except Exception as e : 
        logging.error(e, exc_info=True)

def _main(file_list, ini_list) :

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
    fact_mean = "sharpe : {sharpe}, returns : {returns}, risk : {risk}"
    fact_stddev = "sharpe : {sharpe}, returns : {returns}, risk : {risk}"
    for key in sorted(kwargs.keys()) :
        value_list = sorted(kwargs[key])
        logging.debug(value_list)
        ret = _calculateSharpe(file_list, value_list)
        if len(ret) == 0 : continue
        ret = pd.DataFrame(ret).T
        high, balanced, safe =  findTier(ret,15) 
        high_stocks = list(high.T.columns)
        facts = high.mean().round(2)
        high_mean = fact_mean.format(**facts)
        facts = high.std().round(2)
        high_dev = fact_stddev.format(**facts)
        high_facts = "mean { " + high_mean + " }, { stddev " + high_dev + " }"
        balanced_stocks = []
        if balanced is not None :
           balanced_stocks = list(balanced.T.columns)
           facts = balanced.mean().round(2)
           balanced_mean = fact_mean.format(**facts)
           facts = balanced.std().round(2)
           balanced_dev = fact_stddev.format(**facts)
           balanced_facts = "mean { " + balanced_mean + " }, { stddev " + balanced_dev + " }"
        safe_stocks = list(safe.T.columns)
        facts = safe.mean().round(2)
        safe_mean = fact_mean.format(**facts)
        facts = safe.std().round(2)
        safe_dev = fact_stddev.format(**facts)
        safe_facts = "mean { " + safe_mean + " }, { stddev " + safe_dev + " }"
        yield key, high_stocks, high_facts, balanced_stocks, balanced_facts, safe_stocks, safe_facts

def findTier(ret,size) :
    ret = ret[ ret['returns'] > 0 ]
    desc = ret.describe()
    _len =  desc['len']['50%']
    ret = ret[ ret['len'] >= _len ]
    temp = BIN.ascending(ret,'risk')
    logging.info(temp)
    high, balanced, safe = METHOD_2.process(ret,size)
    logging.debug(high)
    logging.debug(high.describe())
    logging.debug(balanced)
    logging.debug(balanced.describe())
    logging.debug(safe)
    logging.debug(safe.describe())
    return high, balanced, safe

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

class METHOD_1 :
      @staticmethod
      def process(ret,size) :
          high = METHOD_1._filterSharpe(ret,size)
          safe = METHOD_1._filterSafe(ret,size)
          high_stock = set(high.T.columns)
          safe_stock = set(safe.T.columns)
          balanced_stock = safe_stock.intersection(high_stock)
          drop = safe_stock - balanced_stock
          balanced = safe.T.drop(columns=list(drop)).T
          safe = safe.T.drop(columns=list(balanced_stock)).T
          high = high.T.drop(columns=list(balanced_stock)).T
          return high, balanced, safe

      @staticmethod
      def _filterSharpe(ret,size) :
          while len(ret) > size : 
                temp = SHARPE.cut(ret)
                if len(temp) == 0 : break
                ret = temp
          return ret
      @staticmethod
      def _filterSafe(ret,size) :
          while len(ret) > size : 
                temp = RISK.cut(ret)
                if len(temp) == 0 : break
                ret = temp
          return ret

class METHOD_2 :
      @staticmethod
      def process(ret,size) :
          _len = len(ret)
          logging.debug(_len)
          _bracket = int(_len*0.1)
          temp = RISK.shave(ret,_len - _bracket)
          temp = SHARPE.shave(temp,_len - _bracket - _bracket)
          logging.debug(temp)
          risk = BIN.ascending(temp,'risk')
          sharpe = BIN.descending(temp,'sharpe')
          logging.debug(risk)
          logging.debug(sharpe)
          high_stock = set(high.T.columns)
          safe_stock = set(safe.T.columns)
          balanced_stock = safe_stock.intersection(high_stock)
          drop = safe_stock - balanced_stock
          balanced = safe.T.drop(columns=list(drop)).T
          safe = safe.T.drop(columns=list(balanced_stock)).T
          high = high.T.drop(columns=list(balanced_stock)).T
          return high, balanced, safe
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
   logging.info("started {}".format(name))
   high_data, high_fact, balanced_data, balanced_fact, safe_data, safe_fact = main(file_list,ini_list)
   logging.info("finished {} elapsed time : {}".format(name,elapsed()))
 
   config = INI.init()
   for key in high_data.keys() :
       values = high_data[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/sharpe_nasdaq_high.ini".format(local)
   config.write(open(stock_ini, 'w'))

   config = INI.init()
   for key in balanced_data.keys() :
       values = balanced_data[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/sharpe_nasdaq_balanced.ini".format(local)
   config.write(open(stock_ini, 'w'))

   config = INI.init()
   for key in safe_data.keys() :
       values = safe_data[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/sharpe_nasdaq_safe.ini".format(local)
   config.write(open(stock_ini, 'w'))

   config = INI.init()
   for key in high_fact.keys() :
       values = high_fact[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/sharpe_nasdaq_high_fact.ini".format(local)
   config.write(open(stock_ini, 'w'))

   config = INI.init()
   for key in balanced_fact.keys() :
       values = balanced_fact[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/sharpe_nasdaq_balanced_fact.ini".format(local)
   config.write(open(stock_ini, 'w'))

   config = INI.init()
   for key in safe_fact.keys() :
       values = safe_fact[key]
       INI.write_section(config,key,**values)
   stock_ini = "{}/sharpe_nasdaq_safe_fact.ini".format(local)
   config.write(open(stock_ini, 'w'))

