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
    Sector_Top = {}
    Industry_Top = {}
    Fund_Top = {}
    Sector_Fact = {}
    Industry_Fact = {}
    Fund_Fact = {}

    for key, stock_list, facts in find(file_list, **Sector) :
        Sector_Top[key] = sorted(stock_list)
        Sector_Fact[key] = facts
        logging.info("{} {}".format(sorted(stock_list), facts))
    for key, stock_list, facts in find(file_list, **Industry) :
        Industry_Top[key] = sorted(stock_list)
        Industry_Fact[key] = facts
        logging.info("{} {}".format(sorted(stock_list), facts))
    for key, stock_list, facts in find(file_list, **Category) :
        Fund_Top[key] = sorted(stock_list)
        Fund_Fact[key] = facts
        logging.info("{} {}".format(sorted(stock_list), facts))
    return Sector_Top, Industry_Top, Fund_Top, Sector_Fact, Industry_Fact, Fund_Fact

def find(file_list, **kwargs) :
    for key in sorted(kwargs.keys()) :
        value_list = sorted(kwargs[key])
        ret = _calculateSharpe(file_list, value_list)
        if len(ret) == 0 : continue
        ret = pd.DataFrame(ret).T
        high, balanced, safe =  _filter(ret,20) 
        facts = safe.mean().round(2)
        facts = "sharpe : {sharpe}, returns : {returns}, risk : {risk}".format(**facts)
        columns = list(safe.T.columns)
        yield key, columns, facts
        break

def _filter(ret,size) :
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
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.DEBUG)

   local = pwd.replace('bin','local')
   ini_list = glob('{}/*.ini'.format(local))
   file_list = glob('{}/historical_prices/*pkl'.format(local))

   start = time.time()
   logging.info("started {}".format(name))
   Sector_Top, Industry_Top, Fund_Top, Sector_Facts, Industry_Facts, Fund_Facts = main(file_list,ini_list)
   end = time.time()
   elapsed = end - start
   logging.info("finished {} elapsed time : {} seconds".format(name,elapsed))

   config = INI.init()
   INI.write_section(config,'Sector',**Sector_Top)
   INI.write_section(config,'Industry',**Industry_Top)
   INI.write_section(config,'Fund',**Fund_Top)
   stock_ini = "{}/nasdaq_prototype.ini".format(pwd)
   config.write(open(stock_ini, 'w'))

   config = INI.init()
   INI.write_section(config,'Sector',**Sector_Facts)
   INI.write_section(config,'Industry',**Industry_Facts)
   INI.write_section(config,'Fund',**Fund_Facts)
   stock_ini = "{}/nasdaq_facts.ini".format(pwd)
   config.write(open(stock_ini, 'w'))
