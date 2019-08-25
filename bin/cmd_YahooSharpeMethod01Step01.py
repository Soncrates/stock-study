#!/usr/bin/python

import logging
import pandas as pd
from libCommon import INI, STOCK_TIMESERIES, combinations
from libSharpe import BIN, HELPER
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
    try :
        return _main(file_list, ini_list)
    except Exception as e : 
        logging.error(e, exc_info=True)

def _main(file_list, ini_list) :

    Sector, Industry, Category, FundFamily = getByNasdaq(*ini_list)
    Sector_Top = {}
    Industry_Top = {}
    Fund_Top = {}

    for key, top_columns, top_data in find(file_list, **Sector) :
        Sector_Top[key] = sorted(top_columns)
    for key, top_columns, top_data in find(file_list, **Industry) :
        Industry_Top[key] = sorted(top_columns)
    for key, top_columns, top_data in find(file_list, **Category) :
        Fund_Top[key] = sorted(top_columns)
    return Sector_Top, Industry_Top, Fund_Top

def find(file_list, **kwargs) :
    for key in sorted(kwargs.keys()) :
        value_list = sorted(kwargs[key])
        ret = load(file_list, value_list)
        logging.info(key)
        if len(ret) == 0 : continue
        ret = pd.DataFrame(ret).T
        parition_list = make_partition(ret)
        for partition in parition_list :
            stock_list, ret = _reduce(partition)
            print stock_list
            print ret
        yield key, stock_list, ret

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

def make_partition(ret) :
    bin_list = BIN.ascending(ret,'len')
    ret_list = []
    for partition in bin_list :
        _len =  partition.describe()['len']
        if _len['mean'] - _len['min'] < _len['std']:
           ret_list.append(partition)
           continue
        ret_list += BIN.ascending(partition,'len')
    return ret_list

def _reduce(ret) :
    _len = len(ret)
    size = int(_len*.1)
    # filter riskier
    ret = ret.sort_values(['risk']).head(_len - size)
    # filter low performers
    ret = ret.sort_values(['returns']).tail(_len - size - size)
    # screen top players
    ret = ret.round(2)
    ret = ret.sort_values(['sharpe','returns']).tail(8)
    logging.info(ret)
    return list(ret.T.columns), ret

if __name__ == '__main__' :

   from glob import glob
   import os,sys
   from libCommon import TIMER

   pwd = os.getcwd()

   dir = pwd.replace('bin','log')
   name = sys.argv[0].split('.')[0]
   log_filename = '{}/{}.log'.format(dir,name)
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   local = pwd.replace('bin','local')
   ini_list = glob('{}/*.ini'.format(local))
   file_list = glob('{}/historical_prices/*pkl'.format(local))

   logging.info("started {}".format(name))
   elapsed = TIMER.init()
   Sector_Top, Industry_Top, Fund_Top = main(file_list,ini_list)
   logging.info("finished {} elapsed time : {}".format(name,elapsed()))
   
   config = INI.init()
   INI.write_section(config,'Sector',**Sector_Top)
   INI.write_section(config,'Industry',**Industry_Top)
   INI.write_section(config,'Fund',**Fund_Top)
   stock_ini = "{}/yahoo_sharpe_method1_step1.ini".format(local)
   config.write(open(stock_ini, 'w'))

