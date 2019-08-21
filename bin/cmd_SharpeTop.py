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
        columns, ret = _reduce(**ret)
        yield key, columns, ret

def load(file_list, value_list) :
    annual = MonteCarlo.YEAR()
    ret = {}
    for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
        data = annual.findSharpe(data['Adj Close']) 
        #filter stocks that have less than a year
        sharpe = data.get('sharpe',0)
        if sharpe == 0 : continue
        ret[name] = data
    return ret

def _reduce(**ret) :

    if len(ret) == 0 : return [], None
    ret = pd.DataFrame(ret).T
    _len = len(ret)
    size = int(_len*.1)
    # filter riskier
    ret = ret.sort_values(['risk']).head(_len - size)
    # filter low performers
    ret = ret.sort_values(['returns']).tail(_len - size - size)
    # screen top players
    ret = ret.round(2)
    ret = ret.sort_values(['sharpe','returns']).tail(8)
    return list(ret.T.columns), ret

if __name__ == '__main__' :

   from glob import glob
   import os,sys

   pwd = os.getcwd()

   dir = pwd.replace('bin','log')
   name = sys.argv[0].split('.')[0]
   log_filename = '{}/{}.log'.format(dir,name)
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.DEBUG)

   local = pwd.replace('bin','local')
   ini_list = glob('{}/*.ini'.format(local))
   file_list = glob('{}/historical_prices/*pkl'.format(local))

   Sector_Top, Industry_Top, Fund_Top = main(file_list,ini_list)
   
   config = INI.init()
   INI.write_section(config,'Sector',**Sector_Top)
   INI.write_section(config,'Industry',**Industry_Top)
   INI.write_section(config,'Fund',**Fund_Top)
   stock_ini = "{}/nasdaq_sharpe_top.ini".format(local)
   stock_ini = "{}/prototype.ini".format(local)
   config.write(open(stock_ini, 'w'))

