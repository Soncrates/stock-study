#!/usr/bin/python

import pandas as pd
from libCommon import INI, STOCK_TIMESERIES, combinations
from libNasdaq import getByNasdaq
from libMonteCarlo import MonteCarlo

def main(file_list, ini_list) :

    Sector_Top = {}
    Industry_Top = {}
    Fund_Top = {}
    for section, key, top, top_data in _main(file_list, ini_list) :
        if section == "Sector" :
           config_top = Sector_Top
        elif section == "Industry" :
           config_top = Industry_Top
        elif section == "Fund" :
           config_top = Fund_Top
        else : continue
        print section, key, sorted(top)
        print top_data
        config_top[key] = sorted(top)
    return Sector_Top, Industry_Top, Fund_Top

def _main(file_list, ini_list) :
    Sector, Industry, Category, FundFamily = getByNasdaq(*ini_list)
    for key, top_columns, top_data in _process_01(file_list, **Sector) :
        yield "Sector", key, top_columns, top_data
    for key, top_columns, top_data in _process_01(file_list, **Industry) :
        yield "Industry", key, top_columns, top_data
    for key, top_columns, top_data in _process_01(file_list, **Category) :
        yield "Fund", key, top_columns, top_data

'''
  There are over 6 thousand stocks on the nasdaq.
  Full analysis would take weeks
  Calculatiing simple sharpe ration, 
     filtering out returns below a certain threshold, 
     filtering out risk above a certain threshold
  then reduce the list to the top 8 by sharpe.
  repeat for every subset of sector, industry, fund category
'''
def _process_01(file_list, **kwargs) :
    annual = MonteCarlo.YEAR()
    for key in sorted(kwargs.keys()) :
        value_list = sorted(kwargs[key])
        fund_name = []
        fund_data = {}
        for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
            ret, dev, sharpe, length = annual.single(data['Adj Close']) 
            fund_data[name] = {'returns' : ret, 'dev' : dev, 'sharpe' : sharpe, 'length' : length}

        if len(fund_data) == 0 : continue
        ret = pd.DataFrame(fund_data).T
        # filter low performers
        ret = ret[(ret['returns'] > 0.1)]
        if len(ret) == 0 : continue
        # filter riskier
        size = int(len(ret)*.9)
        if size < 8 : size == 8
        ret = ret.sort_values(['dev']).head(size)
        if len(ret) == 0 : continue
        # screen top players
        top = ret.sort_values(['sharpe','returns']).tail(8)
        if len(top) == 0 : continue
        yield key, list(top.T.columns), top

if __name__ == '__main__' :

   from glob import glob
   import os,sys

   pwd = os.getcwd()
   pwd = pwd.replace('bin','local')
   ini_list = glob('{}/*.ini'.format(pwd))
   file_list = glob('{}/historical_prices/*pkl'.format(pwd))

   Sector_Top, Industry_Top, Fund_Top = main(file_list,ini_list)
   
   config = INI.init()
   INI.write_section(config,'Sector',**Sector_Top)
   INI.write_section(config,'Industry',**Industry_Top)
   INI.write_section(config,'Fund',**Fund_Top)
   stock_ini = "{}/nasdaq_top.ini".format(pwd)
   config.write(open(stock_ini, 'w'))

