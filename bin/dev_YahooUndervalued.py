#!/usr/bin/python

import pandas as pd
from libCommon import INI, combinations
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libSharpe import HELPER as MONTECARLO
from libNasdaq import getByNasdaq

'''
   WARNING : in development

   Warren Buffet made a career of finding under values stocks
'''

def main(file_list, ini_list) :

    Sector, Industry, Category, FundFamily = getByNasdaq(*ini_list)
    Sector_Top = {}
    Industry_Top = {}
    Fund_Top = {}

    for key, top_columns, top_data in load(file_list, **Sector) :
        Sector_Top[key] = sorted(top_columns)
    for key, top_columns, top_data in load(file_list, **Industry) :
        Industry_Top[key] = sorted(top_columns)
    for key, top_columns, top_data in load(file_list, **Category) :
        Fund_Top[key] = sorted(top_columns)
    return Sector_Top, Industry_Top, Fund_Top

def load(file_list, **kwargs) :
    for key in sorted(kwargs.keys()) :
        value_list = sorted(kwargs[key])
        ret = process(file_list, value_list)
        columns, ret = lambdaFilter(**ret)
        yield key, columns, ret

def process(file_list, value_list) :
    ret = {}
    for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
        data.sort_index(inplace=True)
        data = data['Adj Close']
        data = MONTECARLO.find(data, risk_free_rate=0.02, period=FINANCE.YEAR, span=0)

        # filter stocks that have less than a year
        sharpe = data.get('sharpe',0)
        if sharpe == 0 : continue
        ret[name] = data
    return ret

def lambdaFilter(**ret) :

    if len(ret) == 0 : return [], None
    ret = pd.DataFrame(ret).T
    # screen potential under valued
    ret = ret[ret['returns'] < 0.15 ]
    if len(ret) == 0 : return [], None

    _len = len(ret)
    size = int(_len*.1)
    # filter riskier
    ret = ret.sort_values(['risk']).head(_len - size)
    return list(ret.T.columns), ret

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
   stock_ini = "{}/yahoo_sharpe_undervalued.ini".format(pwd)
   config.write(open(stock_ini, 'w'))

