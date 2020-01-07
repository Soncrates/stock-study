#!/usr/bin/env python

import math
import logging
import pandas as pd

from libCommon import INI, log_exception, combinations
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libSharpe import HELPER as MONTECARLO,PORTFOLIO


from libDebug import trace, cpu

from cmd_Method03_step01 import HELPER_THIRDS, HELPER_HALVES, HELPER
from cmd_Method03_step02 import HELPER2
'''
Method 02 step 01 - Divide and Conquer

1) Partition stocks by Sector (enumerated)
2) Partition stocks by Risk into 3 (scaled)
3) Partition stocks by Sharpe into 3 (scaled)

Each sector is now divided into 9 groups :
0_0 : low risk, low sharpe
0_1 : low risk, medium sharpe
0_2 : low risk, high sharpe
1_0 : medium risk, low sharpe
1_1 : medium risk, medium sharpe
1_2 : medium risk, high sharpe
2_0 : high risk, low sharpe
2_1 : high risk, medium sharpe
2_2 : high risk, high sharpe

Write results and basic statistics data about each sub section into ini file
'''
class HELPER4() :
    @classmethod
    def listPortfolios(cls, stock_list,data_list) :
        ret = pd.DataFrame()
        for subset in combinations(stock_list,len(stock_list)-1) :
            max_sharpe, min_dev = PORTFOLIO.find(data_list, stocks=subset, portfolios=1000, period=FINANCE.YEAR)
            ret = ret.append(max_sharpe)
            ret = ret.append(min_dev)
            size = len(ret)
            if size > 1000 :
               min_risk = ret.sort_values(['risk']).head(100)
               max_sharpe = ret.sort_values(['sharpe']).tail(100)
               ret = pd.DataFrame()
               ret = ret.append(min_risk)
               ret = ret.append(max_sharpe)
        if len(ret) > 10 :
           min_risk = ret.sort_values(['risk']).head(5)
           max_sharpe = ret.sort_values(['sharpe']).tail(5)
           ret = pd.DataFrame()
           ret = ret.append(min_risk)
           ret = ret.append(max_sharpe)
           logging.info(min_risk)
           logging.info(max_sharpe)
        return ret

    @classmethod
    def data_cap(cls, *largs, **kvargs) :
        target = 'cap'
        cap = kvargs.get(target,20)
        portfolio = largs[0]
        ret = portfolio.drop(columns=['sharpe','returns','risk'])
        ret = ret.describe().T['mean'].sort_values()
        ret = ret.tail(cap)
        ret = list(ret.index)
        return ret

def _prep(*ini_list) :
    ret = {}
    target = "stocks"
    for path, section, key, value in INI.loadList(*ini_list) :
        key = key.replace(section+"_","")
        ret[key] = value
        if target not in key :
            continue
        logging.debug(key)
        prefix = key.replace(target,"")
        ret = HELPER2.transform(prefix,ret)
        if HELPER2.sharpe_cap(ret) :
           ret = {}
           continue
        logging.info((section,ret))
        yield section, ret.get(target,[])
        ret = {}

def prep() :
    target = 'ini_list'
    ini_list = globals().get(target,[])
    logging.info("loading results {}".format(ini_list))
    ret = {}
    for section, stocks in _prep(*ini_list) :
        if section not in ret :
           ret[section] = []
        ret[section] = ret[section] + stocks
    for section in ret :
        value = sorted(ret[section])
        yield section, value

def load(value_list) :
    target = "file_list"
    file_list = globals().get(target,[])
    ret = {}
    for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
        if len(data) < 7*FINANCE.YEAR :
           logging.info("{} of length {} rejected for being less than {}".format(name,len(data),7*FINANCE.YEAR))
           continue
        data = MONTECARLO.find(data['Adj Close'], period=FINANCE.YEAR)
        #filter stocks that have less than a year
        sharpe = data.get('sharpe',0)
        if sharpe == 0 :
           continue
        #filter stocks that have negative returns
        returns = data.get('returns',0)
        if returns <= 0 : 
           logging.info("{} of returns {} rejected for being unprofitable".format(name,returns))
           continue
        msg = HELPER.round_values(**data)
        logging.info((name, msg))
        ret[name] = data
    return ret

def _action(**kvargs) : 
    for sector, stocks in prep() :
        data_list = load(stocks)
        portfolio = HELPER4.listPortfolios(stocks,data_list)
        ret = HELPER4.data_cap(portfolio,**kvargs)
        yield sector, ret

def action() :
    for sector, stocks in _action() :
        data_list = load(stocks)
        portfolio = HELPER4.listPortfolios(stocks,data_list)
        ret = HELPER4.data_cap(portfolio,cap=10)
        portfolio_list = HELPER4.listPortfolios(ret,data_list)
        logging.info(portfolio_list)
        for index, portfolio in portfolio_list.iterrows():
            portfolio = portfolio.dropna(how="all")
            logging.info(portfolio)
            yield "{}_{}".format(sector,index), portfolio

@log_exception
@trace
def main(save_file) : 
    ret = INI.init()
    for key, value in action() :
        logging.info(value)
        INI.write_section(ret,key,**value)
    ret.write(open(save_file, 'w'))
    logging.info("results saved to {}".format(save_file))

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   file_list = env.list_filenames('local/historical_prices/*pkl')
   save_file = "{}/local/method03_step04.ini".format(env.pwd_parent)
   ini_list = env.list_filenames('local/*.ini')
   ini_list = filter(lambda x : 'method03' in x, ini_list)
   ini_list = filter(lambda x : 'step03' in x, ini_list)

   main(save_file)
