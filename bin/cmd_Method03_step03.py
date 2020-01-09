#!/usr/bin/env python

#import math
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
class HELPER3() :
    @classmethod
    def truncateStocks(cls, ret, size) :
        if len(ret) <= size :
           return ret
        data = load(ret)
        data = pd.DataFrame(data).T
        data = data.sort_values(['risk']).head(size)
        ret = data.T.columns.values
        return ret

    @classmethod
    def listStocks(cls, stock_list) :
        count = len(stock_list)-1
        while count > 2 :
            data = load(stock_list)
            stock_list = sorted(data.keys())
            for subset in combinations(stock_list,count) :
                logging.info((len(subset),sorted(subset)))
                yield data, sorted(subset)
            stock_list = cls.truncateStocks(stock_list,count)
            count = len(stock_list)-1

    @classmethod
    def listPortfolios(cls, stock_list,data_list) :
        stock_list = cls.truncateStocks(stock_list,20)
        ret = pd.DataFrame()
        for data, subset in cls.listStocks(stock_list) :
            max_sharpe, min_dev = PORTFOLIO.find(data, stocks=subset, portfolios=1000, period=FINANCE.YEAR)
            ret = ret.append(max_sharpe)
            ret = ret.append(min_dev)
            size = len(ret)
            if size > 1000 :
               min_risk = ret.sort_values(['risk']).head(50)
               max_sharpe = ret.sort_values(['sharpe']).tail(50)
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
    def refinePortfolio(cls, ret, portfolio_list) :
        logging.info(portfolio_list)
        if ret is None :
           ret = pd.DataFrame()
        for stock_list in portfolio_list :
            data_list = load(stock_list)
            stock_list = sorted(data_list.keys())
            max_sharpe, min_risk = PORTFOLIO.find(data_list, stocks=stock_list, portfolios=10000, period=FINANCE.YEAR)
            ret = ret.append(min_risk)
            ret = ret.append(max_sharpe)
        if len(ret) > 6 :
           min_risk = ret.sort_values(['risk']).head(3)
           max_sharpe = ret.sort_values(['sharpe']).tail(3)
           ret = pd.DataFrame()
           ret = ret.append(min_risk)
           ret = ret.append(max_sharpe)
        return ret

    @classmethod
    def is_stock_invalid(cls, name, data) :
        ret = MONTECARLO.find(data['Adj Close'], period=FINANCE.YEAR)
        sharpe = ret.get('sharpe',0)
        if sharpe <= 1.01 :
           logging.info("{} of sharpe ratio {} rejected for being less stable than SPY".format(name,sharpe))
           return True, None
        returns = ret.get('returns',0)
        if returns <= 0.04 :
           logging.info("{} of returns {} rejected for being less profitable than bonds".format(name,returns))
           return True, None
        return False, ret

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
        flag, data = HELPER3.is_stock_invalid(name, data)
        if flag :
            continue
        ret[name] = data
        msg = HELPER.round_values(**data)
        logging.info((name, msg))
    return ret

def _action() : 
    ret = {}
    for sector, stocks in prep() :
        if sector not in ret :
           ret[sector] = {}
        ret_pf = ret[sector]
        data_list = load(stocks)
        stocks = sorted(data_list.keys())
        portfolio_list = HELPER3.listPortfolios(stocks,data_list)
        skip = set()
        for index, portfolio in portfolio_list.iterrows():
            key = "{}_{}".format(sector,index)
            if key in skip :
                continue
            skip.add(key)
            portfolio = portfolio.dropna(how="all")
            ret_pf[key] = portfolio
    for sector in sorted(ret) :
        yield sector, ret[sector]
def _transform(data) :
    logging.info(data)
    meta = set(['returns', 'risk', 'sharpe'])
    ret = set(data.index).difference(meta)
    ret = list(ret)
    ret = sorted(ret)
    logging.info(ret)
    return ret

def action() : 
    ret = {}
    for sector, _list in _action() :
        portfolio_list = None
        if sector not in ret :
           ret[sector] = {}
        if isinstance(_list,dict) :
           _list = _list.values()
        _list = map(lambda x : _transform(x),_list) 
        ret_pf = ret[sector]
        portfolio_list = HELPER3.refinePortfolio(portfolio_list,_list)
        skip = set()
        for index, portfolio in portfolio_list.iterrows():
            key = "{}_{}".format(sector,index)
            if key in skip :
                continue
            skip.add(key)
            portfolio = portfolio.dropna(how="all")
            ret_pf[key] = portfolio
    for sector in sorted(ret) :
        yield sector, ret[sector]

@log_exception
@trace
def main(save_file) : 
    ret = INI.init()
    for key, value in action() :
        logging.info(value)
        INI.write_section(ret,key,**value)
    ret.write(open(save_file, 'w'))
    logging.info("results saved to {}".format(save_file))
@log_exception
@trace
def main(local_dir) :
    for name, section_list in action() :
        output_file = "{}/portfolio_{}.ini".format(local_dir, name)
        output_file = output_file.replace(" ", "_")
        ret = INI.init()
        name_list = sorted(section_list.keys())
        value_list = map(lambda key : section_list[key], name_list)
        for i, name in enumerate(name_list) :
            INI.write_section(ret,name,**value_list[i])
        logging.info("saving results to file {}".format(output_file))
        ret.write(open(output_file, 'w'))


if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   file_list = env.list_filenames('local/historical_prices/*pkl')
   local_dir = "{}/local".format(env.pwd_parent)
   ini_list = env.list_filenames('local/*.ini')
   ini_list = filter(lambda x : 'method03' in x, ini_list)
   ini_list = filter(lambda x : 'step02' in x, ini_list)

   main(local_dir)
