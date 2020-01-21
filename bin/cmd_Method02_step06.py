#!/usr/bin/env python

import math
import logging
import pandas as pd

from libCommon import INI, exit_on_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libSharpe import HELPER, PORTFOLIO

from libDebug import trace

'''
Method 02 step 06 - 

1) Partition portfolio by Risk into 3 (scaled)
2) Partition portfolio by Sharpe into 3 (scaled)

portfolios are now divided into 9 groups :
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

def prep(ini_list) :
    key_list = ['Basic_Materials','Communication_Services','Consumer_Cyclical','Consumer_Defensive','Empty','Energy','Financial_Services','Healthcare','Industrials','Real_Estate','Technology','Utilities']
    not_stock = ['returns', 'risk', 'sharpe']
    portfolio_list = filter(lambda x : 'portfolio' in x, ini_list)
    ret = []
    for key in key_list :
        temp = filter(lambda x : key in x, portfolio_list)
        ret += temp
    portfolio_list = ret
    logging.info(portfolio_list)
    ret = {}
    for path, sector_enum, key, value in INI.loadList(*portfolio_list) :
        if key in not_stock : 
           continue
        if key not in ret :
           ret[key] = []
        value = float(value[0])
        ret[key].append(value)
    stock_list = sorted(ret)
    for stock in stock_list :
        data = ret[stock]
        data = pd.DataFrame(data).describe()
        ret[stock] = data
    return ret

def load(file_list, stock_list) :
    name_list = []
    data_list = pd.DataFrame()
    if len(stock_list) == 0 :
       return name_list, data_list

    for name, stock in STOCK_TIMESERIES.read(file_list, stock_list) :
        try :
            data_list[name] = stock['Adj Close']
            name_list.append(name)
        except Exception as e : logging.error(e, exc_info=True)
        finally : pass
    return name_list, data_list

def prep() :
    target = "portfolio_list"
    portfolio_list = globals().get(target,[])
    target = "sector_list"
    sector_list = globals().get(target,[])
    not_stock = ['returns', 'risk', 'sharpe']
    ret = []
    for sector in sector_list :
        temp = filter(lambda x : sector in x, portfolio_list)
        ret += temp
    portfolio_list = ret
    logging.info(portfolio_list)
    ret = {}
    for path, sector_enum, key, value in INI.loadList(*portfolio_list) :
        logging.info(sector_enum)
        if key in not_stock : 
           continue
        sector = filter(lambda x : x in sector_enum, key_list)
        logging.info(sector)
        if isinstance(sector,list) and len(sector) > 0 :
            sector = sector[0]
        if sector not in ret :
           ret[sector] = {}
        curr = ret[sector]
        if key not in curr :
           curr[key] = []
        curr = curr[key]
        value = float(value[0])
        curr.append(value)
    sector_list = sorted(ret)
    for sector in sector_list :
        curr = ret[sector]
        stock_list = sorted(curr)
        for stock in stock_list :
            data = curr[stock]
            data = pd.DataFrame(data).describe()
            curr[stock] = data
    logging.debug(ret)
    return ret

def action(input_file, file_list, ini_list) : 
    logging.info(portfolio_list)
    ret = {}
    for sector_enum, portfolio_name, stock_name, weight in prep(input_file,portfolio_list) :
        curr = None
        if sector_enum not in ret :
           ret[sector_enum] = {}
        curr = ret[sector_enum]
        if portfolio_name not in curr :
           curr[portfolio_name] = {}
        curr = curr[portfolio_name]
        curr[stock_name] = weight
    sector_enum = sorted(ret.keys())
    portfolio_list = map(lambda sector : ret[sector], sector_enum)
    for i, sector_name in enumerate(sector_enum) :
        yield sector_name, portfolio_list[i]
        for j, value in enumerate(portfolio_list[i]) :
            logging.info((i,j, sector_name,value))

def action(input_file, file_list, ini_list) : 
    sector_data = prep() 
    sector_list = sorted(sector_data)

    combined = []
    for sector in sector_list :
        logging.info( sector)
        stock_data = sector_data[sector]
        stock_list = sorted(stock_data)
        weights = map(lambda x : stock_data[x], stock_list)
        weights = map(lambda x : float(x.T['mean']), weights)
        weights = pd.DataFrame(weights, index=stock_list, columns=['mean'])
        logging.debug(weights)
        weights = weights.sort_values(["mean"]).tail(4)
        logging.debug(weights.T)
        total =  weights.sum(axis = 0, skipna = True) 
        weights = weights/total
        
        logging.debug(weights.T)
        stock_list = sorted(weights.T)
        combined += stock_list
        weights = map(lambda x : weights.T[x], stock_list)
        weights = map(lambda x : float(x.T['mean']), weights)
        weights = dict(zip(stock_list,weights))
        logging.info(weights)
        yield sector, weights
    logging.info(combined)
    stock_list, data_list = load(file_list,combined)
    max_sharp, min_dev = PORTFOLIO.find(data_list, stocks=stock_list, portfolios=500, period=FINANCE.YEAR)
    size = len(max_sharp)
    size *= 0.66
    size = int(size)
    logging.info(size)
    max_sharp = max_sharp.sort_values().tail(size)
    max_sharp = list(max_sharp.axes[0])
    logging.info(max_sharp)
    max_sharp, dummy = PORTFOLIO.find(data_list, stocks=max_sharp, portfolios=2500, period=FINANCE.YEAR)
    logging.info(max_sharp)

    size = len(min_dev)
    size *= 0.66
    size = int(size)
    logging.info(size)
    min_dev = min_dev.sort_values().head(size)
    min_dev = list(min_dev.axes[0])
    logging.info(min_dev)
    dummy, min_dev = PORTFOLIO.find(data_list, stocks=min_dev, portfolios=2500, period=FINANCE.YEAR)
    logging.info(min_dev)

    yield "combined_sharpe", max_sharp
    yield "combined_risk", min_dev
    return
@exit_on_exception
def main(input_file, file_list, ini_list,output_file) : 
    ret = INI.init()
    for name, section_list in action(input_file, file_list, ini_list) :
        INI.write_section(ret,name,**section_list)
    ret.write(open(output_file, 'w'))

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   portfolio_list = env.list_filenames('local/portfolio*.ini')
   ini_list = env.list_filenames('local/*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')
   input_file = env.list_filenames('local/method02*.ini')
   output_file = "{}/local/method02_step06.ini".format(env.pwd_parent)
   sector_list = ['Basic Materials','Communication Services','Consumer Cyclical','Consumer Defensive','Energy','Financial Services','Healthcare','Industrials','Real Estate','Technology','Utilities']

   main(input_file, file_list,ini_list,output_file)
