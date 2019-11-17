#!/usr/bin/python

import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from libCommon import INI
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libSharpe import HELPER as MONTECARLO
from libGraph import LINE, BAR, POINT, save
'''
   Graph portfolios to determine perfomance, risk, diversification
'''
def prep(*ini_list) :
    ret = {}
    for path, section, key, weight in INI.loadList(*ini_list) :
        if section not in ret :
           ret[section] = {}
        ret[section][key] = float(weight[0])
    return ret

def enrich(*ini_list) :
    ret = {}
    ini_list = filter(lambda x : 'background' in x, ini_list)
    ini_list = filter(lambda x : 'yahoo' in x, ini_list)
    for path, section, key, stock_list in INI.loadList(*ini_list) :
        if section == 'Sector' : pass
        elif section == 'Industry' : pass
        else : continue
        for stock in stock_list :
            if stock not in ret : ret[stock] = {}
            ret[stock][section] = key
    return ret

def benchmark(*ini_list) :
    ret = {}
    ini_list = filter(lambda x : 'benchmark' in x, ini_list)
    for path, section, key, stock_list in INI.loadList(*ini_list) :
        if section not in ['MOTLEYFOOL', 'Index'] : continue
        if section == 'MOTLEYFOOL' :
           if 'NASDAQ' not in key : continue
           if 'FUND' not in key : continue
        if section == 'Index' :
           if '500' not in key : continue
        ret[key] = stock_list
    return ret

def main(file_list, portfolo_ini, ini_list) :
    try :
        return _main(file_list, portfolio_ini, ini_list)
    except Exception as e :
        logging.error(e, exc_info=True)

def prototype(file_list,stock_list) :
    name_list, _ret = readData(file_list,stock_list)
    ret = _prototype(_ret)
    value_list = map(lambda x : _ret[x], name_list)
    value_list = map(lambda data : data.sort_index(inplace=True), value_list)
    value_list = map(lambda data : MONTECARLO.find(data, risk_free_rate=0.02, period=FINANCE.YEAR, span=0), value_list)
    sharpe = dict(zip(name_list,value_list))
    return name_list, ret, sharpe

def _prototype(data) :
    ret = data.pct_change().dropna(how="all")
    if len(ret) == 0 :
        return ret
    ret = 1 + ret
    ret.iloc[0] = 1  # set first day pseudo-price
    ret =  ret.cumprod()
    return ret

def _main(file_list, portfolio_ini, ini_list) :
    local_enrich = enrich(*ini_list)
    bench_list = benchmark(*ini_list)

    SNP = 'SNP500'
    snp = bench_list[SNP]
    gcps = snp[0]
    name_list, snp_returns, snp_sharpe = prototype(file_list,snp)
    snp_returns.rename(columns={gcps:SNP},inplace=True)

    FUNDS = 'NASDAQMUTFUND'
    funds = bench_list[FUNDS]
    fund_name_list, fund_returns, fund_sharpe = prototype(file_list,funds)

    portfolio_list = prep(*portfolio_ini)
    portfolio_list = pd.DataFrame(portfolio_list.values())
    portfolio_list = portfolio_list.sort_values(['risk'])
    logging.info(portfolio_list)
    ret_detail_list = []
    ret_name_return_list = []
    ret_summary_list = {}
    ret_diversified_list = []
    ret_name_diversified_list = []
    ret_sharpe_list = {}
    for weights, sharpe, diversified, name_diversified, name_returns in find(local_enrich, portfolio_list) :
        stock_list = sorted(weights.index)
        name_list, ret = readData(file_list,stock_list)
        ret = ret.pct_change().dropna(how="all")
        logging.info(weights)
        logging.info(stock_list)
        logging.info(sharpe)
        portfolio_return = weights.dot(ret.T).dropna(how="all")
        ret[name_returns] = portfolio_return
        ret = 1 + ret
        ret.iloc[0] = 1  # set first day pseudo-price
        ret =  ret.cumprod()
        print ret.head(2)
        print ret.tail(2)
        print name_diversified, name_returns 
        ret_detail_list.append(ret)
        ret_summary_list[name_returns] = ret[name_returns]
        ret_sharpe_list[name_returns] = sharpe
        ret_diversified_list.append(diversified)
        ret_name_return_list.append(name_returns)
        ret_name_diversified_list.append(name_diversified)
    ret_summary_list[SNP] = snp_returns
    ret_sharpe_list[SNP] = snp_sharpe[gcps]
    for name in fund_name_list :
        ret_summary_list[name] = fund_returns[name]
        ret_sharpe_list[name] = fund_sharpe[name]

    return ret_detail_list, ret_name_return_list, ret_summary_list, ret_diversified_list, ret_name_diversified_list, ret_sharpe_list

def find(enrich, portfolio_list) :
    portfolio_list = portfolio_list.T.dropna(how='all').T
    name_format_1 = "portfolio_diversified_{}"
    name_format_2 = "portfolio_returns_{}"
    curr = 1
    for id, portfolio in portfolio_list.iterrows() :
        portfolio = portfolio.T.dropna(how='all').T
        weights, sharpe, diversified = _find(enrich,portfolio)
        key_list = sorted(diversified.keys())
        value_list = map(lambda x : "{} : {}".format(x, "{" + x + "}"), key_list)
        value = "\n ".join(value_list)
        logging.info(value.format(**diversified))
        name_diversified = name_format_1.format(curr)
        name_returns = name_format_2.format(curr)
        yield weights, sharpe, diversified, name_diversified, name_returns
        curr += 1

def _find(enrich, portfolio) :
    meta = ['returns', 'risk', 'sharpe']
    set_meta = set(meta)
    column_list = set(portfolio.index) - set_meta
    ret = {}
    for column in column_list :
        sector = enrich[column]['Sector']
        weight = portfolio[column]
        weight = round(weight,2)
        logging.debug(( column, sector, weight))
        if sector not in ret :
           ret[sector] = 0.0
        ret[sector] += weight
        ret[sector] = round(ret[sector],2)
    weights = portfolio.drop(list(set_meta))
    sharpe = portfolio.drop(list(column_list))
    return weights, sharpe, ret

def readData(file_list, stock_list) :
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

if __name__ == '__main__' :

   from glob import glob
   import os,sys
   from libCommon import TIMER

   pwd = os.getcwd()
   local = pwd.replace('bin','local')
   portfolio_ini = glob('{}/yahoo_sharpe_method1*portfolios.ini'.format(local))
   ini_list = glob('{}/*.ini'.format(local))
   file_list = glob('{}/historical_prices/*pkl'.format(local))

   dir = pwd.replace('bin','log')
   name = sys.argv[0].split('.')[0]
   log_filename = '{}/{}.log'.format(dir,name)
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   logging.info("started {}".format(name))
   elapsed = TIMER.init()
   portfolio_list, portfolio_name_list, summary, portfolio_diversified_list, portfolio_name_diversified_list, portfolio_sharpe_list = main(file_list, portfolio_ini, ini_list )

   LINE.plot(summary, title="Returns")
   path = "{}/portfolio_summary.png".format(local)
   save(path)
   POINT.plot(portfolio_sharpe_list,x='risk',y='returns',ylabel="Returns", xlabel="Risk", title="Sharpe Ratio")
   path = "{}/portfolio_sharpe.png".format(local)
   save(path)

   for i, name in enumerate(portfolio_name_list) :
       graph = portfolio_list[i]
       title = 'Returns for {}'.format(name)
       title = title.replace('_returns_','')
       LINE.plot(graph, title=title)
       path = "{}/{}.png".format(local,name)
       save(path, ncol=3)

   for i, name in enumerate(portfolio_name_diversified_list) :
       graph = portfolio_diversified_list[i]
       title = 'Sector Distribution for {}'.format(name)
       title = title.replace('_diversified_','')
       BAR.plot(graph,xlabel='Percentage',title=title)
       path = "{}/{}.png".format(local,name)
       save(path)

   logging.info("finished {} elapsed time : {} ".format(name,elapsed()))
   
   '''
   config = INI.init()
   for key in ini.keys() :
       values = ini[key]
       if not isinstance(values,dict) :
          values = values.to_dict()
       INI.write_section(config,key,**values)
   stock_ini = "{}/yahoo_sharpe_method1_portfolios.ini".format(local)
   config.write(open(stock_ini, 'w'))
   '''
