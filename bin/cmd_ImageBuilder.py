#!/usr/bin/python

import logging
from copy import deepcopy
import numpy as np
import pandas as pd
import warnings
import matplotlib.pyplot as plt
#plt.style.use('fivethirtyeight')
#plt.style.use('ggplot')
#plt.style.use('seaborn-whitegrid')
fig, ax = plt.subplots()
ax.grid(which='major', linestyle='-', linewidth='0.5', color='gray')


from libCommon import INI, STOCK_TIMESERIES, log_exception
from libDebug import trace
from libGraph import LINE, BAR, POINT, save
from libMonteCarlo import MonteCarlo
from libSharpe import PORTFOLIO, HELPER
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
        elif section == 'Category' : pass
        else : continue
        for stock in stock_list :
            if stock not in ret : ret[stock] = {}
            ret[stock][section] = key
    return ret

@trace
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

def prototype(file_list,stock_list) :
    name_list, _ret = readData(file_list,stock_list)
    ret = _prototype(_ret)
    annual = MonteCarlo.YEAR()
    value_list = map(lambda x : _ret[x], name_list)
    value_list = map(lambda x : annual.findSharpe(x), value_list)
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

@log_exception
@trace
def main(file_list, portfolio_ini, ini_list) :
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
    logging.info(portfolio_list)
    returns_graph_list = []
    returns_name_list = []
    summary_list = {}
    diversified_graph_list = []
    diversified_data_list = []
    diversified_name_list = []
    sharpe_weights_list = []
    sharpe_stocks_list = []
    sharpe_list = {}
    portfolio_name_list = []
    for stock_list, weights, diversified_graph, diversified_data, names in find(local_enrich, portfolio_list) :
        logging.info(weights)
        logging.info(stock_list)
        name_list, timeseries = readData(file_list,stock_list)
        proto = map(lambda x : HELPER.find(timeseries[x], span=0,period=252),name_list)
        proto = dict(zip(name_list,proto))
        sharpe_weights, sharpe_stocks = _newFind(proto,local_enrich,weights)
        logging.info(sharpe_weights)
        logging.info(sharpe_stocks)
        data = timeseries.pct_change().dropna(how="all")
        portfolio_sharpe = PORTFOLIO.findWeightedSharpe(data, weights)
        portfolio_return = weights.dot(data.T).dropna(how="all")
        portfolio_name_list.append(names['portfolio'])
        name_portfolio = 'legend_{portfolio}'.format(**names)
        data[name_portfolio] = portfolio_return
        data = 1 + data
        data.iloc[0] = 1  # set first day pseudo-price
        data =  data.cumprod()
        returns_graph_list.append(data)
        summary_list[name_portfolio] = data[name_portfolio]
        logging.info( portfolio_sharpe )
        sharpe_list[name_portfolio] = portfolio_sharpe
        sharpe_weights_list.append(sharpe_weights)
        sharpe_stocks_list.append(sharpe_stocks)
        diversified_graph_list.append(diversified_graph)
        diversified_data_list.append(diversified_data)
        returns_name_list.append(names['returns'])
        diversified_name_list.append(names['diversified'])
    summary_list[SNP] = snp_returns
    sharpe_list[SNP] = snp_sharpe[gcps]
    for name in fund_name_list :
        summary_list[name] = fund_returns[name]
        sharpe_list[name] = fund_sharpe[name]

    returns = {'graph' : returns_graph_list, 'name' : returns_name_list
            , 'description_summary' : sharpe_weights_list, 'description_details' : sharpe_stocks_list}
    diversified = {'graph' : diversified_graph_list, 'name' : diversified_name_list, 'description' : diversified_data_list }
    return returns, diversified, summary_list, sharpe_list, portfolio_name_list

def find(enrich, portfolio_list) :
    name_format_1 = "portfolio_diversified_{}"
    name_format_2 = "portfolio_returns_{}"
    key_list = sorted(portfolio_list.keys())
    for curr, portfolio_name in enumerate(key_list) :
        portfolio = portfolio_list[portfolio_name]
        portfolio = pd.DataFrame(portfolio, index=['weights'])
        portfolio = portfolio.T.dropna(how='all').T
        stock_list, weights, diversified_graph, diversified_data = _findDiversified(enrich,portfolio)
        key_list = sorted(diversified_graph.keys())
        key_list = filter(lambda x : x is not None, key_list)
        msg = map(lambda x : "{} : {}".format(x, "{" + x + "}"), key_list)
        msg = "\n ".join(msg)
        logging.info(msg.format(**diversified_graph))
        name_diversified = name_format_1.format(curr+1)
        name_returns = name_format_2.format(curr+1)
        names = ['portfolio', 'diversified', 'returns']
        names = dict(zip(names,[portfolio_name, name_diversified, name_returns]))
        yield stock_list, weights, diversified_graph, diversified_data, names

def _findDiversified(enrich, portfolio) :
    stock_diverse_keys = ['weight', 'ticker']
    meta = ['returns', 'risk', 'sharpe']
    set_meta = set(meta)
    column_list = set(portfolio.T.index) - set_meta
    logging.info(column_list)
    diversified_weights = {}
    diversified_stocks = {}
    for column in column_list :
        sector = enrich.get(column, {}).get('Sector',None)
        if sector is None :
           sector = enrich.get(column, {}).get('Category','Unknown')
        weight_1 = portfolio[column]
        weight = round(weight_1,2)
        logging.debug(( column, sector, weight))
        if sector not in diversified_weights :
           diversified_weights[sector] = 0.0
        diversified_weights[sector] += weight
        diversified_weights[sector] = round(diversified_weights[sector],2)
        if sector not in diversified_stocks :
           diversified_stocks[sector] = []
        value = dict(zip(stock_diverse_keys,[round(weight_1*100,2), column]))
        diversified_stocks[sector].append(value)
    temp = filter(lambda x : x in portfolio, meta)
    weights = portfolio.T.drop(list(temp))
    return column_list, weights['weights'], diversified_weights, diversified_stocks

def _newFind(data, enrich, weights) :
    column_list = []
    diversified_stocks = {}
    diversified_weights = {}
    if isinstance(data,dict) : 
       column_list = sorted(data.keys()) 
    meta = ['returns', 'risk', 'sharpe']
    weighted_meta = ['weighted returns', 'weighted risk', 'weighted sharpe']
    for column in column_list :
        ret = deepcopy(data.get(column,{}))
        value_list = map(lambda x : ret.get(x,0),meta)
        value_list = map(lambda x : x*weights[column],value_list)
        temp = dict(zip(weighted_meta,value_list))
        ret.update(temp)
        temp = enrich.get(column,{})
        target = 'Sector'
        if target not in temp :
           target = 'Category'
        sector = temp.get(target,'Unknown')
        if sector not in diversified_stocks :
           diversified_stocks[sector] = {}
        key_list = ret.keys()
        value_list = map(lambda x : ret[x], key_list) 
        value_list = map(lambda x : round(float(x),2), value_list)
        ret = dict(zip(key_list,value_list))
        diversified_stocks[sector][column] = ret
        if sector not in diversified_weights :
           diversified_weights[sector] = dict(zip(weighted_meta,[0.0]*len(weighted_meta)))
        for key in weighted_meta :
            w = diversified_weights[sector][key] + ret[key]
            diversified_weights[sector][key] = w
    return diversified_weights, diversified_stocks

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

   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   portfolio_ini = env.list_filenames('local/method*portfolios.ini')
   ini_list = env.list_filenames('local/*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')

   returns, diversified, summary, portfolio_sharpe_list, portfolio_name_list = main(file_list, portfolio_ini, ini_list )
   summary_path_list = []
   POINT.plot(portfolio_sharpe_list,x='risk',y='returns',ylabel="Returns", xlabel="Risk", title="Sharpe Ratio")
   path = "{pwd_parent}/local/portfolio_sharpe.png".format(**vars(env))
   save(path)
   summary_path_list.append(path)
   LINE.plot(summary, title="Returns")
   path = "{pwd_parent}/local/portfolio_summary.png".format(**vars(env))
   save(path)
   summary_path_list.append(path)

   graph_list = diversified['graph']
   name_list = diversified['name']
   local_diversify_list = []
   for i, name in enumerate(portfolio_name_list) :
       graph = graph_list[i]
       #title = 'Sector Distribution for {}'.format(name)
       #title = title.replace('_diversified_','')
       #BAR.plot(graph,xlabel='Percentage',title=title)
       BAR.plot(graph,xlabel='Percentage')
       path = "{}/local/{}.png".format(env.pwd_parent,name_list[i])
       save(path)
       local_diversify_list.append(path)

   graph_list = returns['graph']
   name_list = returns['name']
   local_returns_list = []
   for i, name in enumerate(portfolio_name_list) :
       graph = graph_list[i]
       #title = 'Returns for {}'.format(name)
       #title = title.replace('_returns_','')
       #LINE.plot(graph, title=title)
       LINE.plot(graph)
       path = "{}/local/{}.png".format(env.pwd_parent,name_list[i])
       save(path, ncol=3)
       local_returns_list.append(path)

   summary = { "images" : summary_path_list , "captions" : ["Return over Risk", "portfolio returns"] }
   portfolio = {}
   for i, value in enumerate(local_diversify_list) :
       images = [ local_diversify_list[i], local_returns_list[i] ]
       captions = [ "portfolio diversity {}", "portfolio returns {}"]
       captions = map(lambda x : x.format(portfolio_name_list[i]), captions)
       captions = map(lambda x : x.replace('_', ' '), captions)
       section = { "images" : images, "captions" : captions }
       section['description1'] = diversified['description'][i]
       section['description2'] = returns['description_summary'][i]
       section['description3'] = returns['description_details'][i]
       section['name'] = portfolio_name_list[i]
       key = "portfolio_{}".format(i)
       portfolio[key]  = section

   config = INI.init()
   INI.write_section(config,"summary",**summary)
   for key in portfolio.keys() :
       values = portfolio[key]
       if not isinstance(values,dict) :
          values = values.to_dict()
       INI.write_section(config,key,**values)
   stock_ini = "{pwd_parent}/local/report_generator.ini".format(**vars(env))
   config.write(open(stock_ini, 'w'))
