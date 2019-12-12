#!/usr/bin/env python

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

from libCommon import INI, log_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libDebug import trace
from libGraph import LINE, BAR, POINT, save
from libSharpe import PORTFOLIO, HELPER as MONTECARLO
'''
   Graph portfolios to determine perfomance, risk, diversification
'''
def prep_Portfolio(*ini_list) :
    ret = {}
    for path, section, key, weight in INI.loadList(*ini_list) :
        if section not in ret :
           ret[section] = {}
        ret[section][key] = float(weight[0])
    return ret

def prep_Enrich(*ini_list) :
    ret = {}
    ini_list = filter(lambda x : 'stock_background' in x, ini_list)
    for path, section, key, stock_list in INI.loadList(*ini_list) :
        for stock in stock_list :
            if stock not in ret : ret[stock] = {}
            ret[stock]['Sector'] = key
    return ret

@trace
def prep_benchmark(*ini_list) :
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

def transformMonteCarlo(file_list,stock_list) :
    name_list, _ret = load(file_list,stock_list)
    value_list = map(lambda x : _ret[x], name_list)
    #value_list = map(lambda data : data.sort_index(inplace=True), value_list)
    #value_list = map(lambda data : MONTECARLO.find(data, span=0, period=FINANCE.YEAR), value_list)
    value_list = map(lambda data : MONTECARLO.find(data, period=FINANCE.YEAR), value_list)
    sharpe = dict(zip(name_list,value_list))
    ret = FINANCE.findDailyReturns(_ret)
    return name_list, ret, sharpe

class HELPER_BASIC :
    name_format_1 = "portfolio_diversified_{}"
    name_format_2 = "portfolio_returns_{}"

    @classmethod
    def add(cls, curr, portfolio_name) :
        name_diversified = cls.name_format_1.format(curr)
        name_returns = cls.name_format_2.format(curr)
        names = ['portfolio', 'diversified', 'returns']
        return dict(zip(names,[portfolio_name, name_diversified, name_returns]))
    @classmethod
    def message(cls, **data) :
        key_list = sorted(data.keys())
        key_list = filter(lambda x : x is not None, key_list)
        ret = map(lambda x : "{} : {}".format(x, "{" + x + "}"), key_list)
        ret = "\n ".join(ret)
        ret = ret.format(**data)
        return ret
    @classmethod
    def transform(cls, portfolio) :
        weights = pd.DataFrame(portfolio, index=['weights'])
        weights = weights.T.dropna(how='all').T
        return weights

def lambdaFind(enrich, portfolio_list) :
    key_list = sorted(portfolio_list.keys())
    for curr, portfolio_name in enumerate(key_list) :
        portfolio = portfolio_list[portfolio_name]
        weights = HELPER_BASIC.transform(portfolio)
        stock_list, weights, graphBySector, dataBySector = enrichWithSectorEnumeration(enrich,weights)

        msg = HELPER_BASIC.message(**graphBySector)
        logging.info(msg)

        names = HELPER_BASIC.add(curr+1, portfolio_name)
        logging.info(weights)
        logging.info(stock_list)
        yield stock_list, weights, graphBySector, dataBySector, names

class PORTFOLIO_HELPER :
    meta = ['returns', 'risk', 'sharpe']
    set_meta = set(meta)
    @classmethod
    def findStocks(cls, data) :
        return set(data.T.index) - cls.set_meta
    @classmethod
    def removeMeta(cls, data) :
        temp = filter(lambda x : x in data, cls.meta)
        return data.T.drop(list(temp))
@trace
def enrichWithSectorEnumeration(enrich, portfolio) :
    stock_diverse_keys = ['weight', 'ticker']
    column_list = PORTFOLIO_HELPER.findStocks(portfolio)
    logging.info(column_list)
    ret_weights = {}
    ret_stocks = {}
    for column in column_list :
        sector = enrich.get(column, {}).get('Sector',None)
        if sector is None :
           sector = enrich.get(column, {}).get('Category','Unknown')
        weight_1 = portfolio[column]
        weight = round(weight_1,2)
        logging.debug(( column, sector, weight))
        if sector not in ret_weights :
           ret_weights[sector] = 0.0
        ret_weights[sector] += weight
        ret_weights[sector] = round(ret_weights[sector],2)
        if sector not in ret_stocks :
           ret_stocks[sector] = []
        value = dict(zip(stock_diverse_keys,[round(weight_1*100,2), column]))
        ret_stocks[sector].append(value)

    weights = PORTFOLIO_HELPER.removeMeta(portfolio)
    return column_list, weights['weights'], ret_weights, ret_stocks

class WEIGHTS_HELPER :
    meta = ['returns', 'risk', 'sharpe']
    weighted_meta = ['weighted returns', 'weighted risk', 'weighted sharpe']

    @classmethod
    def default(cls) :
        return dict(zip(cls.weighted_meta,[0.0]*len(cls.weighted_meta)))

    @classmethod
    def add(cls, data, stock, weights) :
        logging.info(stock)
        logging.info(weights)
        ret = deepcopy(data.get(stock,{}))
        value_list = map(lambda x : ret.get(x,0),cls.meta)
        value_list = map(lambda x : x*weights[stock],value_list)
        temp = dict(zip(cls.weighted_meta,value_list))
        ret.update(temp)
        return ret

    @classmethod
    def humanReadable(cls, data) :
        # round the percentages to human readable
        key_list = data.keys()
        value_list = map(lambda x : data[x], key_list) 
        value_list = map(lambda x : round(float(x),2), value_list)
        ret = dict(zip(key_list,value_list))
        return ret
@trace
def addWeights(data, enrich, weights) :
    ret_stocks = {}
    ret_weights = {}
    stock_list = []
    if isinstance(data,dict) : 
       stock_list = sorted(data.keys()) 

    for stock in stock_list :
        #add weighted percentages to returns, risk and sharpe
        stock_data = WEIGHTS_HELPER.add(data, stock, weights)
        temp = enrich.get(stock,{})
        target = 'Sector'
        if target not in temp :
           target = 'Category'
        sector = temp.get(target,'Unknown')
        if sector not in ret_stocks :
           ret_stocks[sector] = {}

        ret = WEIGHTS_HELPER.humanReadable(stock_data)
        logging.info(ret)
        ret_stocks[sector][stock] = ret
        if sector not in ret_weights :
           ret_weights[sector] = WEIGHTS_HELPER.default()
        for key in WEIGHTS_HELPER.weighted_meta :
            logging.info((key))
            w = ret_weights[sector][key] + ret[key]
            ret_weights[sector][key] = w

    logging.info(ret_weights)
    logging.info(ret_stocks)
    return ret_weights, ret_stocks

class Group :
    def __init__(self) :
       self.graph = []
       self.data = []
       self.name = []
    def __call__(self) :
        return {'graph' : self.graph, 'name' : self.name, 'description' : self.data}
    def append(self, **kwargs) :
        target = 'name'
        name = kwargs.get(target,None)
        target = 'data'
        data = kwargs.get(target,None)
        target = 'graph'
        graph = kwargs.get(target,None)
        if name is not None :
            self.name.append(name)
        if data is not None :
            self.data.append(data)
        if graph is not None :
            self.graph.append(graph)

def process(file_list, portfolio_ini, ini_list) :
    local_enrich = prep_Enrich(*ini_list)
    bench_list = prep_benchmark(*ini_list)

    SNP = 'SNP500'
    snp = bench_list[SNP]
    gcps = snp[0]
    name_list, snp_returns, snp_sharpe = transformMonteCarlo(file_list,snp)
    snp_returns.rename(columns={gcps:SNP},inplace=True)

    FUNDS = 'NASDAQMUTFUND'
    funds = bench_list[FUNDS]
    fund_name_list, fund_returns, fund_sharpe = transformMonteCarlo(file_list,funds)

    if not isinstance(portfolio_ini, list) :
       portfolio_ini = [portfolio_ini]
    portfolio_list = prep_Portfolio(*portfolio_ini)
    logging.info(portfolio_list)
    _returns = Group()
    _enriched = Group()
    _sharpe = Group()
    summary_list = {}
    sharpe_list = {}
    portfolio_name_list = []
    for stock_list, weights, graphBySector, dataBySector, names in lambdaFind(local_enrich, portfolio_list) :
        name_list, timeseries = load(file_list,stock_list)
        value_list = map(lambda x : timeseries[x], name_list)
        value_list = map(lambda data : MONTECARLO.find(data, period=FINANCE.YEAR), value_list)
        logging.info(value_list[0])
        _portfolio = dict(zip(name_list, value_list))
        logging.info(_portfolio)
        sharpe_weights, sharpe_stocks = addWeights(_portfolio,local_enrich,weights)
        data = FINANCE.findDailyReturns(timeseries)
        portfolio_sharpe = PORTFOLIO.findWeightedSharpe(data, weights, risk_free_rate=0.02, period=FINANCE.YEAR)
        portfolio_return = weights.dot(data.T).dropna(how="all")
        portfolio_name_list.append(names['portfolio'])
        name_portfolio = 'legend_{portfolio}'.format(**names)
        portfolio_return = FINANCE.transformReturns(portfolio_return)
        portfolio_return.iloc[0] = 1  # set first day pseudo-price
        data = FINANCE.graphReturns(timeseries)
        data[name_portfolio] = portfolio_return
        summary_list[name_portfolio] = data[name_portfolio]
        logging.info( portfolio_sharpe )
        sharpe_list[name_portfolio] = portfolio_sharpe
        _sharpe.append(name=sharpe_weights, data=sharpe_stocks)
        _enriched.append(graph=graphBySector, data=dataBySector,name=names['diversified'])
        _returns.append(graph=data, name=names['returns'])

    summary_list[SNP] = snp_returns
    sharpe_list[SNP] = snp_sharpe[gcps]
    for name in fund_name_list :
        summary_list[name] = fund_returns[name]
        sharpe_list[name] = fund_sharpe[name]

    returns = _returns()
    returns['description_summary'] = _sharpe().get('name',[])
    returns['description_details'] = _sharpe().get('description',[])
    diversified = _enriched()
    return returns, diversified, summary_list, sharpe_list, portfolio_name_list

@log_exception
@trace
def main(env, file_list, input_file, ini_list, output_file) :
   returns, diversified, summary, portfolio_sharpe_list, portfolio_name_list = process(file_list, input_file, ini_list )
   summary_path_list = []
   POINT.plot(portfolio_sharpe_list,x='risk',y='returns',ylabel="Returns", xlabel="Risk", title="Sharpe Ratio")
   path = "{pwd_parent}/local/portfolio_sharpe.png".format(**vars(env))
   save(path,loc="lower right")
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
   config.write(open(output_file, 'w'))

if __name__ == '__main__' :

   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()

   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')

   input_file = env.list_filenames('local/method*portfolios.ini')
   output_file = "{pwd_parent}/local/report_generator.ini".format(**vars(env))
   if len(env.argv) > 0 :
      input_file = env.argv[0]
   if len(env.argv) > 1 :
      output_file = env.argv[1]

   main(env, file_list, input_file, ini_list, output_file)
