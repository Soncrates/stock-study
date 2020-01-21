#!/usr/bin/env python

import logging
from copy import deepcopy
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
#plt.style.use('fivethirtyeight')
#plt.style.use('ggplot')
#plt.style.use('seaborn-whitegrid')
fig, ax = plt.subplots()
ax.grid(which='major', linestyle='-', linewidth='0.5', color='gray')

from libCommon import INI, exit_on_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libDebug import trace
from libGraph import LINE, BAR, POINT, save
from libSharpe import PORTFOLIO, HELPER as SHARPE_HELPER
'''
   Graph portfolios to determine perfomance, risk, diversification
'''
class PREP() :
    _enrich_cache = None
    _singleton = None
    def __init__(self, _env, _data_store, _portfolio,_enrich,_benchmark,file_list) :
        self._env = _env
        self._data_store = _data_store
        self._portfolio = _portfolio
        self._enrich = _enrich
        self._benchmark = _benchmark
        self.file_list = file_list
    @classmethod
    def singleton(cls, **kwargs) :
        if not (cls._singleton is None) :
           return cls._singleton
        target = 'env'
        _env = globals().get(target,None)
        target = 'data_store'
        _data_store = globals().get(target,'')
        if not isinstance(_data_store,str) :
           _data_store = str(_data_store)
        target = 'input_file'
        _portfolio = globals().get(target,'')
        if not isinstance(_portfolio,list) :
           _portfolio = [_portfolio]
        target = 'stock_background'
        _enrich = globals().get(target,[])
        if not isinstance(_enrich,list) :
           _enrich = list(_enrich)
        target = 'benchmark'
        _benchmark = globals().get(target,[])
        if not isinstance(_benchmark,list) :
           _benchmark = list(_benchmark)
        target = "file_list"
        file_list = globals().get(target,[])
        cls._singleton = cls(_env,_data_store,_portfolio,_enrich,_benchmark,file_list)
        return cls._singleton
    @classmethod
    def prep(cls) :
        data_store = cls.singleton()._data_store
        logging.info('making data store {}'.format(data_store))
        cls.singleton()._env.mkdir(data_store)
    @classmethod
    def readStockData(cls, value_list) :
        file_list = cls.singleton().file_list
        ret = {}
        for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
            logging.info((name,type(data),data.shape))
            ret[name] = data
        return ret
    @classmethod
    def readPrices(cls,stock_list) :
        logging.debug(stock_list)
        ret = cls.readStockData(stock_list)
        stock_list = sorted(ret)
        price_list = map(lambda stock : pd.DataFrame(ret[stock])['Adj Close'], stock_list)
        if not isinstance(price_list,list) :
           price_list = list(price_list)
        ret = dict(zip(stock_list, price_list))
        ret = pd.DataFrame(ret, columns=stock_list)
        logging.info(ret.head(3))
        logging.info(ret.tail(3))
        return ret
    @classmethod
    def readPortfolio(cls) :
        portfolio = cls.singleton()._portfolio
        logging.info('reading file {}'.format(portfolio))
        ret = {}
        for path, section, key, weight in INI.loadList(*portfolio) :
            if section.startswith('dep_') :
               continue
            if section not in ret :
               ret[section] = {}
            ret[section][key] = float(weight[0])
        return ret
    @classmethod
    def readEnrich(cls) :
        enrich = cls.singleton()._enrich
        if not (cls._enrich_cache is None) :
           logging.info('reading cache {}'.format(enrich))
           return cls._enrich_cache
        logging.info('reading file {}'.format(enrich))
        ret = {}
        for path, section, key, stock_list in INI.loadList(*enrich) :
            for stock in stock_list :
                if stock not in ret : ret[stock] = {}
                ret[stock]['Sector'] = key
        cls._enrich_cache = ret
        return ret
    @classmethod
    def readSector(cls,stock) :
        enrich = cls.readEnrich().get(stock,{})
        target = 'Sector'
        ret = enrich.get(target,None)
        if ret is None :
           ret = enrich.get('Category','Unknown')
        return ret
    @classmethod
    def parseStockList(cls,data) :
        if isinstance(data,dict) :
           return sorted(data)
        ret = sorted(data.index.values)
        if not isinstance(ret[0],str) :
           ret = sorted(data.columns.values)
        logging.info(ret)
        return ret
    @classmethod
    def summarizeReturns(cls,data) :
        stock_list = cls.parseStockList(data)
        ret = map(lambda stock: FINANCE.findDailyReturns(data[stock]), stock_list)
        ret = dict(zip(stock_list,ret))
        return ret
    @classmethod
    def summarizeSharpe(cls,data) :
        stock_list = cls.parseStockList(data)
        ret = map(lambda d : SHARPE_HELPER.find(data[d], period=FINANCE.YEAR), stock_list)
        ret = dict(zip(stock_list, ret))
        ret = pd.DataFrame(ret).T
        logging.info(ret)
        return ret
    @classmethod
    def enrichSector(cls,ret) :
        stock_list = cls.parseStockList(ret)
        ret = map(lambda stock : PREP.readSector(stock), stock_list)
        ret = dict(zip(stock_list,ret))
        ret = pd.Series(ret)
        logging.info(ret)
        return ret
    @classmethod
    def enrichWeight(cls,weight,ret) :
        stock_list = cls.parseStockList(ret)
        ret = map(lambda stock : weight.get(stock,0.0), stock_list)
        ret = dict(zip(stock_list,ret))
        ret = pd.Series(ret)
        logging.info(ret)
        return ret

    @classmethod
    def readBenchmark(cls) :
        benchmark = cls.singleton()._benchmark
        logging.info('reading file {}'.format(benchmark))
        ret = {}
        for path, section, key, stock_list in INI.loadList(*benchmark) :
            #if section not in ['MOTLEYFOOL', 'Index'] : continue
            if section not in ['PERSONAL', 'Index'] : continue
            if section == 'MOTLEYFOOL' :
               if 'NASDAQ' not in key : continue
               if 'FUND' not in key : continue
            if section == 'Index' :
               if '500' not in key : continue
            ret[key] = stock_list
        logging.debug(ret)
        return ret
class TRANSFORM() :
    meta_columns = ['returns', 'risk', 'sharpe']
    @classmethod
    def getSummary(cls,weights) :
        ret = weights.round(2)
        logging.debug(ret)
        portfolio_columns = ['returns','risk','sharpe']

        returns = ret['weighted_returns'].to_dict()
        returns = sum(returns.values())
        returns = round(returns,2)
        risk = ret['weighted_risk'].to_dict()
        risk = sum(risk.values())
        risk = round(risk,2)
        sharpe = ret['weighted_sharpe'].to_dict()
        sharpe = sum(sharpe.values())
        sharpe = round(sharpe,2)
        ret = dict(zip(portfolio_columns,[returns,risk,sharpe]))

        logging.info(ret)
        return ret
    @classmethod
    def getPortfolioPrice(cls,weights,prices) :
        weights = pd.DataFrame([weights])
        logging.info(weights)
        logging.info(weights.shape)
        logging.info(prices.shape)
        ret = prices.fillna(0)
        ret = weights.dot(prices.T).dropna(how="all").T
        return ret
    @classmethod
    def parseWeights(cls, portfolio) :
        _ret = sorted(portfolio.keys())
        stock_list = filter(lambda x : x not in cls.meta_columns, _ret)
        if not isinstance(stock_list, list) :
           stock_list = list(stock_list)
        logging.info((len(stock_list),stock_list))
        ret = map(lambda stock : portfolio[stock],stock_list)
        ret = dict(zip(stock_list,ret))
        return ret
    @classmethod
    def smartMassage(cls,data) :
        key_list = sorted(data.columns.values)
        key_list = filter(lambda ref : not ref.startswith('legend_'), key_list)
        key_list = list(key_list)
        logging.info(key_list)
        for ref in key_list :
            data[ref] = FINANCE.graphReturns(data[ref])
        return data

    @classmethod
    def _find(cls) :
        ret = PREP.readPortfolio()
        logging.debug(ret)
        key_list = sorted(ret.keys())
        for curr, key in enumerate(key_list) :
            logging.info((curr+1, key,ret[key]))
            meta_name_list = OUTPUT.add(curr+1, key)
            yield key, ret[key], meta_name_list
    @classmethod
    def find(cls) :
        for name, weights, meta_name_list in cls._find() :
            weights = cls.parseWeights(weights)
            prices = PREP.readPrices(weights.keys())
            prices[meta_name_list['legend']] = cls.getPortfolioPrice(weights,prices)

            logging.info((weights,meta_name_list))

            meta = PREP.summarizeSharpe(prices)
            meta['sector'] = PREP.enrichSector(meta)
            meta['weight'] = PREP.enrichWeight(weights, meta)
            meta['weighted_returns'] = meta['returns']*meta['weight']
            meta['weighted_risk'] = meta['risk']*meta['weight']
            meta['weighted_sharpe'] = meta['sharpe']*meta['weight']
            logging.info(meta.T)
            returns = PREP.summarizeReturns(prices)
            yield weights, meta_name_list, prices, meta, returns

class OUTPUT() :

    name_format_1 = "portfolio_diversified_{}"
    name_format_2 = "portfolio_returns_{}"
    name_format_3 = 'legend_{}'
    @classmethod
    def add(cls, curr, name) :
        name_diversified = cls.name_format_1.format(curr)
        name_returns = cls.name_format_2.format(curr)
        name_legend = cls.name_format_3.format(name)
        name_list = ['portfolio', 'diversified', 'returns', 'legend']
        return dict(zip(name_list,[name, name_diversified, name_returns,name_legend]))
    @classmethod
    def getWeights(cls,data) :
        _weights = data['weight'].to_dict()
        _sectors = data['sector'].to_dict()
        _omit_list = filter(lambda key : key.startswith('legend'), _weights.keys())
        for omit in list(_omit_list) :
            _weights.pop(omit,None)
            _sectors.pop(omit,None)
        logging.info(_weights)
        logging.info(_sectors)
        stock_list = sorted(_sectors)
        sector_list = set(_sectors.values())
        sector_list = sorted(list(sector_list))
        ret_weights = {}
        ret_sector = {}
        for sector in sector_list :
            _t = filter(lambda stock : _sectors[stock] == sector, stock_list)
            _t = list(_t)
            sub = map(lambda stock : _weights[stock],_t)
            _t = map(lambda stock : {'weight': round(_weights[stock]*100,2), 'ticker': stock},_t)
            _t = list(_t)
            ret_weights[sector] = _t
            ret_sector[sector] = round(sum(sub),2)
        logging.info(ret_sector)
        logging.info(ret_weights)
        return ret_weights, ret_sector
    @classmethod
    def getSharpes(cls,data) :
        logging.debug(data)
        #_weights = data['weight'].to_dict()
        _data = data.round(2)
        _sectors = _data['sector'].to_dict()
        _returns = _data['weighted_returns'].to_dict()
        _risk = _data['weighted_risk'].to_dict()
        _sharpe = _data['weighted_sharpe'].to_dict()
        _omit_list = filter(lambda key : key.startswith('legend'), _sectors.keys())
        for omit in list(_omit_list) :
            _sectors.pop(omit,None)
            _returns.pop(omit,None)
            _risk.pop(omit,None)
            _sharpe.pop(omit,None)

        logging.debug(_sectors)
        logging.debug(_returns)
        logging.debug(_risk)
        logging.debug(_sharpe)
        stock_list = sorted(_sectors)
        sector_list = set(_sectors.values())
        sector_list = sorted(list(sector_list))
        ret_sector = {}
        ret_stock = {}
        sector_columns = ['weighted returns','weighted risk','weighted sharpe']
        stock_columns = ['returns', 'risk', 'sharpe', 'len', 'weighted_returns', 'weighted_risk', 'weighted_sharpe']
        rename_columns = {"weighted_returns": "weighted returns"
                         , 'weighted_risk' : 'weighted risk'
                         , 'weighted_sharpe' : 'weighted sharpe'}
        for sector in sector_list :
            _t = filter(lambda stock : _sectors[stock] == sector, stock_list)
            _t = list(_t)
            _t_returns = map(lambda stock : _returns[stock],_t)
            _t_returns = round(sum(_t_returns),2)
            _t_risk = map(lambda stock : _risk[stock],_t)
            _t_risk = round(sum(_t_risk),2)
            _t_sharpe = map(lambda stock : _sharpe[stock],_t)
            _t_sharpe = round(sum(_t_sharpe),2)
            ret_sector[sector] = dict(zip(sector_columns,[_t_returns, _t_risk, _t_sharpe]))
            _wtf = _data[stock_columns]
            _wtf = _wtf.T[_t].T
            _wtf = _wtf.rename(columns=rename_columns)
            ret_stock[sector] = _wtf.round(2).T.to_dict()
        logging.info(ret_sector)
        logging.info(ret_stock)
        return ret_stock, ret_sector
class Group :
    def __init__(self) :
       self.graph = []
       self.data = []
       self.name = []
    def __call__(self) :
        return {'graph' : self.graph, 'name' : self.name, 'description' : self.data}
    def add(self, **kwargs) :
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
    @classmethod
    def appendDiversify(cls,name,data,ret) :
        ret_weights, ret_sector = OUTPUT.getWeights(data)
        ret.add(graph=ret_sector,data=ret_weights,name=name)
        return ret

    @classmethod
    def appendSharpe(cls,name,data,ret) :
        ret_stock, ret_sector = OUTPUT.getSharpes(data)
        ret.add(graph=ret_sector,data=ret_stock,name=name)
        return ret
    @classmethod
    def appendPrices(cls,name,data,ret) :
        prices = FINANCE.graphReturns(data)
        ret.add(graph=prices,name=name)
        return ret

def process() :
    PREP.prep()

    _returns = Group()
    _enriched = Group()
    _sharpe = Group()
    graph_summary_list = {}
    graph_sharpe_list = {}
    _portfolio_name_list = []
    for weights, names, prices, summary, returns in TRANSFORM.find() :
        _name_portfolio = 'legend_{portfolio}'.format(**names)
        _portfolio_name_list.append(names['portfolio'])

        _enriched = Group.appendDiversify(names['diversified'],summary,_enriched)
        #_sharpe = Group.appendSharpe(names['legend'],summary,_sharpe)
        _sharpe = Group.appendSharpe(weights,summary,_sharpe)
        graph_sharpe_list[_name_portfolio] = TRANSFORM.getSummary(summary)
        
        graph_summary_list[_name_portfolio] = prices[_name_portfolio]
        logging.info(prices.head(3))
        _returns = Group.appendPrices(names['returns'],prices, _returns)
        continue
        #portfolio_sharpe = PORTFOLIO.findWeightedSharpe(prices, weights, risk_free_rate=0.02, period=FINANCE.YEAR)
        #graph_sharpe_list[name_portfolio] = portfolio_sharpe
        #_sharpe.append(name=sharpe_weights, data=sharpe_stocks)
    graph_summary_list = pd.concat(graph_summary_list.values(),axis=1)

    diversified = _enriched()
    logging.info(graph_sharpe_list)

    bench_list = PREP.readBenchmark()

    SNP = 'SNP500'
    snp = bench_list[SNP]
    gcps = snp[0]
    snp_prices = PREP.readPrices(snp)
    logging.debug(snp_prices)
    snp_prices.rename(columns={gcps:SNP},inplace=True)
    logging.debug(snp_prices)
    snp_sharpe = PREP.summarizeSharpe(snp_prices).T
    graph_summary_list[SNP] = snp_prices
    graph_sharpe_list[SNP] = snp_sharpe[SNP]

    FUNDS = 'NASDAQMUTFUND'
    funds = bench_list[FUNDS]
    funds_prices = PREP.readPrices(funds)
    funds_sharpe = PREP.summarizeSharpe(funds_prices).T
    funds_name_list = sorted(funds_prices)

    for name in funds_name_list :
        graph_summary_list[name] = funds_prices[name]
        graph_sharpe_list[name] = funds_sharpe[name]

    '''
    Final Massage
    '''
    logging.debug(graph_summary_list)
    graph_summary_list.fillna(method='backfill',inplace=True)
    graph_summary_list.fillna(1,inplace=True)
    graph_summary_list = graph_summary_list / graph_summary_list.iloc[0]
    graph_summary_list = TRANSFORM.smartMassage(graph_summary_list)

    returns = _returns()
    returns['description_summary'] = _sharpe().get('graph',[])
    returns['description_details'] = _sharpe().get('description',[])
    diversified = _enriched()
    logging.info(diversified)
    logging.info(graph_summary_list)
    return returns, diversified, graph_summary_list, graph_sharpe_list, _portfolio_name_list

@exit_on_exception
@trace
def main(local_dir, output_file) :
   returns, diversified, graph_summary, graph_portfolio_sharpe_list, portfolio_name_list = process()
   summary_path_list = []
   POINT.plot(graph_portfolio_sharpe_list,x='risk',y='returns',ylabel="Returns", xlabel="Risk", title="Sharpe Ratio")
   SHARPE = LINE.plot_sharpe(ratio=1)
   SHARPE.plot.line(style='b:', label='sharpe ratio 1',alpha=0.3)
   SHARPE = LINE.plot_sharpe(ratio=2)
   SHARPE.plot.line(style='r:',label='sharpe ratio 2',alpha=0.3)

   path = "{}/images/portfolio_sharpe.png".format(local_dir)
   save(path,loc="lower right")
   summary_path_list.append(path)
   LINE.plot(graph_summary, title="Returns")
   path = "{}/images/portfolio_summary.png".format(local_dir)
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
       path = "{}/images/{}.png".format(local_dir,name_list[i])
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
       path = "{}/images/{}.png".format(local_dir,name_list[i])
       save(path, ncol=3)
       local_returns_list.append(path)

   summary = { "images" : summary_path_list , "captions" : ["Return over Risk", "portfolio returns"] }
   portfolio = {}
   for i, value in enumerate(local_diversify_list) :
       images = [ local_diversify_list[i], local_returns_list[i] ]
       captions = [ "portfolio diversity {}", "portfolio returns {}"]
       captions = map(lambda x : x.format(portfolio_name_list[i]), captions)
       captions = map(lambda x : x.replace('_', ' '), captions)
       if not isinstance(captions,list) :
          captions = list(captions)
       section = { "images" : images, "captions" : captions }
       section['description1'] = diversified['description'][i]
       section['description2'] = returns['description_summary'][i]
       section['description3'] = returns['description_details'][i]
       section['name'] = portfolio_name_list[i]
       key = "portfolio_{}".format(i)
       portfolio[key] = section

   config = INI.init()
   INI.write_section(config,"summary",**summary)
   for key in portfolio.keys() :
       values = portfolio[key]
       if not isinstance(values,dict) :
          values = values.to_dict()
       INI.write_section(config,key,**values)
   config.write(open(output_file, 'w'))
   logging.info('saving file {}'.format(output_file))

if __name__ == '__main__' :

   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()

   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   stock_background = filter(lambda x : 'stock_background' in x, ini_list)
   benchmark = filter(lambda x : 'benchmark' in x, ini_list)
   file_list = env.list_filenames('local/historical_prices/*pkl')

   local_dir = "{pwd_parent}/local".format(**vars(env))
   data_store = '{}/images'.format(local_dir)
   data_store = '../local/images'
   input_file = env.list_filenames('local/method*portfolios.ini')
   if len(input_file) > 0 :
      input_file = input_file[0]
   output_file = "{pwd_parent}/local/report_generator.ini".format(**vars(env))
   if len(env.argv) > 0 :
      input_file = env.argv[0]
   if len(env.argv) > 1 :
      output_file = env.argv[1]

   main(local_dir, output_file)
