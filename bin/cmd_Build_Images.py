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
from libGraph import LINE, BAR, POINT, save, HELPER as GRAPH
'''
   Graph portfolios to determine perfomance, risk, diversification
'''
class EXTRACT() :
    _singleton = None
    def __init__(self, _env, local_dir, data_store, category,input_file,output_file,background,benchmark,repo) :
        self._env = _env
        self.local_dir = local_dir
        self._data_store = data_store
        self.category = category
        self.input_file = input_file
        self.output_file = output_file
        self.background = background
        self._benchmark = benchmark
        self.repo = repo
        msg = vars(self)
        for i, key in enumerate(sorted(msg)) :
            value = msg[key]
            if isinstance(value,list) and len(value) > 10 :
               value = value[:10]
            logging.info((i,key, value))

    @classmethod
    def instance(cls, **kwargs) :
        if not (cls._singleton is None) :
           return cls._singleton
        target = 'env'
        _env = globals().get(target,None)
        target = "local_dir"
        local_dir = globals().get(target,None)
        target = 'data_store'
        data_store = globals().get(target,'')
        if not isinstance(data_store,str) :
           data_store = str(data_store)
        target = 'input_file'
        input_file = globals().get(target,'')
        if not isinstance(input_file,list) :
           input_file = [input_file]
        if len(_env.argv) > 0 :
           input_file = _env.argv[0]
        target = 'output_file'
        output_file = globals().get(target,'')
        if len(_env.argv) > 1 :
           output_file = _env.argv[1]
        target = 'background'
        background = globals().get(target,[])
        background = list(background)
        target = 'category'
        category = globals().get(target,[])
        category = list(category)
        target = 'benchmark'
        benchmark = globals().get(target,[])
        benchmark = list(benchmark)
        target = "repo_stock"
        repo_stock = globals().get(target,[])
        target = "repo_fund"
        repo_fund = globals().get(target,[])
        repo = repo_stock + repo_fund
        
        _env.mkdir(data_store)
        cls._singleton = cls(_env,local_dir,data_store,category,input_file,output_file,background,benchmark,repo)
        return cls._singleton
    @classmethod
    def readPortfolio(cls) :
        portfolio = EXTRACT.instance().input_file
        logging.info('reading file {}'.format(portfolio))
        ret = {}
        for path, section, key, weight in INI.loadList(*[portfolio]) :
            if section.startswith('dep_') :
               continue
            if section not in ret :
               ret[section] = {}
            ret[section][key] = float(weight[0])
        return ret
class EXTRACT_PRICES() :
    @classmethod
    def isReserved(cls,value) :
        flag_1 = value.startswith('legend')
        flag_2 = value.startswith('reference')
        flag_3 = 'portfolio' in value
        return flag_1 or flag_2 or flag_3
    @classmethod
    def read(cls, value_list) :
        repo = EXTRACT.instance().repo
        ret = {}
        for name, data in STOCK_TIMESERIES.read(repo, value_list) :
            logging.info((name,type(data),data.shape))
            ret[name] = data
        return ret
    @classmethod
    def smartMassage(cls,data) :
        data.fillna(method='backfill',inplace=True)
        data = data / data.iloc[0]
        data = data - 1
        key_list = sorted(data.columns.values)
        ticker_list = filter(lambda ref : cls.isReserved(ref), key_list)
        reserved = data[ticker_list]
        ticker_list = filter(lambda ref : cls.isReserved(ref) == False, key_list)
        ret = data[ticker_list]
        ret = FINANCE.new_graphReturns(ret)
        ret.fillna(method='backfill',inplace=True)
        ret = pd.concat([ret,reserved],axis=1, sort=False)
        logging.info(ret)
        return ret
class TRANSFORM_PRICES() :
    _prices = 'Adj Close'
    @classmethod
    def adjClose(cls,data) :
        stock_list = sorted(data)
        price_list = map(lambda stock : pd.DataFrame(data[stock])[cls._prices], stock_list)
        price_list = list(price_list)
        ret = dict(zip(stock_list, price_list))
        ret = pd.DataFrame(ret, columns=stock_list)
        logging.info(ret.head(3))
        logging.info(ret.tail(3))
        return ret
class EXTRACT_SUMMARY() :
    _background_cache = None
    _floats_in_summary = ['CAGR', 'GROWTH', 'LEN', 'RISK', 'SHARPE']
    @classmethod
    def read(cls) :
        load_file = EXTRACT.instance().background
        if not (cls._background_cache is None) :
           logging.info('reading cache {}'.format(load_file))
           return cls._background_cache
        logging.info('reading file {}'.format(load_file))
        ret = {}
        for path, key, stock, value in INI.loadList(*load_file) :
            if "File Creation Time" in stock :
                continue
            if stock not in ret :
               ret[stock] = {}
            if key in cls._floats_in_summary :
               if '=' in value[0] :
                  value = [0]
               value = float(value[0])
            else :
               value = ', '.join(value)
            ret[stock][key] = value
        ret = pd.DataFrame(ret).T
        cls._background_cache = ret
        return ret
class TRANSFORM_SUMMARY() :
      columns = ['CAGR', 'GROWTH','RISK','SHARPE']
      @classmethod
      def step01(cls, stock_list) :
          ret = EXTRACT_SUMMARY.read().loc[stock_list]
          ret['RETURNS'] = ret['RETURNS'].astype(float).round(4)
          ret['GROWTH'] = ret['GROWTH'].astype(float).round(4)
          ret['Initial Balance'] = 10000
          ret['Final Balance'] = ret['Initial Balance']*ret['GROWTH']
          return ret
      @classmethod
      def step02(cls, ret) :
          ret['weighted_RETURNS'] = ret['RETURNS'] * ret['weight']
          ret['weighted_SHARPE'] = ret['SHARPE'] * ret['weight']
          ret['weighted_RISK'] = ret['RISK'] * ret['weight']
          ret['weighted_CAGR'] = ret['CAGR'] * ret['weight']
          ret['weighted_GROWTH'] = ret['GROWTH'] * ret['weight']
          ret['weighted_Initial Balance'] = ret['Initial Balance']*ret['weight']
          ret['weighted_Final Balance'] = ret['Final Balance']*ret['weight']
          return ret
      @classmethod
      def step03(cls, ret) :
          columns = ret.columns.values
          logging.info(columns)
          weighted_columns = filter(lambda x : x.startswith('weighted_'), columns)
          weighted_columns = list(weighted_columns)
          new_columns = map(lambda x : x.replace('weighted_',''), weighted_columns)
          new_columns = list(new_columns)
          temp = map(lambda x : ret[x], weighted_columns)
          temp = map(lambda x : cls._refactorSum(x), temp)
          temp = map(lambda x : round(x,4), temp)
          portfolio = dict(zip(new_columns, temp))
          portfolio = pd.DataFrame([portfolio])
          return portfolio
      @classmethod
      def _refactorSum(cls, column) :
          ret = column.T.sum()
          return ret
      @classmethod
      @trace
      def refactor(cls, weights) :
          stock_list = sorted(weights.columns.values)
          ret = cls.step01(stock_list)
          ret['weight'] = weights.T
          ret = cls.step02(ret)
          portfolio = cls.step03(ret)
          logging.info(portfolio)
          ret.loc['portfolio'] = portfolio.T[0]
          return ret

class EXTRACT_SECTOR() :
    _cache = None
    @classmethod
    def _readSector(cls) :
        load_file = EXTRACT.instance().category
        if not (cls._cache is None) :
           logging.info('reading cache {}'.format(load_file))
           return cls._cache
        logging.info('reading file {}'.format(load_file))
        ret = {}
        for path, section, key, ticker_list in INI.loadList(*load_file) :
            if 'fund' in path :
               key = '{} ({})'.format(section,key)
            for ticker in ticker_list :
                if ticker not in ret : 
                   ret[ticker] = {}
                ret[ticker]['Sector'] = key
        cls._cache = ret
        return ret
    @classmethod
    def read(cls,stock) :
        enrich = cls._readSector().get(stock,{})
        target = 'Sector'
        ret = enrich.get(target,None)
        if ret is None :
           ret = enrich.get('Category','Unknown')
        return ret
class TRANSFORM_SECTOR() :
    @classmethod
    def bySector(cls,summary) :
        summary = summary[summary['weight'] > 0.0]
        ret = filter(lambda key : EXTRACT_PRICES.isReserved(key) == False, summary.index.values)
        ret = list(ret)
        ret = summary.loc[ret]
        sector_list = set(ret['sector'])
        sector_list = sorted(list(sector_list))
        for sector in sector_list :
            value = ret[ret['sector'] == sector]
            logging.info(value)
            yield sector, value
class EXTRACT_BENCHMARK() :
    FUNDS = 'NASDAQMUTFUND'
    SNP = 'SNP500'
    @classmethod
    def read(cls) :
        benchmark = EXTRACT.instance()._benchmark
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
    @classmethod
    def tickers(cls) :
        bench_list = cls.read()
        ret = bench_list[cls.FUNDS]
        snp = bench_list[cls.SNP]
        ret += snp
        return ret
    @classmethod
    def process(cls) :
        bench_list = cls.read()
        SNP = 'SNP500'
        snp = bench_list[SNP]
        gcps = snp[0]
        ret = cls.tickers()
        ret = TRANSFORM_SUMMARY.step01(ret)
        ret.rename(index={gcps:SNP},inplace=True)
        logging.info(ret)
        return ret
    @classmethod
    def prices(cls) :
        bench = EXTRACT_BENCHMARK.tickers()
        bench_list = EXTRACT_BENCHMARK.read()
        SNP = 'SNP500'
        snp = bench_list[SNP]
        gcps = snp[0]

        raw_prices = EXTRACT_PRICES.read(bench)
        ret = TRANSFORM_PRICES.adjClose(raw_prices)
        ret.rename(columns={gcps:SNP},inplace=True)
        return ret
class TRANSFORM_STOCK() :
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
    def enrichSector(cls,ret) :
        stock_list = cls.parseStockList(ret)
        ret = map(lambda stock : EXTRACT_SECTOR.read(stock), stock_list)
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

class TRANSFORM_PORTFOLIO() :
    meta_columns = ['returns', 'risk', 'sharpe']
    name_format_1 = "portfolio_diversified_{}"
    name_format_2 = "portfolio_returns_{}"
    name_format_3 = 'legend_{}'
    sector_columns = ['weighted returns','weighted risk','weighted sharpe']
    stock_columns = ['RETURNS', 'RISK', 'SHARPE', 'LEN', 'weighted_RETURNS', 'weighted_RISK', 'weighted_SHARPE']
    rename_columns = {"weighted_RETURNS": "weighted returns"
                       , 'weighted_RISK' : 'weighted risk'
                       , 'weighted_SHARPE' : 'weighted sharpe'
                       , 'RETURNS' :'returns'
                       , 'RISK' : 'risk'
                       , 'SHARPE' : 'sharpe'}
    @classmethod
    def getPortfolioPrice(cls,weights,prices) :
        logging.info(weights.shape)
        logging.info(prices.shape)
        ret = prices.fillna(0)
        ret = weights.dot(prices.T).dropna(how="all").T
        return ret
    @classmethod
    def parseWeights(cls, portfolio) :
        ret = pd.DataFrame([portfolio])
        stock_list = filter(lambda x : x not in cls.meta_columns, ret.columns.values)
        ret = ret[list(stock_list)].T
        ret.rename(columns={0:'weight'},inplace=True)
        ret = ret[ret['weight'] > 0.0].T 
        logging.info(ret)
        return ret
    @classmethod
    def getWeights(cls,data) :
        ret_weights = {}
        ret_sector = {}
        for sector, summary in TRANSFORM_SECTOR.bySector(data) :
            _W = summary['weight']
            _W = _W*100
            _W = _W.round(1)
            ret_sector[sector] = round(sum(_W),1)

            _t = summary[['NAME']]
            _t['weight']  =_W
            _t['ticker'] = _t.index.values
            _t.rename(columns={'NAME':'Name'},inplace=True)
            _t = _t.T.to_dict().values()
            _t = list(_t)
            ret_weights[sector] = _t

        logging.info(ret_sector)
        logging.info(ret_weights)
        return ret_weights, ret_sector
    @classmethod
    def getSharpes(cls,data) :
        ret_sector = {}
        ret_stock = {}
        for sector, summary in TRANSFORM_SECTOR.bySector(data) :
            table = summary[cls.rename_columns.keys()]
            table = table.sum().round(2)
            table.rename(index=cls.rename_columns, inplace=True)
            ret_sector[sector] = table.to_dict()

            wtf = summary[cls.stock_columns]
            wtf = wtf.round(2).T
            wtf.rename(index=cls.rename_columns, inplace=True)
            ret_stock[sector] = wtf.to_dict()

        logging.info(ret_sector)
        logging.info(ret_stock)
        return ret_stock, ret_sector

    @classmethod
    def getNames(cls, curr, name) :
        name_diversified = cls.name_format_1.format(curr)
        name_returns = cls.name_format_2.format(curr)
        name_legend = cls.name_format_3.format(name)
        name_list = ['portfolio', 'diversified', 'returns', 'legend']
        return dict(zip(name_list,[name, name_diversified, name_returns,name_legend]))

    @classmethod
    def _find(cls) :
        ret = EXTRACT.readPortfolio()
        logging.debug(ret)
        key_list = sorted(ret.keys())
        for curr, key in enumerate(key_list) :
            logging.info((curr+1, key,ret[key]))
            meta_name_list = cls.getNames(curr+1, key)
            yield key, ret[key], meta_name_list
    @classmethod
    def find(cls) :
        for name, weights, meta_name_list in cls._find() :
            weights = cls.parseWeights(weights)
            raw_prices = EXTRACT_PRICES.read(weights.columns.values)
            prices = TRANSFORM_PRICES.adjClose(raw_prices)
            prices[meta_name_list['legend']] = cls.getPortfolioPrice(weights,prices)
            returns = TRANSFORM_STOCK.summarizeReturns(prices)

            logging.info((weights,meta_name_list))
            # Portfolio	Initial Balance	Final Balance	CAGR	Stdev	Best Year	Worst Year	Max. Drawdown	Sharpe Ratio	Sortino Ratio	US Mkt Correlation
            # 'Portfolio','Initial Balance','Final Balance','CAGR','Stdev','Sharpe Ratio','Sortino Ratio','US Mkt Correlation'
            meta = TRANSFORM_SUMMARY.refactor(weights)
            logging.info(meta)
            meta['sector'] = TRANSFORM_STOCK.enrichSector(meta)
            logging.info(meta.T)
            yield weights, meta_name_list, prices, meta, returns

class LOAD() :

    @classmethod
    def config(cls,summary,portfolio) :
        save_file = EXTRACT.instance().output_file
        ret = INI.init()
        INI.write_section(ret,"summary",**summary)
        for key in portfolio.keys() :
            values = portfolio[key]
            if not isinstance(values,dict) :
               values = values.to_dict()
            INI.write_section(ret,key,**values)
        ret.write(open(save_file, 'w'))
        logging.info('saving file {}'.format(save_file))
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
        ret_weights, ret_sector = TRANSFORM_PORTFOLIO.getWeights(data)
        ret.add(graph=ret_sector,data=ret_weights,name=name)
        return ret
    @classmethod
    def appendSharpe(cls,name,data,ret) :
        ret_stock, ret_sector = TRANSFORM_PORTFOLIO.getSharpes(data)
        ret.add(graph=ret_sector,data=ret_stock,name=name)
        return ret
    @classmethod
    def appendPrices(cls,name,data,ret) :
        logging.info(data.columns.values)
        prices = EXTRACT_PRICES.smartMassage(data)
        ret.add(graph=prices,name=name)
        return ret
def process() :

    price_summary = EXTRACT_BENCHMARK.prices()
    text_summary = EXTRACT_BENCHMARK.process().T
    _returns = Group()
    distribution = Group()
    _sharpe = Group()
    _portfolio_name_list = []
    for weights, names, prices, summary, returns in TRANSFORM_PORTFOLIO.find() :
        _portfolio_name_list.append(names['portfolio'])
        distribution = Group.appendDiversify(names['diversified'],summary,distribution)
        _sharpe = Group.appendSharpe(weights,summary,_sharpe)
        _returns = Group.appendPrices(names['returns'],prices, _returns)
        
        target_From = 'legend_{portfolio}'.format(**names)
        target_To = names['portfolio']

        price_summary[target_From] = prices[target_From]
        text_summary[target_To] = summary.loc['portfolio']

    price_summary = EXTRACT_PRICES.smartMassage(price_summary)
    logging.info(price_summary)

    returns = _returns()
    returns['description_summary'] = _sharpe().get('graph',[])
    returns['description_details'] = _sharpe().get('description',[])

    graph_list = returns['graph']
    for i, value in enumerate(graph_list) :
        value['reference_SNP'] = price_summary[EXTRACT_BENCHMARK.SNP]
        logging.info(value)

    diversified = distribution()

    sharpe_summary = text_summary
    text_summary = text_summary.T
    text_summary['NAME'] = text_summary['NAME'].fillna("Unavailable")
    text_summary.fillna(0, inplace=True)
    text_summary['Initial Balance'] = text_summary['Initial Balance'].round(2)
    text_summary['Final Balance'] = text_summary['Final Balance'].round(2)
    rename = {'RISK':'Stdev','NAME':'Name','SHARPE':'Sharpe Ratio','GROWTH':'Growth'}
    text_summary.rename(columns=rename,inplace=True)
    text_summary = text_summary.T.to_dict()

    logging.info(returns) 
    logging.info(diversified)
    logging.info(price_summary) 
    logging.info(sharpe_summary) # should be price list, that does not make sense
    logging.info(text_summary)
    logging.info(_portfolio_name_list)
    return returns, diversified, price_summary, sharpe_summary, text_summary, _portfolio_name_list

@exit_on_exception
@trace
def main() :
   returns, diversified, graph_summary, graph_portfolio_sharpe_list, text_summary, portfolio_name_list = process()
   summary_path_list = []
   logging.info(graph_portfolio_sharpe_list)
   POINT.plot(graph_portfolio_sharpe_list,x='RISK',y='RETURNS',ylabel="Returns", xlabel="Risk", title="Sharpe Ratio")
   SHARPE = LINE.plot_sharpe(ratio=1)
   SHARPE.plot.line(style='b:', label='sharpe ratio 1',alpha=0.3)
   SHARPE = LINE.plot_sharpe(ratio=2)
   SHARPE.plot.line(style='r:',label='sharpe ratio 2',alpha=0.3)

   local_dir = EXTRACT.instance().local_dir
   path = "{}/images/portfolio_sharpe.png".format(local_dir)
   save(path,loc="lower right")
   summary_path_list.append(path)
   LINE.plot(graph_summary, title="Returns")
   GRAPH.tick_right()
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
   summary['table'] = text_summary
   portfolio = {}
   for i, value in enumerate(local_diversify_list) :
       images = [ local_diversify_list[i], local_returns_list[i] ]
       captions = [ "portfolio diversity {}", "portfolio returns {}"]
       captions = map(lambda x : x.format(portfolio_name_list[i]), captions)
       captions = map(lambda x : x.replace('_', ' '), captions)
       captions = list(captions)
       section = { "images" : images, "captions" : captions }
       section['description1'] = diversified['description'][i]
       section['description2'] = returns['description_summary'][i]
       section['description3'] = returns['description_details'][i]
       section['name'] = portfolio_name_list[i]
       key = "portfolio_{}".format(i)
       portfolio[key] = section

   LOAD.config(summary,portfolio)

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
   category = filter(lambda x : 'stock_by_sector.ini' in x or 'fund_by_type.ini' in x, ini_list)
   background = filter(lambda x : 'background.ini' in x, ini_list)
   benchmark = filter(lambda x : 'benchmark' in x, ini_list)
   repo_stock = env.list_filenames('local/historical_prices/*pkl')
   repo_fund = env.list_filenames('local/historical_prices_fund/*pkl')
   input_file = env.list_filenames('local/method*portfolios.ini')

   local_dir = "{pwd_parent}/local".format(**vars(env))
   data_store = '{}/images'.format(local_dir)
   data_store = '../local/images'
   output_file = "{pwd_parent}/local/report_generator.ini".format(**vars(env))

   main()
   #ret = EXTRACT.readSummary()
   #print(ret)
   #print(ret['RISK'].describe())
   #temp = ret.sort_values(['RISK'])
   #print(temp)
   #print(temp['RISK'].std())

