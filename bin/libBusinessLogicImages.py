# -*- coding: utf-8 -*-
"""
Created on Thu Apr 21 12:20:27 2022

@author: emers
"""
import logging as log
import pandas as pd

from libBusinessLogic import INI_READ
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE

class EXTRACT() :
    @classmethod
    def readPortfolio(cls,input_file) :
        portfolio = input_file
        ret = {}
        for section, key, weight in INI_READ.read(*[portfolio]) :
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
    def read(cls, value_list, repo_stock) :
        log.info(value_list)
        repo = repo_stock
        ret = {}
        for name, data in STOCK_TIMESERIES.read(repo, value_list) :
            log.info((name,type(data),data.shape))
            ret[name] = data
        return ret
    @classmethod
    def smartMassage(cls,data) :
        log.info(data)
        data.fillna(method='backfill',inplace=True)
        log.info(data)
        data = data / data.iloc[0]
        log.info(data)
        data = data - 1
        key_list = sorted(data.columns.values)
        ticker_list = filter(lambda ref : cls.isReserved(ref), key_list)
        reserved = data[ticker_list]
        ticker_list = filter(lambda ref : cls.isReserved(ref) == False, key_list)
        ret = data[ticker_list]
        ret = FINANCE.new_graphReturns(ret)
        ret.fillna(method='backfill',inplace=True)
        ret = pd.concat([ret,reserved],axis=1, sort=False)
        log.info(ret)
        return ret
class TRANSFORM_PRICES() :
    _prices = 'Adj Close'
    @classmethod
    def adjClose(cls,data) :
        stock_list = sorted(list(set(data)))
        price_list = { stock : pd.DataFrame(data[stock])[cls._prices] for stock in stock_list }
        ret = pd.DataFrame.from_dict(price_list)
        log.info(ret.head(3))
        log.info(ret.tail(3))
        return ret
class EXTRACT_SUMMARY() :
    _background_cache = None
    _floats_in_summary = ['CAGR', 'GROWTH', 'LEN', 'RISK', 'SHARPE']
    @classmethod
    def read(cls,background) :
        load_file = background
        if not (cls._background_cache is None) :
           log.info('reading cache {}'.format(load_file))
           return cls._background_cache
        log.info('reading file {}'.format(load_file))
        ret = {}
        for key, stock, value in INI_READ.read(*load_file) :
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
      def step01(cls, stock_list,background) :
          ret = EXTRACT_SUMMARY.read(background).loc[stock_list]
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
          log.info(columns)
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
      def refactor(cls, weights,background) :
          stock_list = sorted(weights.columns.values)
          ret = cls.step01(stock_list,background)
          ret['weight'] = weights.T
          ret = cls.step02(ret)
          portfolio = cls.step03(ret)
          log.info(portfolio)
          ret.loc['portfolio'] = portfolio.T[0]
          return ret

class EXTRACT_SECTOR() :
    _cache = None
    @classmethod
    def _readSector(cls,category) :
        load_file = category
        if not (cls._cache is None) :
           log.debug('reading cache {}'.format(load_file))
           return cls._cache
        log.info('reading file {}'.format(load_file))
        ret = {}
        for path, section, key, ticker_list in INI_READ.read(*load_file) :
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
            log.info(value)
            yield sector, value
class EXTRACT_BENCHMARK() :
    FUNDS = 'NASDAQMUTFUND'
    SNP = 'SNP500'
    @classmethod
    def read(cls,benchmark) :
        ret = {}
        for section, key, stock_list in INI_READ.read(*benchmark) :
            #if section not in ['MOTLEYFOOL', 'Index'] : continue
            if section not in ['PERSONAL', 'Index'] : continue
            if section == 'MOTLEYFOOL' :
               if 'NASDAQ' not in key : continue
               if 'FUND' not in key : continue
            if section == 'Index' :
               if '500' not in key : continue
            ret[key] = stock_list
        log.debug(ret)
        return ret
    @classmethod
    def tickers(cls,benchmark) :
        bench_list = cls.read(benchmark)
        log.debug(bench_list)
        ret = bench_list.get(cls.FUNDS,[])
        snp = bench_list.get(cls.SNP,[])
        ret += snp
        log.debug(ret)
        return ret
    @classmethod
    def process(cls,background,benchmark) :
        bench_list = cls.read(benchmark)
        SNP = 'SNP500'
        snp = bench_list.get(SNP,[])
        gcps = None
        if len(snp) > 0 :
            gcps = snp[0]
        ret = cls.tickers(benchmark)
        ret = TRANSFORM_SUMMARY.step01(ret,background)
        ret.rename(index={gcps:SNP},inplace=True)
        log.info(ret)
        return ret
    @classmethod
    def find_prices(cls,repo_stock,benchmark) :
        bench = cls.tickers(benchmark)
        bench_list = cls.read(benchmark)
        SNP = 'SNP500'
        snp = bench_list.get(SNP,[])
        gcps = None 
        if len(snp) > 0 :
            gcps = snp[0]

        raw_prices = EXTRACT_PRICES.read(bench,repo_stock)
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
        log.info(ret)
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
        log.info(ret)
        return ret
    @classmethod
    def enrichWeight(cls,weight,ret) :
        stock_list = cls.parseStockList(ret)
        ret = map(lambda stock : weight.get(stock,0.0), stock_list)
        ret = dict(zip(stock_list,ret))
        ret = pd.Series(ret)
        log.info(ret)
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
        log.info(weights.shape)
        log.info(prices.shape)
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
        log.info(ret)
        return ret
    @classmethod
    def getWeights(cls,data) :
        ret_weights = {}
        ret_sector = {}
        for sector, summary in TRANSFORM_SECTOR.bySector(data) :
            _W = summary['weight'].copy()
            _W = _W*100
            _W = _W.round(1)
            ret_sector[sector] = round(sum(_W),1)

            _t = summary[['NAME']].copy()
            _t['weight']  =_W
            _t['ticker'] = _t.index.values
            _t = cls.cleanup(_t)
            for idx, name in enumerate(_W.index.values) :
                _t.loc[name,_W.name] = _W.loc[name]
                _t.loc[name,'ticker'] = name
            _t.rename(columns={'NAME':'Name'},inplace=True)
            _t = _t.T.to_dict().values()
            _t = list(_t)
            ret_weights[sector] = _t

        log.info(ret_sector)
        log.info(ret_weights)
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

        log.info(ret_sector)
        log.info(ret_stock)
        return ret_stock, ret_sector

    @classmethod
    def getNames(cls, curr, name) :
        name_diversified = cls.name_format_1.format(curr)
        name_returns = cls.name_format_2.format(curr)
        name_legend = cls.name_format_3.format(name)
        name_list = ['portfolio', 'diversified', 'returns', 'legend']
        return dict(zip(name_list,[name, name_diversified, name_returns,name_legend]))

    @classmethod
    def _find(cls,input_file) :
        ret = EXTRACT.readPortfolio(input_file)
        log.debug(ret)
        key_list = sorted(ret.keys())
        for curr, key in enumerate(key_list) :
            log.info((curr+1, key,ret[key]))
            meta_name_list = cls.getNames(curr+1, key)
            yield key, ret[key], meta_name_list
    @classmethod
    def find(cls,input_file,repo_stock,background) :
        for name, weights, meta_name_list in cls._find(input_file) :
            weights = cls.parseWeights(weights)
            if len(weights.columns.values) == 0 :
                continue
            raw_prices = EXTRACT_PRICES.read(weights.columns.values,repo_stock)
            prices = TRANSFORM_PRICES.adjClose(raw_prices)
            prices[meta_name_list['legend']] = cls.getPortfolioPrice(weights,prices)
            returns = TRANSFORM_STOCK.summarizeReturns(prices)

            log.info((weights,meta_name_list))
            # Portfolio	Initial Balance	Final Balance	CAGR	Stdev	Best Year	Worst Year	Max. Drawdown	Sharpe Ratio	Sortino Ratio	US Mkt Correlation
            # 'Portfolio','Initial Balance','Final Balance','CAGR','Stdev','Sharpe Ratio','Sortino Ratio','US Mkt Correlation'
            meta = TRANSFORM_SUMMARY.refactor(weights,background)
            log.info(meta)
            meta['sector'] = TRANSFORM_STOCK.enrichSector(meta)
            log.info(meta.T)
            yield weights, meta_name_list, prices, meta, returns
    @classmethod
    def cleanup(cls,ret) :
        ret['NAME'] = ret['NAME'].str.replace("'", "")
        ret['NAME'] = ret['NAME'].str.replace(" - ", ", ")
        ret['NAME'] = ret['NAME'].str.replace(", ", " ")
        ret['NAME'] = ret['NAME'].str.replace("Common Stock", "Cmn Stk")
        ret['NAME'] = ret['NAME'].str.replace("Limited", "Ltd.")
        ret['NAME'] = ret['NAME'].str.replace("Corporation", "Corp.")
        ret['NAME'] = ret['NAME'].str.replace("Pharmaceuticals", "Pharm.")
        ret['NAME'] = ret['NAME'].str.replace("Technologies", "Tech.")
        ret['NAME'] = ret['NAME'].str.replace("Technology", "Tech.")
        ret['NAME'] = ret['NAME'].str.replace("International", "Int.")
        return ret


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
        log.info(data.columns.values)
        prices = EXTRACT_PRICES.smartMassage(data)
        ret.add(graph=prices,name=name)
        return ret

def process(background,repo_ticker,benchmark,input_file) :

    price_summary = EXTRACT_BENCHMARK.find_prices(repo_ticker,benchmark)
    text_summary = EXTRACT_BENCHMARK.process(background,benchmark).T
    _returns = Group()
    distribution = Group()
    _sharpe = Group()
    _portfolio_name_list = []
    for weights, names, prices, summary, returns in TRANSFORM_PORTFOLIO.find(input_file,repo_ticker,background) :
        _portfolio_name_list.append(names['portfolio'])
        distribution = Group.appendDiversify(names['diversified'],summary,distribution)
        _sharpe = Group.appendSharpe(weights,summary,_sharpe)
        _returns = Group.appendPrices(names['returns'],prices, _returns)
        
        target_From = 'legend_{portfolio}'.format(**names)
        target_To = names['portfolio']

        price_summary[target_From] = prices[target_From]
        text_summary[target_To] = summary.loc['portfolio']

    price_summary = EXTRACT_PRICES.smartMassage(price_summary)
    log.info(price_summary)

    returns = _returns()
    returns['description_summary'] = _sharpe().get('graph',[])
    returns['description_details'] = _sharpe().get('description',[])

    graph_list = returns['graph']
    for i, value in enumerate(graph_list) :
        value['reference_SNP'] = price_summary[EXTRACT_BENCHMARK.SNP]
        log.info(value)

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

    log.info(returns) 
    log.info(diversified)
    log.info(price_summary) 
    log.info(sharpe_summary) # should be price list, that does not make sense
    log.info(text_summary)
    log.info(_portfolio_name_list)
    return returns, diversified, price_summary, sharpe_summary, text_summary, _portfolio_name_list
