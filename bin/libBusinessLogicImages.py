# -*- coding: utf-8 -*-
"""
Created on Thu Apr 21 12:20:27 2022

@author: emers
"""
import logging as log
import pandas as pd

from libBusinessLogic import INI_READ,LOAD_HISTORICAL_DATA
from libFinance import HELPER as FINANCE

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
    @classmethod
    def find_value(cls,key,**data) :
        ret = data.get(key,None)
        if ret is None :
            ret = data.get(key.lower(),None)
        if ret is None :
            ret = data.get(key.upper(),None)
        if ret is None :
            log.warn("key : '{}' , not in {}".format(key,sorted(data)))
        return ret
class EXTRACT_PRICES() :
    @classmethod
    def isReserved(cls,value) :
        flag_1 = value.lower().startswith('legend')
        flag_2 = value.lower().startswith('reference')
        flag_3 = 'portfolio' in value.lower()
        return flag_1 or flag_2 or flag_3
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
        if '^GSPC' in ret.columns.values :
            ret.rename(columns={'^GSPC':'SNP500'}, inplace = True)
        log.info(ret)
        log.info(ret.columns.values)
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
      def step01(cls, ticker_list,background) :
          column_list = [key.lower() for key in ticker_list]
          ret = EXTRACT_SUMMARY.read(background).loc[column_list]
          ret.columns = ret.columns.str.upper()
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
          weighted_columns = [x for x in columns if x.startswith('weighted_') ]
          new_columns = [ x.replace('weighted_','') for x in weighted_columns ]
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
          ticker_list = sorted(weights.columns.values)
          ret = cls.step01(ticker_list,background)
          ret['weight'] = weights.T
          ret = cls.step02(ret)
          portfolio = cls.step03(ret)
          log.info(portfolio)
          ret.loc['portfolio'] = portfolio.T[0]
          return ret

class EXTRACT_SECTOR() :
    sector_list = ['basic materials','communication services','consumer cyclical','consumer defensive','energy','financial services','healthcare','industrials','real estate','technology','utilities']
    def __init__(self,load_file) :
        log.info('reading file {}'.format(load_file))
        self._cache = {}
        for sector, ticker in self._readSectorStock(load_file) :
            if ticker not in self._cache :
                self._cache[ticker] = {}
            self._cache[ticker]['Sector'] = sector
    @classmethod
    def _readSectorStock(cls,load_file) :
        for section, key, ticker_list in INI_READ.read(*load_file) :
            if key not in cls.sector_list : continue
            for ticker in ticker_list :
                yield key, ticker.lower()
    @classmethod
    def _readSectorFund(cls,load_file) :
        for section, key, ticker_list in INI_READ.read(*load_file) :
            for ticker in ticker_list :
                yield '{} ({})'.format(section,key), ticker
    def read(self,stock) :
        enrich = self._cache.get(stock,{})
        target = 'Sector'
        ret = enrich.get(target,None)
        if ret is None :
           ret = enrich.get('Category','Unknown')
        return ret
class TRANSFORM_SECTOR() :
    @classmethod
    def bySector(cls,summary) :
        summary = summary[summary['weight'] > 0.0]
        ret = [ key for key in summary.index.values if EXTRACT_PRICES.isReserved(key) == False]
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
    def is_worthy(cls,section, key) :
        if section not in ['PERSONAL', 'Index'] : return False
        if section == 'MOTLEYFOOL' :
           if 'NASDAQ' not in key : return False
           if 'FUND' not in key : return False
        if section == 'Index' :
           if '500' not in key : return False
        return True
    @classmethod
    def read(cls,benchmark) :
        log.info(benchmark)
        ret = {}
        for section, key, ticker_list in INI_READ.read(*benchmark) :
            if cls.is_worthy(section, key) == False : continue
            ret[key] = ticker_list
        log.info(ret)
        return ret
    @classmethod
    def tickers(cls,benchmark) :
        bench_list = cls.read(benchmark)
        log.debug(bench_list)
        ret = EXTRACT.find_value(cls.FUNDS, **bench_list)
        snp = EXTRACT.find_value(cls.SNP, **bench_list)
        ret += snp
        log.debug(ret)
        return ret
    @classmethod
    def process(cls,background,benchmark) :
        bench_list = cls.read(benchmark)
        snp = EXTRACT.find_value(cls.SNP, **bench_list)
        gcps = None
        if len(snp) > 0 :
            gcps = snp[0]
        log.info("snp : {}".format(gcps))
        ret = cls.tickers(benchmark)
        log.info("snp : {}".format(ret))
        ret = TRANSFORM_SUMMARY.step01(ret,background)
        ret.rename(index={gcps:cls.SNP},inplace=True)
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

        loader = LOAD_HISTORICAL_DATA(repo_stock,LOAD_HISTORICAL_DATA.default_column)
        ret = loader.act(bench)
        ret.rename(columns={gcps:SNP},inplace=True)
        return ret
class TRANSFORM_TICKER() :
    @classmethod
    def parseTickerList(cls,data) :
        if isinstance(data,dict) :
           return sorted(data)
        ret = sorted(data.index.values)
        if not isinstance(ret[0],str) :
           ret = sorted(data.columns.values)
        log.info(ret)
        return ret
    @classmethod
    def summarizeReturns(cls,data) :
        ticker_list= cls.parseTickerList(data)
        ret = { stock : FINANCE.findDailyReturns(data[stock]) for stock in ticker_list}
        return ret
    @classmethod
    def enrichSector(cls,category, ret) :
        ticker_list= cls.parseTickerList(ret)
        sector = EXTRACT_SECTOR(category)
        ret = { ticker : sector.read(ticker) for ticker in ticker_list}
        ret = pd.Series(ret)
        log.info(ret)
        return ret
    @classmethod
    def enrichWeight(cls,weight,ret) :
        ticker_list = cls.parseTickerList(ret)
        ret = { ticker : weight.get(ticker,0.0) for ticker in ticker_list}
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
    def transform_portfolio_prices_by_weight(cls,weights,prices) :
        log.info(weights.shape)
        log.info(prices.shape)
        ret = prices.fillna(0)
        ret = weights.dot(prices.T).dropna(how="all").T
        return ret
    @classmethod
    def parseWeights(cls, portfolio) :
        ret = pd.DataFrame([portfolio])
        ticker_list = [ x for x in ret.columns.values if x not in cls.meta_columns]
        ret = ret[ticker_list].T
        ret.rename(columns={0:'weight'},inplace=True)
        ret = ret[ret['weight'] > 0.0].T 
        log.info(ret)
        return ret
    @classmethod
    def transformSummaryByWeight(cls,summary) :
        weight = summary['weight'].copy()
        weight = weight*100
        weight = weight.round(1)

        ticker_list = summary[['NAME']].copy()
        ticker_list['weight'] = weight
        ticker_list['ticker'] = ticker_list.index.values
        ticker_list = cls.cleanup(ticker_list)
        log.info(weight)
        log.info(ticker_list)
        return weight, ticker_list
    @classmethod
    def getWeights(cls,data) :
        ret_weights = {}
        ret_sector = {}
        for sector, summary in TRANSFORM_SECTOR.bySector(data) :
            _weight, _ticker_list = cls.transformSummaryByWeight(summary)
            ret_sector[sector] = round(sum(_weight),1)
            for idx, name in enumerate(_weight.index.values) :
                _ticker_list.loc[name,_weight.name] = _weight.loc[name]
                _ticker_list.loc[name,'ticker'] = name
            _ticker_list.rename(columns={'NAME':'Name'},inplace=True)
            _ticker_list = _ticker_list.T.to_dict().values()
            _ticker_list = list(_ticker_list)
            ret_weights[sector] = _ticker_list

        log.info(ret_sector)
        log.info(ret_weights)
        return ret_weights, ret_sector
    @classmethod
    def transformSummaryBySharpe(cls,summary) :
        table = summary[cls.rename_columns.keys()]
        log.info(table)
        table = table.sum()
        log.info(table)
        table.rename(index=cls.rename_columns, inplace=True)

        wtf = summary[cls.stock_columns]
        wtf = wtf.round(2).T
        wtf.rename(index=cls.rename_columns, inplace=True)
        log.info(table)
        log.info(wtf)
        return table, wtf
    @classmethod
    def getSharpes(cls,data) :
        ret_sector = {}
        ret_stock = {}
        for sector, summary in TRANSFORM_SECTOR.bySector(data) :
            table, wtf = cls.transformSummaryBySharpe(summary)
            ret_sector[sector] = table.to_dict()
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
    def find(cls,input_file,repo_stock,background,category) :
        loader = LOAD_HISTORICAL_DATA(repo_stock,LOAD_HISTORICAL_DATA.default_column)
        for name, weights, meta_name_list in cls._find(input_file) :
            weights = cls.parseWeights(weights)
            if len(weights.columns.values) == 0 :
                continue
            prices = loader.act(weights.T)
            prices[meta_name_list['legend']] = cls.transform_portfolio_prices_by_weight(weights,prices)
            returns = TRANSFORM_TICKER.summarizeReturns(prices)

            log.info((weights,meta_name_list))
            # Portfolio	Initial Balance	Final Balance	CAGR	Stdev	Best Year	Worst Year	Max. Drawdown	Sharpe Ratio	Sortino Ratio	US Mkt Correlation
            # 'Portfolio','Initial Balance','Final Balance','CAGR','Stdev','Sharpe Ratio','Sortino Ratio','US Mkt Correlation'
            meta = TRANSFORM_SUMMARY.refactor(weights,background)
            log.info(meta)
            meta['sector'] = TRANSFORM_TICKER.enrichSector(category,meta)
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

def process(background,date_repo_prices,benchmark,input_file,category) :

    price_summary = EXTRACT_BENCHMARK.find_prices(date_repo_prices,benchmark)
    text_summary = EXTRACT_BENCHMARK.process(background,benchmark).T
    _returns = Group()
    distribution = Group()
    _sharpe = Group()
    _portfolio_name_list = []
    for weights, names, prices, summary, returns in TRANSFORM_PORTFOLIO.find(input_file,date_repo_prices,background,category) :
        _portfolio_name_list.append(names['portfolio'])
        distribution = Group.appendDiversify(names['diversified'],summary,distribution)
        _sharpe = Group.appendSharpe(weights,summary,_sharpe)
        _returns = Group.appendPrices(names['returns'],prices, _returns)
        
        target_From = 'legend_{portfolio}'.format(**names)
        target_To = names['portfolio']

        price_summary[target_From] = prices[target_From]
        text_summary[target_To] = summary.loc['portfolio']

    price_summary = EXTRACT_PRICES.smartMassage(price_summary)

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
