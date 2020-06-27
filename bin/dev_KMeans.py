#!/usr/bin/env python

import logging
import pandas as pd

from sklearn.cluster import KMeans
from math import sqrt
import  pylab as pl
import numpy as np

from libCommon import INI_READ, INI_WRITE
from libUtils import combinations, exit_on_exception, log_on_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from newSharpe import PORTFOLIO
from libDebug import trace, cpu

class EXTRACT() :
    _singleton = None
    _background_cache = None
    _prices = 'Adj Close'
    _floats_in_summary = ['CAGR', 'GROWTH', 'LEN', 'RISK', 'SHARPE']

    def __init__(self, _env, local_dir, sector,background, benchmark, config_list, input_file, output_file,file_list) :
        self._env = _env
        self.local_dir = local_dir
        self.sector = sector
        self.background = background
        self.benchmark = benchmark
        self.config_list = config_list
        self.input_file = input_file
        self.output_file = output_file
        self.file_list = file_list
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
        target = 'local_dir'
        local_dir = globals().get(target,None)
        target = 'sector'
        sector = globals().get(target,[])
        sector = list(sector)
        target = 'background'
        bg = globals().get(target,[])
        bg = list(bg)
        target = 'benchmark'
        bm = globals().get(target,[])
        bm = list(bm)
        target = 'file_list'
        prices = globals().get(target,[])
        prices = list(prices)

        input_file = None
        if len(_env.argv) > 0 :
           input_file = _env.argv[0]
        output_file = None
        if len(_env.argv) > 1 :
           output_file = _env.argv[1]
        target = 'ini_list'
        config_list = globals().get(target,[])
        if not isinstance(config_list,list) :
           config_list = list(config_list)
        cls._singleton = cls(_env,local_dir,sector,bg,bm,config_list,input_file, output_file, prices)
        return cls._singleton
    @classmethod
    def background(cls) :
        load_file = cls.instance().background
        if not (cls._background_cache is None) :
           logging.info('reading cache {}'.format(load_file))
           return cls._background_cache
        logging.info('reading file {}'.format(load_file))
        ret = {}
        for path, key, stock, value in INI_READ.read(*load_file) :
            if "File Creation Time" in stock :
                continue
            if stock not in ret :
               ret[stock] = {}
            if key in cls._floats_in_summary :
               value = float(value[0])
            else :
               value = ', '.join(value)
            ret[stock][key] = value

        load_file = cls.instance().sector
        logging.info('reading file {}'.format(load_file))
        for path, section, key, ticker_list in INI_READ.read(*load_file) :
            entity = 'stock'
            if 'fund' in path :
               key = '{} ({})'.format(section,key)
               entity = 'fund'
            for ticker in ticker_list :
                if ticker not in ret :
                   ret[ticker] = {}
                ret[ticker]['SECTOR'] = key
                ret[ticker]['ENTITY'] = entity
        
        ret = pd.DataFrame(ret).T
        ret['SECTOR'] = ret['SECTOR'].fillna("Unknown") 
        ret['NAME'] = ret['NAME'].fillna("Unavailable") 
        ret.fillna(0.0, inplace = True) 
        return ret
    @classmethod
    def dep_config() :
        ini_list = EXTRACT.instance().config_list
        logging.info("loading results {}".format(ini_list))
        for path, section, key, stock_list in INI_READ.read(*ini_list) :
            yield path, section, key, stock_list
    @classmethod
    @log_on_exception
    def prices(cls, ticker) :
        data_store = cls.instance().file_list
        suffix = '/{}.pkl'.format(ticker)
        filename = filter(lambda x : x.endswith(suffix), data_store)
        filename = list(filename)
        if len(filename) > 0 :
           filename = filename[0]
        logging.info((filename,ticker))
        name, ret = STOCK_TIMESERIES.load(filename)
        ret =  ret[cls._prices]
        #ret = ret.reset_index(drop=False)
        #ret.sort_index(inplace=True)
        return ret
    @classmethod
    @log_on_exception
    def portfolio(cls, ticker_list) :
        prices = map(lambda x : cls.prices(x), ticker_list)
        prices = dict(zip(ticker_list,prices))
        logging.debug(prices.values())
        prices = pd.DataFrame(prices)
        prices.fillna(method='bfill', inplace=True)
        return prices

class TRANSFORM():
    keys = ['RISK','SHARPE','CAGR']
    @classmethod
    def by_sector(cls,ret) :
        _list = ret['SECTOR']
        _list = sorted(set(_list))
        for entry in _list :
            group = ret[ret['SECTOR'] == entry]
            raw = map(lambda key : group[key].mean(), cls.keys)
            readable = map(lambda value : round(value,4), raw)
            readable = dict(zip(cls.keys,readable))
            logging.info((entry,group.shape,readable))
            yield entry, group
    @classmethod
    def by_K(cls,ret) :
        _list = ret['K']
        _list = sorted(set(_list))
        for entry in _list :
            group = ret[ret['K'] == entry]
            raw = map(lambda key : group[key].mean(), cls.keys)
            readable = map(lambda value : round(value,4), raw)
            readable = dict(zip(cls.keys,readable))
            logging.info((entry,group.shape,readable))
            yield entry, group

class TRANSFORM_PORTFOLIO() :
    @classmethod
    def validate(cls, data) :
        stock_list = data.index.values
        stock_list = list(stock_list)
        minimum_portfolio_size = len(stock_list)-5
        if minimum_portfolio_size < 1 :
           minimum_portfolio_size = 1
        return stock_list, minimum_portfolio_size
    @classmethod
    def reduceRisk(cls, data,count) :
        data = data.sort_values(['RISK']).head(count)
        stock_list = data.index.values
        stock_list = list(stock_list)
        return stock_list
    @classmethod
    def stocks(cls, data) :
        stock_list, minimum_portfolio_size = cls.validate(data)
        if len(stock_list) == 0 :
           return
        if len(stock_list) <= 3 :
           yield stock_list
           return

        yield stock_list
        count = len(stock_list)-1
        while count > minimum_portfolio_size :
            for subset in combinations(stock_list,count) :
                yield sorted(subset)
            stock_list = cls.reduceRisk(data,count)
            count = len(stock_list)-1
            if count < 3 :
               break
    @classmethod
    @trace
    def portfolio(cls, prices, stocks, ret = None) :
        if ret is None :
           ret = pd.DataFrame()
        max_sharpe, min_dev = PORTFOLIO.find(prices, stocks=stocks, portfolios=10000, period=FINANCE.YEAR)
        ret = ret.append(max_sharpe)
        ret = ret.append(min_dev)
        return ret
    @classmethod
    @trace
    def getList(cls, data, prices) :
        ret = pd.DataFrame()
        for stock_list in cls.stocks(data) :
            ret = cls.portfolio(prices,stock_list,ret)
            ret = cls.truncate(ret)
        if len(ret) > 5 :
           #min_risk = ret.sort_values(['risk']).head(5)
           #max_sharpe = ret.sort_values(['sharpe']).tail(5)
           min_risk = ret.sort_values(['risk']).head(2)
           max_sharpe = ret.sort_values(['sharpe']).tail(2)
           ret = pd.DataFrame()
           ret = ret.append(min_risk)
           ret = ret.append(max_sharpe)
           logging.debug(min_risk)
           logging.debug(max_sharpe)
        ret = ret.T
        ret.fillna(0,inplace=True)
        return ret
    @classmethod
    def truncate(cls, ret) :
        if ret is None :
           ret = pd.DataFrame()
        size = len(ret)
        if size < 1000 :
           return ret

        min_risk = ret.sort_values(['risk']).head(50)
        max_sharpe = ret.sort_values(['sharpe']).tail(50)
        ret = pd.DataFrame()
        ret = ret.append(min_risk)
        ret = ret.append(max_sharpe)
        return ret

class TRANSFORM_K():
    @classmethod
    def test(cls,ret) :
        X =  ret.values #Converting ret_var into nummpy arraysse = []for k in range(2,15):
        #test for number of categories
        #kmeans = KMeans(n_clusters = k)
        #kmeans.fit(X)
        #sse.append(kmeans.inertia_) #SSE for each n_clusters
        #centroids = kmeans.cluster_centers_
    @classmethod
    def cluster(cls,ret) :
        X =  ret.values 
        groups = int(len(X)/10)
        if groups < 5 :
           groups = 5
        if groups > len(X) :
           groups = len(X)-1
        logging.debug(groups)
        kmeans = KMeans(n_clusters = groups).fit(X)
        ret = pd.DataFrame(kmeans.labels_)
        return ret
    @classmethod
    def process(cls,ret) :
        cluster_labels = cls.cluster(ret)
        _x = range(0,len(ret.index.values))
        ticker = dict(zip(_x,ret.index.values))
        cluster_labels.rename(index=ticker,inplace=True)
        ret['K'] = cluster_labels
        return ret
class LOAD() :
    @classmethod
    def config(cls, save_file, **config) :
        logging.info("results saved to {}".format(save_file))
        INI_WRITE.write(save_file,**config)

def reduceTickerList(ret) :
    if len(ret) < 10 :
       return ret
    while len(ret) >= 10 :
        data = ret[ret['RISK'] <= ret['RISK'].mean()]
        if len(data) < 4 :
           return ret
        if len(data) < 10 :
           return data
        ret = data
    return ret
def process_Step01(data) :
    stock_list = data.index.values
    stock_list = list(stock_list)
    prices = EXTRACT.portfolio(stock_list)
    ret = TRANSFORM_PORTFOLIO.getList(data,prices)
    #ret.drop_duplicates(inplace=True)
    ret = ret.loc[:,~ret.columns.duplicated()]
    logging.info(round(ret,4))
    return ret

def normalize(ret) :
    SNP_GROWTH = 3.1
    SNP_RISK = 0.4187
    SNP_CAGR = 0.1204
    ret = ret[['RISK','CAGR','SHARPE','SECTOR']]
    ret['CAGR'] = ret['CAGR']/SNP_CAGR
    ret['RISK'] = ret['RISK']/SNP_RISK
    return ret
def baseFilter(ret) :
    ret = ret[ret['CAGR'] > 0.9 ]
    ret = ret[ret['RISK'] <= 2 ]
    return ret
def partition(ret) :
    _YYY = [ 'CAGR', 'RISK', 'SHARPE' ]
    for sector, group in TRANSFORM.by_sector(ret) :
        K = TRANSFORM_K.process(group[_YYY])
        group['K'] = K['K']
        ret = pd.DataFrame()
        for _K, k in TRANSFORM.by_K(group) :
            portfolio = '{}_{}'.format(sector,_K)
            yield sector, portfolio, k
def process_stocks(stock) :
    _XXX = [ 'RISK','CAGR','SHARPE']
    _YYY = [ 'CAGR', 'RISK', 'SHARPE' ]
    stock = normalize(stock)
    stock = baseFilter(stock)
    ret = {}
    for sector, portfolio, group in partition(stock) :
        if sector not in ret :
            ret[sector] = {}
        group = reduceTickerList(group)
        msg = group[_XXX].sort_values(['RISK'])
        logging.info(msg)
        logging.info(round(group[_YYY].corr(),1))
        _pf = process_Step01(group)
        _pf = round(_pf,4)

        columns = _pf.columns.values
        columns = list(columns)
        rename = map(lambda p : "{}_{}".format(portfolio,p), columns)
        rename = dict(zip(columns,rename))
        _pf.rename(columns = rename, inplace = True)
        for column in _pf.columns.values :
            logging.info(_pf[column])
            ret[sector][column] = _pf[column].to_dict()
        logging.info(ret)
    for sector in ret :
        portfolio_list = pd.DataFrame(ret[sector])
        column = portfolio_list.columns.values
        _sector = {}
        for name in column :
            weights = portfolio_list[name]
            weights = weights[weights>0]
            _sector[name] = weights.to_dict()
        output_file = '../local/portfolio_{}.ini'.format(sector)
        output_file = output_file.replace(' ','_')
        LOAD.config(output_file,**_sector)
def process_funds(fund) :
    '''
    PSLDX 	PIMCO StocksPLUS Long Duration Instl 	43.29%
    AKRIX 	Akre Focus Instl 	                56.71%
    -------------------------------------------------------------
    MFEKX 	MFS Growth R6 	                        56.92%
    PSGCX 	Virtus KAR Small-Cap Growth C 	        37.72%
    LREIX 	Lazard US Realty Equity Instl 	        5.36%
    '''
    _XXX = [ 'RISK','CAGR','SHARPE']
    _YYY = [ 'CAGR', 'RISK', 'SHARPE' ]
    fund = normalize(fund)
    fund = baseFilter(fund)
    fund = fund[fund['CAGR'] > 1.4]
    fund = fund[fund['SHARPE'] > 1.25]
    ret = {}
    for sector, portfolio, group in partition(fund) :
        if sector not in ret :
            ret[sector] = {}
        if len(group) == 0 :
            continue
        #group = reduceTickerList(group)
        msg = group[_XXX].sort_values(['RISK'])
        print(msg)
        print(round(group[_YYY].corr(),1))
        print(portfolio)
@exit_on_exception
@trace
def main() : 
    tickers = EXTRACT.background()
    tickers = tickers[tickers['LEN'] > 8*FINANCE.YEAR]
    stock = tickers[tickers['ENTITY'] == 'stock']
    fund = tickers[tickers['ENTITY'] == 'fund']
    process_funds(fund)
    process_stocks(stock)

if __name__ == '__main__' :
   import sys
   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   local_dir = "{}/local".format(env.pwd_parent)
   ini_list = env.list_filenames('local/*.ini')
   sector = filter(lambda x : 'stock_by_sector.ini' in x , ini_list)
   background = filter(lambda x : 'background.ini' in x, ini_list)
   background = filter(lambda x : 'stock_' in x or 'fund_' in x, background)
   benchmark = filter(lambda x : 'benchmark' in x, ini_list)
   file_list = env.list_filenames('local/historical_*/*pkl')

   main()
   # Execution speed for main : hours : 4.0, minutes : 7.0, seconds : 48.5
