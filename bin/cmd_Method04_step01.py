#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI,combinations, exit_on_exception, log_on_exception
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
        background = globals().get(target,[])
        background = list(background)
        target = 'benchmark'
        benchmark = globals().get(target,[])
        benchmark = list(benchmark)
        target = 'file_list'
        file_list = globals().get(target,[])
        file_list = list(file_list)

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
        cls._singleton = cls(_env,local_dir,sector,background,benchmark,config_list,input_file, output_file, file_list)
        return cls._singleton
    @classmethod
    def readBackground(cls) :
        load_file = cls.instance().background
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
               value = float(value[0])
            else :
               value = ', '.join(value)
            ret[stock][key] = value

        load_file = cls.instance().sector
        logging.info('reading file {}'.format(load_file))
        for path, section, key, ticker_list in INI.loadList(*load_file) :
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
        for sector, category in TRANSFORM.by_sector(ret) : pass
        ret = cls.filterBackground(ret)
        for sector, category in TRANSFORM.by_sector(ret) : pass
        cls._background_cache = ret
        return ret
    @classmethod
    def filterBackground(cls,ret) :
        cls.analysis(ret)
        ret = ret[ret['LEN'] > 8*FINANCE.YEAR]
        logging.info('LEN 8 years')
        cls.analysis(ret)
        ret = ret[ret['CAGR'] >= 0.18]
        logging.info('CAGR over 18%')
        cls.analysis(ret)
        ret = ret[ret['SHARPE'] >= 0.9]
        logging.info('SHARPE 0.9')
        cls.analysis(ret)
        #ret = ret[ret['RISK'] <= 0.2]
        #logging.info('RISK')
        #cls.analysis(ret)
        #ret = ret[ret['GROWTH'] >= 2.0]
        return ret
    @classmethod
    def analysis(cls,ret) :
        stock = ret[ret['ENTITY'] == 'stock']
        logging.info(stock.shape)
        logging.info(round(stock.corr(),1))
        fund = ret[ret['ENTITY'] == 'fund']
        logging.info(fund.shape)
        logging.info(round(fund.corr(),1))
    @classmethod
    def config() :
        ini_list = EXTRACT.instance().config_list
        logging.info("loading results {}".format(ini_list))
        for path, section, key, stock_list in INI.loadList(*ini_list) :
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
    keys = ['RISK','SHARPE','CAGR','GROWTH']
    @classmethod
    def by_sector(cls,background) :
        sector_list = background['SECTOR']
        sector_list = sorted(set(sector_list))
        for sector in sector_list :
            group = background[background['SECTOR'] == sector]
            raw = map(lambda key : group[key].mean(), cls.keys)
            readable = map(lambda value : round(value,4), raw)
            readable = dict(zip(cls.keys,readable))
            logging.info((sector,group.shape,readable))
            yield sector, group

class TRANSFORM_PORTFOLIO() :
    @classmethod
    def validate(cls, data) :
        stock_list = data.index.values
        stock_list = list(stock_list)
        minimum_portfolio_size = len(stock_list)-3
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
    @classmethod
    @trace
    def _getList(cls, prices, stocks, ret) :
        if ret is None :
           ret = pd.DataFrame()
        max_sharpe, min_dev = PORTFOLIO.find(prices, stocks=stocks, portfolios=10000, period=FINANCE.YEAR)
        ret = ret.append(max_sharpe)
        ret = ret.append(min_dev)
        return ret
    @classmethod
    def getList(cls, data, prices) :
        ret = None
        for stock_list in cls.stocks(data) :
            ret = cls._getList(prices,stock_list,ret)
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
        if size > 1000 :
           min_risk = ret.sort_values(['risk']).head(50)
           max_sharpe = ret.sort_values(['sharpe']).tail(50)
           ret = pd.DataFrame()
           ret = ret.append(min_risk)
           ret = ret.append(max_sharpe)
        return ret

class LOAD() :
    @classmethod
    def config(cls, save_file, **config) :
        logging.info("results saved to {}".format(save_file))
        ret = INI.init()
        for key in sorted(config) :
            value = config.get(key,{})
            INI.write_section(ret,key,**value)
        ret.write(open(save_file, 'w'))
def reduceTickerList(ret) :
    if len(ret) < 10 :
       return ret
    ret = ret[ret['RISK'] <= ret['RISK'].mean()]
    if len(ret) < 10 :
       return ret
    ret = ret[ret['CAGR'] >= ret['CAGR'].mean()]
    return ret

def groupByCategory(data) :
    local_dir = EXTRACT.instance().local_dir
    ret = {}
    for sector, group in TRANSFORM.by_sector(data) :
        data = reduceTickerList(group)
        ret[sector] = data.to_dict()
        output_file = "{}/sector_{}.ini".format(local_dir, sector)
        output_file = output_file.replace(' ','_')
        LOAD.config(output_file,**data.to_dict())

        stock_list = data.index.values
        stock_list = list(stock_list)
        prices = EXTRACT.portfolio(stock_list)
        logging.debug(prices)
        #max_sharpe, min_dev = PORTFOLIO.find(prices, stocks=stock_list, portfolios=10000, period=FINANCE.YEAR)
        #print (max_sharpe)
        #print (min_dev)
        portfolio_list = TRANSFORM_PORTFOLIO.getList(data,prices)
        logging.info(portfolio_list)
    return ret
def process_stock(data) :
    ret = groupByCategory(data)

def process_fund(data) :
    for sector, group in TRANSFORM.by_sector(data) :
        data = reduceTickerList(group)
        yield data.to_dict() 
        
@exit_on_exception
@trace
def main() : 
    logging.info('reading from file {}'.format(EXTRACT.instance().input_file))
    ret = EXTRACT.readBackground()
    stock = ret[ret['ENTITY'] == 'stock']
    process_stock(stock)
    fund = ret[ret['ENTITY'] == 'fund']
    process_fund(fund)
    #LOAD.config(**{})

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   local_dir = "{}/local".format(env.pwd_parent)
   ini_list = env.list_filenames('local/*.ini')
   sector = filter(lambda x : 'stock_by_sector.ini' in x or 'fund_by_type' in x , ini_list)
   background = filter(lambda x : 'background.ini' in x, ini_list)
   #background = filter(lambda x : 'stock_' in x, background)
   benchmark = filter(lambda x : 'benchmark' in x, ini_list)
   file_list = env.list_filenames('local/historical_*/*pkl')

   main()

