#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI,combinations, exit_on_exception, log_on_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from newSharpe import PORTFOLIO
from libDebug import trace, cpu
from libKMeans import EXTRACT_K

class EXTRACT() :
    _singleton = None
    _background_cache = None
    _prices = 'Adj Close'
    _floats_in_summary = ['CAGR', 'GROWTH', 'LEN', 'RISK', 'SHARPE', 'MAX DRAWDOWN']

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
        #for sector, category in TRANSFORM.by_sector(ret) : pass
        #ret = FILTER.background(ret)
        #for sector, category in TRANSFORM.by_sector(ret) : pass
        #cls._background_cache = ret
        return ret

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

class EXTRACT_SECTOR() :
    @classmethod
    def enrich(cls,data) :
        stock_list = []
        if isinstance(data,pd.DataFrame) :
           stock_list = data.index.values
        elif isinstance(data,dict) :
           stock_list = sorted(data.keys())
        elif isinstance(data,list) :
           stock_list = sorted(data)
        stock_list = cls.read().index.intersection(stock_list)
        ret = cls.read().loc[stock_list]
        logging.info(ret)
        return ret
    @classmethod
    def enumerate(cls,ret) :
        _list = ret['SECTOR']
        _list = sorted(set(_list))
        for entry in _list :
            group = ret[ret['SECTOR'] == entry]
            FILTER.analysis(group)
            logging.info((entry,len(group),sorted(group.index.values)))
            yield entry, group

class FILTER() :
    @classmethod
    def see_stock(cls,data) :
        ret = data[data['ENTITY'] == 'stock']
        logging.info("STOCK")
        return ret
    @classmethod
    def see_fund(cls,data) :
        ret = data[data['ENTITY'] == 'fund']
        logging.info("FUND")
        return ret
    @classmethod
    def _background(cls,ret,**kwargs) :
        LEN, RISK, CAGR, DRAWDOWN = cls.validate(**kwargs)
        cls.analysis(ret)
        logging.info('Initial')

        ret = ret[ret['LEN'] > LEN]
        cls.analysis(ret)
        logging.info('LEN {}'.format(LEN))

        ret = ret[ret['RISK'] <= RISK]
        cls.analysis(ret)
        logging.info('RISK under {}%'.format(RISK*100))

        ret = ret[ret['CAGR'] >= CAGR]
        cls.analysis(ret)
        logging.info('CAGR over {}%'.format(CAGR*100))

        ret = ret[ret['MAX DRAWDOWN'] >= DRAWDOWN]
        cls.analysis(ret)
        logging.info('MAX DRAWDOWN under {}%'.format(DRAWDOWN*100))

        return ret
    @classmethod
    def validate(cls, **kwargs) :
        target = 'LEN'
        LEN = kwargs.get(target,8*FINANCE.YEAR)
        target = 'RISK'
        RISK = kwargs.get(target,0.20) 
        target = 'CAGR'
        CAGR = kwargs.get(target,0.10)
        target = 'MAX DRAWDOWN'
        DRAWDOWN = kwargs.get(target,-0.20)
        return LEN, RISK, CAGR, DRAWDOWN
    @classmethod
    def background(cls,ret,**kwargs) :
        stock = cls.see_stock(ret)
        stock = cls._background(stock,**kwargs)

        fund = cls.see_fund(ret)
        fund = cls._background(fund)
        stock.append(fund)
        return stock
    @classmethod
    def analysis(cls,ret) :
        if len(ret) > 0 :
           logging.debug(ret.iloc[0])
        msg = ret[EXTRACT._floats_in_summary]
        msg = msg.describe().loc[['count','mean']]
        logging.info(msg.T)
        logging.info(round(ret.corr(),1))

class TRANSFORM():
    keys = ['RISK','SHARPE','CAGR','GROWTH']
    @classmethod
    def def_by_sector(cls,background) :
        sector_list = background['SECTOR']
        sector_list = sorted(set(sector_list))
        for sector in sector_list :
            group = background[background['SECTOR'] == sector]
            FILTER.analysis(group)
            logging.info(sector)
            yield sector, group

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
        columns = ret.columns.values
        columns = list(columns)
        rename = map(lambda p : "portfolio_{}".format(p), columns)
        rename = dict(zip(columns,rename))
        ret.rename(columns = rename, inplace = True) 
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

class LOAD() :
    @classmethod
    def config(cls, save_file, **config) :
        logging.info("results saved to {}".format(save_file))
        ret = INI.init()
        for key in sorted(config) :
            value = config.get(key,{})
            INI.write_section(ret,key,**value)
        ret.write(open(save_file, 'w'))

    @classmethod
    def portfolio(cls, save_file, **portfolio) :
        logging.info("saving results to file {}".format(save_file))
        ret = INI.init()
        name_list = sorted(portfolio.keys())
        value_list = map(lambda key : portfolio[key], name_list)
        for i, name in enumerate(name_list) :
            if not isinstance(value_list,list) :
               value_list = list(value_list)
            INI.write_section(ret,name,**value_list[i])
        ret.write(open(save_file, 'w'))
        logging.info("results saved to file {}".format(save_file))

def reduceTickerList(data) :
    if len(data) < 20 :
       return data
    ret = data
    initial_count = len(ret)
    while len(ret) >= 20 :
        target = 'RISK'
        cap = ret[target].mean() + ret[target].std()
        data = ret[ret[target] <= cap]
        if len(data) < 20 :
           return data
        target = 'MAX DRAWDOWN'
        ret = data
        cap = ret[target].mean()
        if ret[target].std() > 0 :
           cap -= ret[target].std()
        else :
           cap += ret[target].std()
        cap = ret[target].mean() - ret[target].std()
        data = ret[ret[target] >= cap]
        if len(data) < 20 :
           return data
        target = 'CAGR'
        ret = data
        cap = ret[target].mean() - ret[target].std()
        data = ret[ret[target] >= cap]
        if len(data) < 20 :
           return data
        ret = data
        if len(ret) == initial_count :
           ret = ret.sort_values('RISK').head(len(ret)-2)
        initial_count = len(ret)
        logging.info(initial_count)
    logging.info(len(ret))
    return ret
def process_Step01(output_file, data) :
    stock_list = data.index.values
    stock_list = list(stock_list)
    prices = EXTRACT.portfolio(stock_list)
    portfolio_list = TRANSFORM_PORTFOLIO.getList(data,prices)
    logging.info(round(portfolio_list,4))
    LOAD.portfolio(output_file,**portfolio_list.to_dict())

def process_stock(data) :
    data = FILTER.background(data, CAGR=0.0)
    _YYY = ['CAGR', 'RISK', 'SHARPE', 'MAX DRAWDOWN']
    _YYY = ['CAGR', 'RISK', 'MAX DRAWDOWN']
    _ZZZ = [ 'RISK', 'SHARPE','MAX DRAWDOWN']
    _XXX = ['CAGR', 'RISK', 'SHARPE', 'MAX DRAWDOWN', 'K STOCK','K SECTOR','K','NAME']
    _XXX = ['CAGR', 'RISK', 'SHARPE', 'MAX DRAWDOWN', 'K','L','NAME']
    local_dir = EXTRACT.instance().local_dir
    ret = {}
    cluster = int(len(data)/5)+1
    data['K STOCK'] = EXTRACT_K.cluster(data[_YYY],cluster)
    logging.info(data)

    for sector, group in EXTRACT_SECTOR.enumerate(data) :
        clusters = int(len(group)/5)+1
        group['K SECTOR'] = EXTRACT_K.cluster(group[_YYY],clusters)
        data = reduceTickerList(group)
        FILTER.analysis(data)
        clusters = int(len(data)/3)+1
        data['K'] = EXTRACT_K.cluster(data[_YYY],clusters)
        data['L'] = EXTRACT_K.cluster(data[_ZZZ],clusters)
        output_file = "{}/sector_{}.ini".format(local_dir, sector)
        output_file = output_file.replace(' ','_')
        t = data.to_dict()
        for k in sorted(t.keys()) :
            if k not in ret :
                ret[k] = {}
            ret[k].update(t[k])
        LOAD.config(output_file,**data.to_dict())
        logging.info('saved file {}'.format(output_file))
        data = data.sort_values(_YYY)
        print(data[_XXX])

        #output_file = "{}/portfolio_{}.ini".format(local_dir, sector)
        #output_file = output_file.replace(" ", "_")
        #process_Step01(output_file,data)
    output_file = "{}/sector_Total.ini".format(local_dir)
    LOAD.config(output_file,**ret)

def process_fund(data) :
    local_dir = EXTRACT.instance().local_dir
    for sector, group in TRANSFORM.by_sector(data) :
        output_file = "{}/fund_{}.ini".format(local_dir, sector)
        output_file = output_file.replace('(','')
        output_file = output_file.replace(')','')
        output_file = output_file.replace(' ','_')
        data = reduceTickerList(group)
        LOAD.config(output_file,**data.to_dict())

        #output_file = "{}/portfolio_{}.ini".format(local_dir, sector)
        #output_file = output_file.replace(" ", "_")
        #process_Step01(output_file,data)
        
@exit_on_exception
@trace
def main() : 
    logging.info('reading from file {}'.format(EXTRACT.instance().input_file))
    ret = EXTRACT.background()
    stock = ret[ret['ENTITY'] == 'stock']
    process_stock(stock)
    #fund = ret[ret['ENTITY'] == 'fund']
    #process_fund(fund)

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
   background = filter(lambda x : 'stock_' in x or 'fund_' in x, background)
   benchmark = filter(lambda x : 'benchmark' in x, ini_list)
   file_list = env.list_filenames('local/historical_*/*pkl')

   main()

