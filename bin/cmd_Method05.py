#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI_READ,INI_WRITE
from libUtils import combinations
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from newSharpe import PORTFOLIO
from libDebug import trace, cpu
from libDecorators import exit_on_exception, log_on_exception, singleton

@log_on_exception
def prices(ticker) :
    suffix = '/{}.pkl'.format(ticker)
    filename = filter(lambda x : x.endswith(suffix), VARIABLES().price_list)
    filename = list(filename)
    if len(filename) > 0 :
           filename = filename[0]
    logging.info((filename,ticker))
    name, ret = STOCK_TIMESERIES.load(filename)
    ret =  ret[VARIABLES().prices]
    #ret = ret.reset_index(drop=False)
    #ret.sort_index(inplace=True)
    return ret
@log_on_exception
def portfolio(ticker_list) :
    prices = map(lambda x : prices(x), ticker_list)
    prices = dict(zip(ticker_list,prices))
    prices = pd.DataFrame(prices)
    logging.debug(prices)
    prices.fillna(method='bfill', inplace=True)
    return prices

def load_background(background_files,floats_in_summary) :
    logging.info('reading file {}'.format(background_files))
    ret = {}
    for path, key, stock, value in INI_READ.read(*background_files) :
        if "File Creation Time" in stock :
           continue
        if stock not in ret :
           ret[stock] = {}
        if key in floats_in_summary :
           value = float(value[0])
        else :
           value = ', '.join(value)
        ret[stock][key] = value

    ret = pd.DataFrame(ret).T
    ret.dropna(subset=['LEN'],inplace=True)
    logging.info(ret[ret['CATEGORY'].notnull()])
    #logging.info(ret[ret['TYPE'].isnull()])
    ret['SECTOR'] = ret['SECTOR'].fillna("Unknown") 
    ret['NAME'] = ret['NAME'].fillna("Unavailable") 
    ret.fillna(0.0, inplace = True) 
    logging.info(ret['CATEGORY'].describe())
    logging.info(ret['TYPE'].describe())
    logging.info(ret['ENTITY'].describe())
    return ret

def get_globals(*largs) :
    ret = {}
    for name in largs :
        value = globals().get(name,None)
        if value is None :
           continue
        ret[name] = value
    return ret

@singleton
class VARIABLES() :
    var_names = ["env", "local_dir", "sector_files","background_files", "benchmark", "input_file", "output_file","price_list","ini_list",'prices', 'floats_in_summary' ]

    def __init__(self) :
        values = get_globals(*VARIABLES.var_names)
        self.__dict__.update(**values)
        if len(self.env.argv) > 0 :
           self.input_file = _env.argv[0]
        output_file = None
        if len(self.env.argv) > 1 :
           self.output_file = _env.argv[1]
        self.background = load_background(self.background_files,self.floats_in_summary)

class TRANSFORM():
    keys = ['RISK','SHARPE','CAGR','GROWTH','RETURNS']
    @classmethod
    def by_sector(cls,background) :
        sector_list = background['SECTOR']
        sector_list = sorted(set(sector_list))
        for sector in sector_list :
            group = background[background['SECTOR'] == sector]
            cls.stats(sector,group)
            yield sector, group
    @classmethod
    def stats(cls,msg,data) :
        raw = map(lambda key : data[key].mean(), cls.keys)
        readable = map(lambda value : round(value,3), raw)
        readable = dict(zip(cls.keys,readable))
        logging.info((msg,data.shape,readable))
        logging.info(round(data.corr(),1))

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
            logging.debug(stock_list)
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
        INI_WRITE.write(save_file, **config)
    @classmethod
    def portfolio(cls, save_file, **portfolio) :
        logging.info("saving results to file {}".format(save_file))
        INI_WRITE.write(save_file, **portfolio)
        logging.info("results saved to file {}".format(save_file))

def reduceLessThan(target, _data) :
    if len(_data) < 10 :
       return _data
    _len = len(_data)
    t = _data[target].mean()
    s = _data[target].std()
    threshold1, threshold2, threshold3 = t+s,t,t-s
    ret = _data[_data[target] <= threshold1]
    if _len == len(ret) :
       logging.info('reduce {}'.format(target))
       ret = ret[ret[target] <= threshold2]
       if _len == len(ret) :
          logging.info('double reduce {}'.format(target))
          ret = ret[ret[target] <= threshold3]
    return ret
def reduceGreaterThan(target, _data) :
    if len(_data) < 10 :
       return _data
    _len = len(_data)
    t = _data[target].mean()
    s = _data[target].std()
    threshold1, threshold2, threshold3 = t-s,t,t+s
    ret = _data[_data[target] >= threshold1]
    if _len == len(ret) :
       logging.info('increase {}'.format(target))
       ret = ret[ret[target] >= threshold2]
       if _len == len(ret) :
          logging.info('double increase{}'.format(target))
          ret = ret[ret[target] >= threshold3]
    return ret

def reduceTickerList(ret) :
    if len(ret) < 10 :
       return ret
    TRANSFORM.stats('Original',ret)
    while len(ret) > 10 :
        target = 'RISK'
        cap = len(ret)-10
        if cap < 10 :
            cap = 10
        ret = ret.sort_values(by=[target]).head(cap)
        TRANSFORM.stats(target,ret)
        target = 'SHARPE'
        target = 'CAGR'
        cap = len(ret)-10
        if cap < 10 :
            cap = 10
        ret = ret.sort_values(by=[target],ascending=False).head(cap)
        TRANSFORM.stats(target,ret)
    TRANSFORM.stats('reduced',ret)
    return ret
def process_Step01(output_file, data) :
    stock_list = data.index.values
    stock_list = list(stock_list)
    prices = portfolio(stock_list)
    portfolio_list = TRANSFORM_PORTFOLIO.getList(data,prices)
    logging.info(round(portfolio_list,4))
    LOAD.portfolio(output_file,**portfolio_list.to_dict())

def process_stock(stock_data) :
    ret = {}
    print(stock_data)
    for sector, group in TRANSFORM.by_sector(stock_data) :
        output_file = "{}/sector_{}.ini".format(VARIABLES().local_dir, sector)
        print(output_file)
        output_file = output_file.replace(' ','_')
        data = reduceTickerList(group)
        print(data)
        #print(group.sort_values(['SHARPE']))
        #print(data.sort_values(['SHARPE']))
        t = data.to_dict()
        for k in sorted(t.keys()) :
            if k not in ret :
                ret[k] = {}
            ret[k].update(t[k])
        LOAD.config(output_file,**data.to_dict())
        logging.info('saved file {}'.format(output_file))

        output_file = "{}/portfolio_{}.ini".format(local_dir, sector)
        output_file = output_file.replace(" ", "_")
        process_Step01(output_file,data)
    output_file = "{}/sector_Total.ini".format(local_dir)
    LOAD.config(output_file,**ret)

def process_fund(data) :
    local_dir = VARIABLES().local_dir
    for sector, group in TRANSFORM.by_sector(data) :
        output_file = "{}/fund_{}.ini".format(VARIABLES().local_dir, sector)
        output_file = output_file.replace(' ','_')
        data = reduceTickerList(group)
        LOAD.config(output_file,**data.to_dict())

        output_file = "{}/portfolio_{}.ini".format(VARIABLES().local_dir, sector)
        output_file = output_file.replace(" ", "_")
        process_Step01(output_file,data)
        
@trace
def prep() :
    #logging.info('reading from file {}'.format(VARIABLES().input_file))
    ret = VARIABLES().background
    TRANSFORM.stats('raw',ret)
    ret = ret[ret['LEN'] > 8*FINANCE.YEAR]
    TRANSFORM.stats('established',ret)
    ret = ret[ret['CAGR'] > 0]
    TRANSFORM.stats('profitable',ret)
    ret = ret[ret['RETURNS'] > 0]
    TRANSFORM.stats('profiable 2',ret)
    #ret = ret[ret['SHARPE'] > 0]
    #TRANSFORM.stats('sharpe',ret)
    #ret = ret[ret['RISK'] < 1]
    #TRANSFORM.stats('sane',ret)
    print(ret)
    n = ret['NAME'].copy(deep=True)
    n = n.str.replace("'", "")
    n = n.str.replace(" - ", ", ")
    n = n.str.replace(", ", " ")
    n = n.str.replace("Common Stock", "Cmn Stk")
    n = n.str.replace("Limited", "Ltd.")
    n = n.str.replace("Corporation", "Corp.")
    n = n.str.replace("Pharmaceuticals", "Pharm.")
    n = n.str.replace("Technologies", "Tech.")
    n = n.str.replace("Technology", "Tech.")
    n = n.str.replace("International", "Int.")
    ret['NAME'] = n
    stock = ret[ret['ENTITY'] == 'stock']
    fund = ret[ret['ENTITY'] == 'fund']
    s = fund['SECTOR'].copy(deep=True)
    s = s.str.replace("(", "")
    s = s.str.replace(")", "")
    fund['SECTOR'] = s
    for sector in sorted(stock['SECTOR'].unique()) :
        x = stock[stock['SECTOR'] == sector ]
        TRANSFORM.stats(sector,x)
    TRANSFORM.stats('stock',stock)
    TRANSFORM.stats('fund',fund)
    return stock, fund

@exit_on_exception
@trace
def main() : 
    stock, fund = prep()
    process_stock(stock)
    process_fund(fund)

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
   background = filter(lambda x : 'background.ini' in x, ini_list)
   background_files = filter(lambda x : 'stock_' in x or 'fund_' in x, background)
   background_files = list(background_files)
   benchmark = filter(lambda x : 'benchmark' in x, ini_list)
   price_list = env.list_filenames('local/historical_*/*pkl')
   prices = 'Adj Close'
   floats_in_summary = ['CAGR', 'GROWTH', 'LEN', 'RISK', 'SHARPE','RETURNS']

   main()

