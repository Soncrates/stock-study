#!/usr/bin/env python

import logging
import pandas as pd
from libCommon import INI_READ,INI_WRITE
from libUtils import combinations
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from newSharpe import PORTFOLIO
from libDebug import trace, cpu
from libDecorators import exit_on_exception, log_on_exception, singleton

@exit_on_exception
def get_globals(*largs) :
    ret = {}
    for name in largs :
        value = globals().get(name,None)
        if value is None :
           raise ValueError(name)
        ret[name] = value
    return ret

@singleton
class VARIABLES() :
    var_names = ["env", "local_dir", "background_files", "price_list","ini_list",'prices', 'floats_in_summary','sector_cap','portfolio_iterations', 'threshold','columns_drop' ]

    def __init__(self) :
        values = get_globals(*VARIABLES.var_names)
        self.__dict__.update(**values)
        data = TRANSFORM.load_background(self.background_files,self.floats_in_summary)
        self.background = TRANSFORM.massage_SECTOR(data)

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

class TRANSFORM():
    keys = ['RISK','SHARPE','CAGR','GROWTH','RETURNS']
    @classmethod
    def load_background(cls,background_files,floats_in_summary) :
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
        return ret
    @classmethod
    def massage_SECTOR(cls,ret) :
        ret = pd.DataFrame(ret).T
        ret.dropna(subset=['LEN'],inplace=True)
        funds = ret[ret['ENTITY'] != 'stock']
        funds.dropna(subset=['CATEGORY','TYPE'],inplace=True)
        for index, row in funds.iterrows():
            row['SECTOR'] = '{} {}'.format(row['CATEGORY'],row['TYPE']).replace(' ','_')
            row['ENTITY'] = 'fund'
        logging.info(ret[ret['CATEGORY'].notnull()])
        #logging.info(ret[ret['TYPE'].isnull()])
        ret.update(funds)
        ret.drop(['CATEGORY', 'TYPE'], axis=1,inplace=True)
        ret['SECTOR'] = ret['SECTOR'].fillna("Unknown") 
        ret['NAME'] = ret['NAME'].fillna("Unavailable") 
        ret.fillna(0.0, inplace = True) 
        return ret
    @classmethod
    def massage_NAME(cls,ret) :
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
        return n
    @classmethod
    @trace
    def process(cls, ret) :
        cls.stats('raw',ret)
        ret = ret[ret['LEN'] > 8*FINANCE.YEAR]
        cls.stats('established',ret)
        ret = ret[ret['CAGR'] > 0]
        cls.stats('profitable',ret)
        ret = ret[ret['RETURNS'] > 0]
        cls.stats('profitable 2',ret)
        #ret = ret[ret['SHARPE'] > 0]
        #TRANSFORM.stats('sharpe',ret)
        #ret = ret[ret['RISK'] < 1]
        #TRANSFORM.stats('sane',ret)
        ret['NAME'] = cls.massage_NAME(ret)
        stock = ret[ret['ENTITY'] == 'stock']
        fund = ret[ret['ENTITY'] == 'fund']
        s = fund['SECTOR'].copy(deep=True)
        s = s.str.replace("(", "")
        s = s.str.replace(")", "")
        fund['SECTOR'] = s
        for sector in sorted(stock['SECTOR'].unique()) :
            x = stock[stock['SECTOR'] == sector ]
            cls.stats(sector,x)
        cls.stats('stock',stock)
        cls.stats('fund',fund)
        return stock, fund

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

class SUB_ACTOR() :
    def __init__(self, portfolio_iterations,threshold, columns_drop) :
        self.portfolio_iterations = portfolio_iterations
        self.threshold = threshold
        self.columns_drop = columns_drop
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
    def find_average(self, ret):
        ret = ret.T.mean()
        ret = ret[ret > self.threshold]
        ret = ret/ret.sum()
        ret = round(ret,2)
        return ret
    @classmethod
    @trace
    def portfolio(cls, prices, stocks, portfolio_iterations, ret = None) :
        if ret is None :
           ret = pd.DataFrame()
        max_sharpe, min_dev = PORTFOLIO.find(prices, stocks=stocks, portfolios=portfolio_iterations, period=FINANCE.YEAR)
        ret = ret.append(max_sharpe)
        ret = ret.append(min_dev)
        return ret

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
    def getList(self, data, prices) :
        ret = None
        for stock_list in SUB_ACTOR.stocks(data) :
            logging.info(stock_list)
            ret = SUB_ACTOR.portfolio(prices,stock_list,self.portfolio_iterations,ret)
            ret = SUB_ACTOR.truncate_1000(ret)
        return ret.T
    @classmethod
    def truncate_5(cls, ret) :
        if len(ret) <= 5 :
           return ret
        ret = ret.T
        #min_risk = ret.sort_values(['risk']).head(5)
        #max_sharpe = ret.sort_values(['sharpe']).tail(5)
        min_risk = ret.sort_values(['risk']).head(2)
        max_sharpe = ret.sort_values(['sharpe']).tail(2)
        ret = pd.DataFrame()
        ret = ret.append(min_risk)
        ret = ret.append(max_sharpe)
        logging.debug(min_risk)
        logging.debug(max_sharpe)
        return ret.T
    @classmethod
    def truncate_1000(cls, ret) :
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
    def massage_portfolio(self, ret) :
        ret.fillna(0,inplace=True)
        portfolio_id_list = ret.columns.values.tolist()
        portfolio_name_list = map(lambda p : "portfolio_{}".format(p), portfolio_id_list)
        rename = dict(zip(portfolio_id_list,portfolio_name_list))
        ret.rename(columns = rename, inplace = True) 
        return ret
    def act(self, data, prices) :
        ret = self.getList(data,prices)
        logging.debug(ret)
        ret.drop_duplicates(inplace=True)
        ret.fillna(0, inplace=True)
        logging.info(ret)

        avg = ret.drop(labels=self.columns_drop)
        avg = self.find_average(avg)
        logging.debug(avg)
        stock_list = avg[ avg > 0 ]
        stock_list = stock_list.T.keys().tolist()
        logging.debug(stock_list)
        x = None
        x = SUB_ACTOR.portfolio(prices,stock_list,self.portfolio_iterations*5,x)
        x = x.T
        x.drop_duplicates(inplace=True)

        ret = self.truncate_5(ret)
        logging.info((type(ret),type(x)))
        logging.info(ret)
        logging.info(x)
        for i, name in enumerate(x.keys().tolist()) :
            ret[name] = x[name]
        ret = self.massage_portfolio(ret)
        ret = round(ret,3)

        ret['summary'] = avg
        ret.fillna(0, inplace=True)
        logging.info(ret)
        return ret

class ACTOR() :
    def __init__(self, price_list, price_column,cap_size) :
        self.price_list = price_list
        self.price_column = price_column
        self.cap_size = cap_size
    def load_price_data(self, *ticker_list):
        filename_list = map(lambda ticker : '/{}.pkl'.format(ticker), ticker_list)
        filename_list = list(filename_list)
        logging.info((ticker_list,filename_list))
        ret = {}
        for i, suffix in enumerate(filename_list) :
            filename = filter(lambda x : x.endswith(suffix), self.price_list)
            filename = list(filename)
            if len(filename) == 0 :
               continue
            filename = filename[0]
            name, temp = STOCK_TIMESERIES.load(filename)
            key = ticker_list[i]
            ret[key] =  temp[self.price_column]
        return ret
    def act(self, data):
        ticker_list = data.index.values.tolist()
        ret = self.load_price_data(*ticker_list)
        ret = pd.DataFrame(ret)
        ret.fillna(method='bfill', inplace=True)
        logging.debug(ret)
        return ret
    def refine(self, ret) :
        if len(ret) < self.cap_size :
           return ret
        TRANSFORM.stats('Original',ret)
        while len(ret) > self.cap_size :
            target = 'RISK'
            cap = len(ret)-self.cap_size
            if cap < self.cap_size :
                cap = self.cap_size
            ret = ret.sort_values(by=[target]).head(cap)
            TRANSFORM.stats(target,ret)
            target = 'SHARPE'
            target = 'CAGR'
            cap = len(ret)-self.cap_size
            if cap < self.cap_size :
                cap = self.cap_size
            ret = ret.sort_values(by=[target],ascending=False).head(cap)
            TRANSFORM.stats(target,ret)
        TRANSFORM.stats('reduced',ret)
        return ret

def process_stock(stock_data, actor, sub_actor, local_dir) :
    ret = {}
    for sector, group in TRANSFORM.by_sector(stock_data) :
        output_file = "{}/sector_{}.ini".format(local_dir, sector)
        output_file = output_file.replace(' ','_')
        data = actor.refine(group)
        t = data.to_dict()
        for k in sorted(t.keys()) :
            if k not in ret :
                ret[k] = {}
            ret[k].update(t[k])

        LOAD.config(output_file,**data.to_dict())
        logging.info('saved file {}'.format(output_file))

        output_file = "{}/portfolio_{}.ini".format(local_dir, sector)
        output_file = output_file.replace(" ", "_")
        output_data = actor.act(data)
        output_data = sub_actor.act(data, output_data)
        LOAD.portfolio(output_file,**output_data.to_dict())
    output_file = "{}/sector_Total.ini".format(local_dir)
    LOAD.config(output_file,**ret)

def process_fund(data, actor, sub_actor, local_dir) :
    for sector, group in TRANSFORM.by_sector(data) :
        output_file = "{}/fund_{}.ini".format(local_dir, sector)
        output_file = output_file.replace(' ','_')
        data = actor.refine(group)
        LOAD.config(output_file,**data.to_dict())

        output_file = "{}/portfolio_{}.ini".format(local_dir, sector)
        output_file = output_file.replace(" ", "_")
        output_data = actor.act(data)
        output_data = sub_actor.act(data, output_data)
        LOAD.portfolio(output_file,**output_data.to_dict())
        
@exit_on_exception
@trace
def main() : 
    sub_actor = SUB_ACTOR(VARIABLES().portfolio_iterations,VARIABLES().threshold,VARIABLES().columns_drop)
    actor = ACTOR(VARIABLES().price_list,VARIABLES().prices,VARIABLES().sector_cap)
    stock, fund = TRANSFORM.process(VARIABLES().background)
    process_stock(stock,actor,sub_actor,VARIABLES().local_dir)
    process_fund(fund,actor,sub_actor, VARIABLES().local_dir)

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
   #benchmark = filter(lambda x : 'benchmark' in x, ini_list)
   price_list = env.list_filenames('local/historical_*/*pkl')
   prices = 'Adj Close'
   floats_in_summary = ['CAGR', 'GROWTH', 'LEN', 'RISK', 'SHARPE','RETURNS']
   sector_cap = 5
   portfolio_iterations = 10000
   threshold = 0.10
   columns_drop = ['returns','risk','sharpe']

   main()

