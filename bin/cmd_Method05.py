#!/usr/bin/env python

import logging as log
import pandas as PD
from libCommon import INI_READ,INI_WRITE, find_subset
from libUtils import combinations
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from newSharpe import PORTFOLIO as MONTERCARLO
from libDebug import pprint, trace, cpu
from libDecorators import exit_on_exception, log_on_exception, singleton

@singleton
class VARIABLES() :
    var_names = ["cli", "local_dir", "background_files", "price_list","ini_list",'floats_in_summary', 'columns_drop','disqualified']

    def __init__(self) :
        self.__dict__.update(**find_subset(globals(),*VARIABLES.var_names))
        self.__dict__.update(**self.cli)
        if len(self.suffix) > 0 :
           self.suffix = "_" + self.suffix
        self.entity = self.entity.lower()
        self.flag_stock = 'stock' in self.entity
        self.flag_fund = 'fund' in self.entity
        self.flag_etl = 'etl' in self.entity
        self.flag_etl = False
        if not (self.flag_stock or self.flag_fund or self.flag_etl) :
           raise ValueError('entity must be stocks or funds')
        bg = []
        if self.flag_stock :
           v = filter(lambda x : 'stock' in x, self.background_files)
           bg.extend(list(v))
        if self.flag_fund :
           v = filter(lambda x : 'fund' in x, self.background_files)
           bg.extend(list(v))
        self.background_files = bg
        pprint(vars(self)) 

class LOAD() :
    @classmethod
    def config(cls, save_file, **config) :
        INI_WRITE.write(save_file, **config)
    @classmethod
    def background(cls,background_files) :
        ret = {}
        for key, stock, value in INI_READ.read(*background_files) :
            if "File Creation Time" in stock :
               continue
            if stock not in ret :
               ret[stock] = {}
            value = ', '.join(value)
            ret[stock][key] = value
        return ret

class CURATE_BACKGROUND():
    @classmethod
    def simple(cls,ret, floats_in_summary,disqualified) :
        log.info(ret)
        ret.dropna(subset=['LEN'],inplace=True)
        ret.drop(disqualified,errors='ignore',inplace=True)
        for field in ['SECTOR','ENTITY','NAME'] :
            ret[field].fillna("Unknown",inplace=True) 
        for field in ['MAX DRAWDOWN','MAX INCREASE'] :
            ret[field] = ret[field].apply(lambda x : round(float(x),2))
        for field in floats_in_summary :
            ret[field] = ret[field].apply(lambda x : round(float(x),4))
        ret = cls.curate_names(ret)
        log.info(ret)
        return ret
    @classmethod
    def act(cls,ret) :
        f = ret[ret['ENTITY'] != 'stock']
        f = f[f['ENTITY'] != 'Unknown']
        if f.empty :
            return ret
        f = cls.curate_funds(f)
        ret.update(f)
        ret.drop(['CATEGORY', 'TYPE','GROWTH'], axis=1,errors='ignore',inplace=True)
        log.info(ret.dtypes)
        return ret
    @classmethod
    def curate_funds(cls,ret) :
        log.info(ret)
        ret.dropna(subset=['CATEGORY','TYPE'],inplace=True)
        for index, row in ret.iterrows():
            s = '{} {}'.format(row['CATEGORY'],row['TYPE'])
            s = s.replace("(", "").replace(")", "").replace(' ','_')
            row['SECTOR'] = s
            row['ENTITY'] = 'fund'
        log.info(ret[ret['CATEGORY'].notnull()])
        return ret
    @classmethod
    def curate_names(cls,ret) :
        for index, row in ret.iterrows():
            n = row['NAME']
            n = n.replace("'", "")
            n = n.replace(" - ", ", ")
            n = n.replace(", ", " ")
            n = n.replace("Common Stock", "Cmn Stk")
            n = n.replace("Limited", "Ltd.")
            n = n.replace("Corporation", "Corp.")
            n = n.replace("Pharmaceuticals", "Pharm.")
            n = n.replace("Technologies", "Tech.")
            n = n.replace("Technology", "Tech.")
            n = n.replace("International", "Int.")
            row['NAME'] = n
        return ret

class MEAN():
    key_list = ['RISK','SHARPE','CAGR','RETURNS']
    @classmethod
    def stats(cls,msg,data) :
        log.info(data)
        if data is None or data.empty :
           return data
        ret = [ round(data[key].astype(float).mean(),3) for key in cls.key_list if key in data ]
        ret = dict(zip(cls.key_list,ret))
        log.info((msg,data.shape,ret))
        log.info(round(data.corr(),1))
        return ret

class BACKGROUND():
    SECTOR = 'SECTOR'
    ENTITY = 'ENTITY'
    @classmethod
    def by_entity(cls,d) :
        ret = {}
        for key, value in cls.by_field(d,cls.ENTITY) :
            ret[key] = value
        stock = ret.pop('stock',None)
        funds = ret.pop('fund',None)
        log.info(ret)
        MEAN.stats('stock',stock)
        for sector, group in cls.by_field(stock,cls.SECTOR):
            pass
        MEAN.stats('fund',funds)
        for sector, group in cls.by_field(funds,cls.SECTOR):
            pass
        return stock, funds
    @classmethod
    def by_sector(cls,d) :
        for sector, ret in cls.by_field(d,cls.SECTOR) :
            yield sector, ret
    @classmethod
    def by_field(cls,d,t=None) :
        if d is None :
           return
        if isinstance(d,dict) :
           d = PD.DataFrame(d)
        if t is None :
           t = cls.SECTOR
        distinct = d[t].unique()
        log.info((t,distinct))
        for key in sorted(distinct) :
            value = d[d[t] == key]
            MEAN.stats(key,value)
            yield key, value
    @classmethod
    def reduceRisk(cls, data,count) :
        data = data.sort_values(['RISK']).head(count)
        stock_list = data.index.values
        stock_list = list(stock_list)
        return stock_list
    @classmethod
    def refine(cls,ret) :
        ret.fillna(0.0, inplace = True) 
        MEAN.stats('raw',ret)
        ret = ret[ret['LEN'] >= 8*FINANCE.YEAR]
        MEAN.stats('established',ret)
        ret = ret[ret['CAGR'] >= 0]
        MEAN.stats('profitable',ret)
        ret = ret[ret['RETURNS'] >= 0]
        MEAN.stats('profitable 2',ret)
        log.info(ret)
        return ret

class TRANSFORM():
    @classmethod
    def addMean(self, ret) :
        if ret.empty :
            return ret
        mean = MEAN.stats('mean',ret)
        mean = PD.Series(mean)
        log.info(mean)
        ret = ret.T
        ret['mean'] = mean
        ret = ret.T
        return ret
    @classmethod
    def validate(cls, data) :
        stock_list = data.index.values.tolist()
        minimum_portfolio_size = len(stock_list)-5
        if minimum_portfolio_size < 1 :
           minimum_portfolio_size = 1
        return stock_list, minimum_portfolio_size
    @classmethod
    def merge(cls, left, right) :
        left = left.T.reset_index(drop=True)
        ret = PD.concat([left,right.T],sort=True)
        log.info(ret)
        return ret
class PORTFOLIO():
    @classmethod
    def truncate_5(cls, ret) :
        if ret is None or ret.empty :
            log.warning("no data")
            return ret
        if len(ret) <= 5 :
           return ret
        ret = ret.T
        #min_risk = ret.sort_values(['risk']).head(5)
        #max_sharpe = ret.sort_values(['sharpe']).tail(5)
        min_risk = ret.sort_values(['risk']).head(2)
        max_sharpe = ret.sort_values(['sharpe']).tail(2)
        ret = PD.DataFrame()
        ret = ret.append(min_risk)
        ret = ret.append(max_sharpe)
        log.debug(min_risk)
        log.debug(max_sharpe)
        ret = ret.T
        log.info(ret)
        return ret
    @classmethod
    def truncate_1000(cls, ret) :
        if ret is None :
           ret = PD.DataFrame()
        size = len(ret)
        if size < 1000 :
           return ret

        min_risk = ret.sort_values(['risk']).head(50)
        max_sharpe = ret.sort_values(['sharpe']).tail(50)
        ret = PD.DataFrame()
        ret = ret.append(min_risk)
        ret = ret.append(max_sharpe)
        return ret
    @classmethod
    @trace
    def portfolio(cls, prices, stocks, portfolio_iterations, ret = None) :
        if ret is None :
           ret = PD.DataFrame()
        max_sharpe, min_dev = MONTERCARLO.find(prices, stocks=stocks, portfolios=portfolio_iterations, period=FINANCE.YEAR)
        ret = PD.concat([ret,max_sharpe])
        ret = PD.concat([ret,min_dev])
        return ret
    @classmethod
    def massage(cls, ret) :
        if ret is None or ret.empty :
            return ret 
        ret.fillna(0,inplace=True)
        portfolio_id_list = ret.columns.values.tolist()
        portfolio_name_list = map(lambda p : "portfolio_{}".format(p), portfolio_id_list)
        rename = dict(zip(portfolio_id_list,portfolio_name_list))
        ret.rename(columns = rename, inplace = True) 
        return ret

class STEP_01() :
    '''
    STEP_01 based on background, filter list 
    '''
    def __init__(self, cap_size, reduce_risk, reduce_return) :
        self.cap_size = cap_size
        self.reduce_risk = reduce_risk
        self.reduce_return = reduce_return
    def __repr__(self):
        return f"Background filter (max:{self.cap_size},risk reduction:{self.reduce_risk}, return improve : {self.reduce_return})"
    def act(self, background, keys = None) :
        if keys is None :
           keys = []
        while len(background) > self.cap_size :
              background = self.reduce(background)
        keys.extend(background.index.values.tolist())
        return background, keys
    def reduce(self, ret) :
        target = 'RISK'
        cap = len(ret) - self.reduce_risk
        if cap < self.cap_size :
            cap = self.cap_size
        ret = ret.sort_values(by=[target]).head(cap)
        MEAN.stats(target,ret)
        target = 'SHARPE'
        target = 'CAGR'
        cap = len(ret) - self.reduce_return
        if cap < self.cap_size :
            cap = self.cap_size
        ret = ret.sort_values(by=[target],ascending=False).head(cap)
        MEAN.stats(target,ret)
        return ret

class STEP_02() :
    def __init__(self, price_list, price_column) :
        self.price_list = price_list
        self.price_column = price_column
    def __repr__(self):
        return f"Historical loader (column:{self.price_column})"
    def load(self, *ticker_list):
        filename_list = [ '/{}.pkl'.format(ticker) for ticker in ticker_list ]
        log.info((ticker_list,filename_list))
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
        log.info(ticker_list)
        ret = self.load(*ticker_list)
        ret = PD.DataFrame(ret)
        ret.fillna(method='bfill', inplace=True)
        log.info(ret)
        return ret

class STEP_03() :
    def __init__(self, portfolio_iterations,columns_drop) :
        self.portfolio_iterations = portfolio_iterations
        self.columns_drop = columns_drop
    def __repr__(self):
        return f"Portfolio Generator (iterations:{self.portfolio_iterations},remove columns {self.columns_drop})"
    def stocks(self, data) :
        ret = data.drop(labels=self.columns_drop,errors='ignore')
        stock_list, minimum_portfolio_size = TRANSFORM.validate(ret)
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
            stock_list = BACKGROUND.reduceRisk(ret,count)
            count = len(stock_list)-1
            if count < 3 :
               break
    def act(self, data, prices) :
        ret = None
        for stock_list in self.stocks(data) :
            log.info(stock_list)
            ret = PORTFOLIO.portfolio(prices,stock_list,self.portfolio_iterations,ret)
            ret = PORTFOLIO.truncate_1000(ret)
        if ret is None :
            log.info(ret)
            return ret
        ret = ret.drop_duplicates()
        ret.reset_index(drop=True, inplace=True)
        ret.fillna(0, inplace=True)
        ret = ret.T
        log.info(ret)
        return ret
class STEP_04() :
    def __init__(self, portfolio_iterations,threshold, columns_drop) :
        self.portfolio_iterations = portfolio_iterations
        self.threshold = threshold
        self.columns_drop = columns_drop
    def __repr__(self):
        return f"Sweet Spot (iterations:{self.portfolio_iterations},remove columns {self.columns_drop}, threshold : {self.threshold})"
    def find_average(self, ret):
        ret = ret.drop(labels=self.columns_drop,errors='ignore')
        ret = ret.T.mean()
        ret = ret[ret > self.threshold]
        ret = ret/ret.sum()
        ret = round(ret,2)
        log.debug(ret)
        stock_list = ret[ ret > 0 ]
        stock_list = stock_list.T.keys().tolist()
        log.debug(stock_list)
        return ret, stock_list

    def act(self, data, prices, total) :
        if total is None :
           total = []
        avg, stock_list = self.find_average(data)
        total.extend(stock_list)
        ret = None
        ret = PORTFOLIO.portfolio(prices,stock_list,self.portfolio_iterations*5,ret)
        ret = ret.drop_duplicates().T
        ret['summary'] = avg
        ret.fillna(0, inplace=True)
        log.info(ret)
        return ret, total

def process_stock(local_dir, suffix, data, step_01, step_02, step_03, step_04,reduce_99) :
    _90 = None
    _99 = None
    for sector, group in BACKGROUND.by_sector(data) :
        top_tier, _90 = step_01.act(group, _90)

        summary = TRANSFORM.addMean(top_tier)
        output_file = "{}/sector_{}{}.ini".format(local_dir, sector,suffix)
        output_file = output_file.replace(' ','_')
        LOAD.config(output_file,**summary.to_dict())

        prices = step_02.act(top_tier)
        left = step_03.act(top_tier, prices)
        right, _99 = step_04.act(left, prices,_99)
        left = PORTFOLIO.truncate_5(left)
        output_data = TRANSFORM.merge(left,right).T
        output_data = PORTFOLIO.massage(output_data)
        output_data = round(output_data,3)

        output_file = "{}/portfolio_{}{}.ini".format(local_dir, sector,suffix)
        output_file = output_file.replace(" ", "_")
        LOAD.config(output_file,**output_data.to_dict())

    output_file = "{}/sector_90{}.ini".format(local_dir,suffix)
    _90 = data.loc[ _90 , : ]
    _90s = TRANSFORM.addMean(_90)
    log.info(_90s)
    LOAD.config(output_file,**_90s.to_dict())

    _99 = data.loc[ _99 , : ]
    log.info(_99)
    MEAN.stats('99',_99)
    _99,dummy = reduce_99.act(_99)
    log.info(_99)
    output_file = "{}/sector_99{}.ini".format(local_dir,suffix)
    _99s = TRANSFORM.addMean(_99)
    LOAD.config(output_file,**_99s.to_dict())

    prices = step_02.act(_99)
    portfolios = step_03.act(_99, prices)
    portfolios = PORTFOLIO.truncate_5(portfolios)
    portfolios = PORTFOLIO.massage(portfolios)
    if portfolios is None or portfolios.empty :
        log.warning("portfolio is empty")
        return
    output_file = "{}/portfolio_99{}.ini".format(local_dir,suffix)
    LOAD.config(output_file,**portfolios.to_dict())

def process_fund(local_dir,suffix, data, step_01, step_02, step_03, step_04) :
    _90_Alt = None
    _99 = None
    for sector, group in BACKGROUND.by_sector(data) :
        top_tier, _90_Alt = step_01.act(group, _90_Alt)

        summary = TRANSFORM.addMean(top_tier)
        output_file = "{}/fund_{}{}.ini".format(local_dir, sector,suffix)
        output_file = output_file.replace(' ','_')
        LOAD.config(output_file,**summary.to_dict())

        prices = step_02.act(top_tier)
        left = step_03.act(top_tier, prices)
        right, _99 = step_04.act(left, prices,_99)
        left = PORTFOLIO.truncate_5(left)
        output_data = TRANSFORM.merge(left,right).T
        output_data = PORTFOLIO.massage(output_data)
        output_data = round(output_data,3)

        output_file = "{}/portfolio_{}{}.ini".format(local_dir, sector,suffix)
        output_file = output_file.replace(" ", "_")
        LOAD.config(output_file,**output_data.to_dict())
        
@exit_on_exception
@trace
def main() : 
    step_01 = STEP_01(VARIABLES().sector_cap,VARIABLES().reduce_risk,VARIABLES().reduce_returns)
    step_02 = STEP_02(VARIABLES().price_list,VARIABLES().prices)
    step_03 = STEP_03(VARIABLES().portfolio_iterations,VARIABLES().columns_drop)
    step_04 = STEP_04(VARIABLES().portfolio_iterations,VARIABLES().threshold,VARIABLES().columns_drop)
    reduce_99 = STEP_01(25,1,2)
    for msg in [step_01,step_02,step_03,step_04] :
        log.info(repr(msg))

    bg = LOAD.background(VARIABLES().background_files)
    bg = PD.DataFrame(bg).T
    bg = CURATE_BACKGROUND.simple(bg, VARIABLES().floats_in_summary, VARIABLES().disqualified)
    bg = CURATE_BACKGROUND.act(bg)
    bg = BACKGROUND.refine(bg)
    bg.drop(['LEN', 'MAX DRAWDOWN','MAX INCREASE'], axis=1,errors='ignore',inplace=True)
    stock, fund = BACKGROUND.by_entity(bg)
    if VARIABLES().flag_stock :
       process_stock(VARIABLES().local_dir, VARIABLES().suffix, stock, step_01, step_02, step_03, step_04,reduce_99)
    if VARIABLES().flag_fund :
       process_fund(VARIABLES().local_dir,VARIABLES().suffix, fund, step_01, step_02, step_03, step_04)

if __name__ == '__main__' :
   import argparse
   import sys
   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(levelname)s %(module)s.%(funcName)s(%(lineno)s) - %(message)s'
   log.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=log.INFO)
   #log.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   parser = argparse.ArgumentParser(description='Portfoio Generator')
   parser.add_argument('--threshold', action='store', dest='threshold', type=float, default=0.12, help='magic number in refinement')
   parser.add_argument('--iterations', action='store', dest='portfolio_iterations', type=int, default=10000, help='Number of portfolios to try')
   parser.add_argument('--risk', action='store', dest='reduce_risk', type=int, default=7, help='Store a simple value')
   parser.add_argument('--returns', action='store', dest='reduce_returns', type=int, default=1, help='Store a simple value')
   parser.add_argument('--sector', action='store', dest='sector_cap', type=int, default=11, help='Max number of stocks per sector')
   parser.add_argument('--prices', action='store', dest='prices', default='Adj Close', help='Open|Close|Adj Close|Volume')
   parser.add_argument('--suffix', action='store', dest='suffix',default="",help='Store a simple value')
   parser.add_argument('--entity', action='store', dest='entity',default="stock",help='stock|fund')
   cli = vars(parser.parse_args())

   local_dir = "{}/local".format(env.pwd_parent)
   ini_list = env.list_filenames('local/*.ini')
   background = filter(lambda x : 'background.ini' in x, ini_list)
   background_files = filter(lambda x : 'stock_' in x or 'fund_' in x, background)
   background_files = list(background_files)
   price_list = env.list_filenames('local/historical_*/*pkl')
   floats_in_summary = ['CAGR','RETURNS','RISK','SHARPE','LEN'] 
   columns_drop = ['returns','risk','sharpe','mean']
   # too risky AMZN 
   # levelled off after initial increase
   # Not significanly better than SNP
   disqualified = ['TYL','CBPO','COG','CHTR','TPL','TTWO','MNST','VLO','WMB','REGN','GMAB','TCX'
                  , 'NEU','AOS','IFF','PPG','BKNG','STZ','REX','HQL','RGR'
                  ,'FB','WMT','CLX','BOOM','NYT','FNV']

    #keys = ['RISK','SHARPE','CAGR','GROWTH','RETURNS']

   main()

