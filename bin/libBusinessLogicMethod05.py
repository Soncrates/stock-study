# -*- coding: utf-8 -*-
"""
Created on Thu Apr 21 15:09:44 2022

@author: emers
"""
import logging as log
import pandas as PD
import os

from libBusinessLogic import INI_READ,INI_WRITE,PANDAS_FINANCE
from libFinance import HELPER as FINANCE
from newSharpe import PORTFOLIO as MONTERCARLO
from libUtils import combinations
from libDebug import trace

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
            stock = stock.upper()
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
        for index, row in ret.iterrows():
            row['NAME'] = cls.curate_name(row['NAME'])
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
        log.info(ret)
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
    def curate_name(cls,n) :
        log.debug(n)
        if "unknown" == n.lower() :
            return n
        ret = n.replace("'", "")
        ret = ret.replace(" - ", ", ")
        ret = ret.replace(", ", " ")
        ret = ret.replace("Common Stock", "Cmn Stk")
        ret = ret.replace("common stock", "Cmn Stk")
        ret = ret.replace("Common Shares", "Cmn Shr")
        ret = ret.replace("common shares", "Cmn Shr")
        ret = ret.replace("Depositary Shares", "Dep. Shr")
        ret = ret.replace("depositary shares", "Dep. Shr")
        ret = ret.replace("Ordinary Shares", "Ord. Shr")
        ret = ret.replace("ordinary shares", "Ord. Shr")
        ret = ret.replace("Limited", "Ltd.")
        ret = ret.replace("Corporation", "Corp.")
        ret = ret.replace("Pharmaceuticals", "Pharm.")
        ret = ret.replace("Technologies", "Tech.")
        ret = ret.replace("Technology", "Tech.")
        ret = ret.replace("International", "Int.")
        log.debug(ret)
        if ret == n :
           log.warn(ret)
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
        stock_list = ret.pop('stock',None)
        fund_list = ret.pop('fund',None)
        if stock_list is not None and stock_list.empty == False:
           log.info(stock_list.columns)
        if fund_list is not None and fund_list.empty == False:
           log.info(fund_list.columns)
        log.info(ret)
        MEAN.stats('stock',stock_list)
        for sector, group in cls.by_field(stock_list,cls.SECTOR):
            pass
        MEAN.stats('fund',fund_list)
        for sector, group in cls.by_field(fund_list,cls.SECTOR):
            pass
        return stock_list, fund_list
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
        log.info((t,list(distinct)))
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
        log.info(type(mean))
        log.info(mean)
        ret = ret.T
        ret['mean'] = mean
        ret.dropna()
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
        log.info(left)
        log.info(right)
        left = left.T.reset_index(drop=True)
        ret = PD.concat([left,right.T],sort=True)
        log.info(ret)
        return ret
class PORTFOLIO():
    @classmethod
    def truncate_5(cls, ret) :
        log.info(ret)
        if ret is None or ret.empty :
            log.warning("no data")
            return ret
        if len(ret) <= 5 :
           return ret
        ret = ret.T
        min_risk = ret.sort_values(['risk']).head(2)
        max_sharpe = ret.sort_values(['sharpe']).tail(2)
        log.debug(min_risk)
        log.debug(max_sharpe)
        ret = PD.concat([min_risk,max_sharpe])
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
           ret = []
        max_sharpe, min_dev = MONTERCARLO.find(prices, stocks=stocks, portfolios=portfolio_iterations, period=FINANCE.YEAR)
        log.info(max_sharpe)
        log.info(min_dev)
        ret.append(max_sharpe)
        ret.append(min_dev)
        log.info(PD.DataFrame(ret))
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

class FILTER_STOCKS_BY_PERFORNACE() :
    '''
    FILTER_STOCKS_BY_PERFORNACE based on background, filter list
    constructor params originate from the command line.
    '''
    def __init__(self, cap_size, reduce_risk, reduce_return) :
        self.cap_size = cap_size
        self.reduce_risk = reduce_risk
        self.reduce_return = reduce_return
    def __repr__(self):
        return f"Background filter (max:{self.cap_size},risk reduction:{self.reduce_risk}, return improve : {self.reduce_return})"
    def act(self, background) :
        while len(background) > self.cap_size :
              background = self.reduce(background)
        keys = background.index.values.tolist()
        log.info(background)
        log.info(keys)
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

class LOAD_HISTORICAL_DATA() :
    def __init__(self, price_list, price_column) :
        self.price_list = price_list
        self.price_column = price_column
    def __repr__(self):
        return f"Historical loader (column:{self.price_column})"
    def load(self, *ticker_list):
        filename_list = [ '{}{}.pkl'.format(os.path.sep,ticker.upper()) for ticker in ticker_list ]
        log.debug((ticker_list,filename_list))
        ret = {}
        for i, suffix in enumerate(filename_list) :
            filename = self.transform(suffix)
            if not filename : continue
            #name, temp = STOCK_TIMESERIES.load(filename)
            name, temp = PANDAS_FINANCE.LOAD(filename)
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
    def transform(self, suffix):
        ret = [ x for x in self.price_list if x.endswith(suffix)]
        if len(ret) == 0 :
           log.warning("{} not in {}".format(suffix, self.price_list[:5]))
           return None
        if len(ret) > 1 :
            suffix = os.path.sep + suffix
            ret = [ x for x in ret if x.endswith(suffix)]
        ret = ret[0]
        log.info(ret)
        return ret

class MONTE_CARLO_REFINEMENT_ROUGH() :
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
            log.warning("Lost data!!")
            return ret
        if isinstance(ret, list) :
            ret = PD.DataFrame(ret)
        log.info(ret)
        ret = ret.drop_duplicates()
        log.info(ret)
        ret.reset_index(drop=True, inplace=True)
        log.info(ret)
        ret.fillna(0, inplace=True)
        log.info(ret)
        ret = ret.T
        log.info(ret)
        return ret
class MONTE_CARLO_REFINEMENT_FINE() :
    def __init__(self, portfolio_iterations,threshold, columns_drop) :
        self.portfolio_iterations = portfolio_iterations
        self.threshold = threshold
        self.columns_drop = columns_drop
    def __repr__(self):
        return f"Sweet Spot (iterations:{self.portfolio_iterations},remove columns {self.columns_drop}, threshold : {self.threshold})"
    def rebalance(self, ret, threshold):
        if len(ret) <= threshold :
            log.info(ret)
            log.info("sufficently small")
            return ret
        log.info(repr(self))
        log.info(ret)
        while len(ret) > threshold :
            ret = ret[ret > min(ret)]
            ret = ret/ret.sum()
            log.info(ret)
        ret = round(ret,2)
        log.info(ret)
        return ret
    def find_average(self, ret):
        ret = ret.drop(labels=self.columns_drop,errors='ignore')
        log.info(repr(self))
        log.info(ret)
        ret = ret.T.mean()
        ret = self.rebalance(ret,5)
        stock_list = ret[ ret > 0 ]
        log.info(stock_list)
        stock_list = stock_list.T.keys().tolist()
        log.info(stock_list)
        log.info(ret)
        return ret, stock_list

    def act(self, data, prices) :
        avg, name_list = self.find_average(data)
        ret = None
        ret = PORTFOLIO.portfolio(prices,name_list,self.portfolio_iterations*5,ret)
        if isinstance(ret,list) :
            ret = PD.DataFrame(ret)
        ret = ret.drop_duplicates().T
        log.info(ret)
        ret['summary'] = avg
        log.info(ret)
        ret.fillna(0, inplace=True)
        log.info(ret)
        return ret, name_list

def process_stock(data_store, suffix, data, performance_based_as_1, load_prices_as_2, monte_carlo_as_3, monte_carlo_refinement_fine_as_4,reduce_99) :
    _90 = []
    _99 = []
    for sector, stock_name_subset in BACKGROUND.by_sector(data) :
        stock_summary, name_list = performance_based_as_1.act(stock_name_subset)
        _90.extend(name_list)

        summary = TRANSFORM.addMean(stock_summary)
        output_file = "{}/sector_{}{}.ini".format(data_store, sector,suffix)
        output_file = output_file.replace(' ','_')
        LOAD.config(output_file,**summary.to_dict())

        prices = load_prices_as_2.act(stock_summary)
        left = monte_carlo_as_3.act(stock_summary, prices)
        right, name_list = monte_carlo_refinement_fine_as_4.act(left, prices)
        _99.extend(name_list)
        left = PORTFOLIO.truncate_5(left)
        output_data = TRANSFORM.merge(left,right).T
        output_data = PORTFOLIO.massage(output_data)
        output_data = round(output_data,3)
        log.info(output_data)

        output_file = "{}/portfolio_{}{}.ini".format(data_store, sector,suffix)
        output_file = output_file.replace(" ", "_")
        LOAD.config(output_file,**output_data.to_dict())

    log.info(data)
    log.debug(data.index)
    log.info(_90)
    log.info(_99)
    
    output_file = "{}/sector_90{}.ini".format(data_store,suffix)
    _90 = data.loc[ _90 ]
    _90s = TRANSFORM.addMean(_90)
    log.info(_90s)
    LOAD.config(output_file,**_90s.to_dict())

    output_file = "{}/sector_99{}.ini".format(data_store,suffix)
    _99 = data.loc[ _99 , : ]
    log.info(_99)
    MEAN.stats('99',_99)
    _99,dummy = reduce_99.act(_99)
    log.info(_99)
    _99s = TRANSFORM.addMean(_99)
    _99s = round(_99s,5)
    log.info(_99s)
    LOAD.config(output_file,**_99s.to_dict())

    prices = load_prices_as_2.act(_99)
    portfolios = monte_carlo_as_3.act(_99, prices)
    portfolios = PORTFOLIO.truncate_5(portfolios)
    portfolios = PORTFOLIO.massage(portfolios)
    if portfolios is None or portfolios.empty :
        log.warning("portfolio is empty")
        return
    portfolios = round(portfolios,5)
    output_file = "{}/portfolio_99{}.ini".format(data_store,suffix)
    LOAD.config(output_file,**portfolios.to_dict())

def process_fund(data_store,suffix, data, performance_based_as_1, step_02, monte_carlo_as_3, monte_carlo_refinement_fine) :
    _90_Alt = []
    _99 = {}
    for sector, fund_name_subset in BACKGROUND.by_sector(data) :
        fund_summary, name_list = performance_based_as_1.act(fund_name_subset)
        _90_Alt.extend(name_list)

        summary = TRANSFORM.addMean(fund_summary)
        output_file = "{}/fund_{}{}.ini".format(data_store, sector,suffix)
        output_file = output_file.replace(' ','_')
        LOAD.config(output_file,**summary.to_dict())

        prices = step_02.act(fund_summary)
        left = monte_carlo_as_3.act(fund_summary, prices)
        right, _99 = monte_carlo_refinement_fine.act(left, prices,_99)
        left = PORTFOLIO.truncate_5(left)
        output_data = TRANSFORM.merge(left,right).T
        output_data = PORTFOLIO.massage(output_data)
        output_data = round(output_data,3)

        output_file = "{}/portfolio_{}{}.ini".format(data_store, sector,suffix)
        output_file = output_file.replace(" ", "_")
        LOAD.config(output_file,**output_data.to_dict())