#!/usr/bin/env python

import logging
import types
from functools import reduce
import pandas as pd
from libCommon import INI
from libUtils import combinations, exit_on_exception
from libKMeans import EXTRACT_K
from libDebug import trace, cpu
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libSharpe import PORTFOLIO, HELPER as SHARPE

class EXTRACT() :
    _singleton = None
    def __init__(self, _env, config_list, file_list, input_file, output_file,background,category) :
        self._env = _env
        self.config_list = config_list
        self.file_list = file_list
        self.input_file = input_file
        self.output_file = output_file
        self.background = background
        self.category = category
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
        #msg = sorted(globals().keys())
        #msg = filter(lambda x : '__' not in x, msg)
        #msg = filter(lambda x: not isinstance((globals().get(x)),types.ModuleType), msg)
        #msg = filter(lambda x: not isinstance((globals().get(x)),types.FunctionType), msg)
        #msg = list(msg)
        #print(msg)
        #msg = map(lambda x: type(globals().get(x)), msg)
        #msg = list(msg)
        #print(msg)
        #for i, j in enumerate(vars(types)) :
        #    print((i,j))
        target = 'env'
        _env = globals().get(target,None)
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
        target = "file_list"
        file_list = globals().get(target,[])
        target = 'background'
        background = globals().get(target,[])
        background = list(background)
        target = 'category'
        category = globals().get(target,[])
        category = list(category)

        cls._singleton = cls(_env,config_list,file_list, input_file, output_file,background,category)
        return cls._singleton
    @classmethod
    def flatten(cls, ret) :
        logging.debug(ret)
        ret = [item for sublist in ret for item in sublist]
        ret = sorted(set(ret))
        logging.info((len(ret),ret))
        return ret
    @classmethod
    def read(cls) :
        data = cls.instance().input_file
        logging.info('reading file {}'.format([data]))
        ret = {}
        for path, section, stock_sector, stock_list in INI_READ.read(*[data]) :
            if section not in ret :
               ret[section] = {}
            ret[section][stock_sector] = stock_list
        target='MERGED'
        ret = ret.get(target,ret)
        stock_list = cls.flatten(ret.values())
        return ret, stock_list
    @classmethod
    def load(cls, value_list) :
        file_list = cls.instance().file_list
        ret = {}
        for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
            ret[name] = data['Adj Close']
        return ret
class EXTRACT_SECTOR() :
    _cache = None
    @classmethod
    def read(cls) :
        load_file = EXTRACT.instance().category
        if not (cls._cache is None) :
           return cls._cache
        logging.info('reading file {}'.format(load_file))
        ret = {}
        for path, section, key, ticker_list in INI_READ.read(*load_file) :
            entity = 'Stock'
            if 'fund' in path :
               key = '{} ({})'.format(section,key)
               entity = 'Fund'
            for ticker in ticker_list :
                if ticker not in ret :
                   ret[ticker] = {}
                ret[ticker]['SECTOR'] = key
                ret[ticker]['ENTITY'] = entity
        ret = pd.DataFrame(ret).T
        logging.info(ret)
        cls._cache = ret
        return ret
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
            logging.info((entry,len(group),sorted(group.index.values)))
            yield entry, group

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
        for path, key, stock, value in INI_READ.read(*load_file) :
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

class TRANSFORM_STOCK() :
    @classmethod
    def getList(cls,data) :
        ret = None
        if isinstance(data,dict) :
           ret = sorted(data)
        if ret is None :
           ret = sorted(data.index.values)
           if not isinstance(ret[0],str) :
              ret = sorted(data.columns.values)
        logging.info((len(ret),ret))
        return ret
    @classmethod
    def getSharpe(cls, stock_list, data) :
        ret = map(lambda d : SHARPE.find(data[d], period=FINANCE.YEAR), stock_list)
        ret = dict(zip(stock_list, ret))
        ret = pd.DataFrame(ret).T
        logging.info(ret.head(3))
        logging.info(ret.tail(3))
        return ret
    @classmethod
    def truncate(cls, ret, size) :
        if len(ret) <= size :
           return ret
        data = EXTRACT.load(ret)
        stock_list = cls.getList(data)
        data = cls.getSharpe(stock_list,data)
        data = data.sort_values(['risk']).head(size)
        ret = data.T.columns.values
        ret = sorted(ret)
        logging.info((len(ret),ret))
        return ret

class TRANSFORM_PORTFOLIO() :
    @classmethod
    def getStocks(cls, stock_list) :
        minimum_portfolio_size = len(stock_list)-2
        count = len(stock_list)-1
        while count > minimum_portfolio_size :
            data = EXTRACT.load(stock_list)
            stock_list = sorted(data.keys())
            for subset in combinations(stock_list,count) :
                logging.info((len(subset),sorted(subset)))
                yield data, sorted(subset)
            stock_list = TRANSFORM_STOCK.truncate(stock_list,count)
            count = len(stock_list)-1

    @classmethod
    def smart_truncate(cls, ret) :
        if ret is None :
           ret = pd.DataFrame()
        size = len(ret)
        if size < 1000 :
            return ret
        return cls.truncate(ret,50)
    @classmethod
    def truncate(cls, ret,count) :
        if ret is None :
           ret = pd.DataFrame()
        min_risk = ret.sort_values(['risk']).head(count)
        max_sharpe = ret.sort_values(['sharpe']).tail(count)
        ret = pd.DataFrame()
        ret = ret.append(min_risk)
        ret = ret.append(max_sharpe)
        return ret
    @classmethod
    @trace
    def _getList(cls, data, stocks, ret) :
        if ret is None :
           ret = pd.DataFrame()
        max_sharpe, min_dev = PORTFOLIO.find(data, stocks=stocks, portfolios=100, period=FINANCE.YEAR)
        ret = ret.append(max_sharpe)
        ret = ret.append(min_dev)
        return ret
    @classmethod
    @trace
    def getList(cls, stock_list) :
        ret = None
        for data, subset in cls.getStocks(stock_list) :
            ret = cls._getList(data,subset,ret)
            ret = cls.smart_truncate(ret)
        if len(ret) > 5 :
            #ret = cls.truncate(ret,5)
            ret = cls.truncate(ret,2)
        ret = ret.T
        ret.fillna(0,inplace=True)
        return ret
    @classmethod
    def _to_ini(cls,ret) :
        zero_list = filter(lambda x : ret[x] == 0.0, sorted(ret))
        zero_list = list(zero_list)
        for key in zero_list :
            ret.pop(key,None)
        stock_list = sorted(ret)
        value_list = map(lambda x : round(ret[x],4), stock_list)
        ret = dict(zip(stock_list,value_list))
        return ret
    @classmethod
    def to_ini(cls,ret) :
        ret = ret.to_dict()
        portfolio_list = sorted(ret)
        value_list = map(lambda x : cls._to_ini(ret[x]), portfolio_list)
        key_list = map(lambda x : 'portfolio_{}'.format(x), portfolio_list)
        ret = dict(zip(key_list,value_list))
        logging.debug(ret)
        return ret
    @classmethod
    def round_values(cls, **data) :
        key_list = data.keys()
        value_list = map(lambda x : data[x], key_list)
        value_list = map(lambda x : round(x,2), value_list)
        ret = dict(zip(key_list,value_list))
        return ret
class LOAD() :
    @classmethod
    def config(cls,data) :
        save_file = EXTRACT.instance().output_file
        INI_WRITE.write(save_file,**data)
        logging.info('writing to file {}'.format(save_file))

def df_crossjoin(df_left, df_right, **kwargs):
    """
    Make a cross join (cartesian product) between two dataframes by using a constant temporary key.
    Also sets a MultiIndex which is the cartesian product of the indices of the input dataframes.
    See: https://github.com/pydata/pandas/issues/5401
    :param df_left dataframe 1
    :param df_right dataframe 2
    :param kwargs keyword arguments that will be passed to pd.merge()
    :return cross join of df_left and df_right
    """
    logging.info(df_left)
    logging.info(df_right)
    if not isinstance(df_left, pd.DataFrame) :
        df_left = pd.DataFrame(df_left)
    if not isinstance(df_right, pd.DataFrame) :
        df_right = pd.DataFrame(df_right)
    df_left['_tmpkey'] = 1
    df_right['_tmpkey'] = 1

    res = pd.merge(df_left, df_right, on='_tmpkey', **kwargs).drop('_tmpkey', axis=1)

    df_left.drop('_tmpkey', axis=1, inplace=True)
    df_right.drop('_tmpkey', axis=1, inplace=True)

    return res

def partition(data) :
    test = {}
    for sector, group in EXTRACT_SECTOR.enumerate(data) :
        if sector == 'Energy' :
            #Error Energy comes up as Nan because only one group
            continue
        if sector == 'Financial Services' :
            #Performance
            continue
        test[sector] = []
        for _K, k in EXTRACT_K.enumerate(group) :
            if len(k) > 5 :
                #performance
                k = k.sort_values(['RISK']).head(4)
                logging.info(k)
            if len(k) == 1 :
                #outlier
                continue
            test[sector].append(_K)
    temp = []
    for i, key in enumerate(sorted(test.keys())) :
        v = pd.Series(test[key], name=key)
        temp.append(v)
    temp = reduce(lambda x,y : df_crossjoin(x,y),temp)
    logging.info(temp)
    for i, entry in temp.iterrows() :
        flags = entry.to_dict()
        portfolio = []
        for f in sorted(flags.keys()) :
            _temp = data[data['SECTOR'] == f]
            _temp = _temp[_temp['K'] == flags[f]]
            _temp = list(_temp.index.values)
            portfolio += _temp
        yield portfolio
def process() :
    bySector, stock_list = EXTRACT.read()
    sector = EXTRACT_SECTOR.enrich(stock_list)
    summary = EXTRACT_SUMMARY.enrich(stock_list)
    ret = pd.concat([sector,summary], axis=1, join='inner')
    logging.info(ret)
    K = []
    for sector, data in EXTRACT_SECTOR.enumerate(ret) :
        _YYY = [ 'CAGR', 'RISK', 'SHARPE' ]
        flag = len(data) < 5
        if flag :
           data['K'] = 0
           K.append(data['K'])
           continue
        groups = int(len(data)/5)+1
        v = EXTRACT_K.cluster(data[_YYY],groups)
        K.append(v)
    K = pd.concat(K, axis=0)
    logging.info(K)
    ret = pd.concat([ret,K['K']], axis=1, join='inner')
    logging.info(ret)
    return ret

@exit_on_exception
@trace
def main() : 
    stock_summary = process()
    portfolio_list = pd.DataFrame()
    for portfolio in partition(stock_summary) :
        ret = TRANSFORM_PORTFOLIO.getList(portfolio)
        portfolio_list = pd.concat([portfolio_list,ret], axis=1 )
        portfolio_list.fillna(0,inplace=True)
        if len(portfolio_list) > 50 :
           ret = TRANSFORM_PORTFOLIO.truncate(portfolio_list.T, 6)
           logging.info(ret)
           portfolio_list = ret.T
           config = TRANSFORM_PORTFOLIO.to_ini(portfolio_list)
           LOAD.config(config)
    if len(portfolio_list) > 10 :
       ret = TRANSFORM_PORTFOLIO.truncate(portfolio_list.T, 5)
       portfolio_list = ret.T
    ret = TRANSFORM_PORTFOLIO.to_ini(portfolio_list)
    LOAD.config(ret)
    return
    data_list = EXTRACT.load(stock_list)
    stocks = sorted(data_list.keys())

if __name__ == '__main__' :
   import sys
   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.DEBUG)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')
   category = filter(lambda x : 'stock_by_sector.ini' in x , ini_list)
   background = filter(lambda x : 'background.ini' in x, ini_list)
   background = filter(lambda x : 'stock_' in x or 'fund_' in x, background)


   main()

