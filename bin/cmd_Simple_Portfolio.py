#!/usr/bin/env python

import logging
import sys
import pandas as pd
from libCommon import INI, combinations, exit_on_exception
from libDebug import trace, cpu
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libSharpe import PORTFOLIO, HELPER as SHARPE

class EXTRACT() :
    _singleton = None
    def __init__(self, _env, config_list, file_list, input_file, output_file) :
        self._env = _env
        self.config_list = config_list
        self.file_list = file_list
        self.input_file = input_file
        self.output_file = output_file
    @classmethod
    def singleton(cls, **kwargs) :
        if not (cls._singleton is None) :
           return cls._singleton
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
        cls._singleton = cls(_env,config_list,file_list, input_file, output_file)
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
        data = cls.singleton().input_file
        logging.info('reading file {}'.format([data]))
        ret = {}
        for path, section, stock_sector, stock_list in INI.loadList(*[data]) :
            if section not in ret :
               ret[section] = {}
            ret[section][stock_sector] = stock_list
        target='MERGED'
        ret = ret.get(target,ret)
        stock_list = cls.flatten(ret.values())
        return ret, stock_list
    @classmethod
    def load(cls, value_list) :
        file_list = cls.singleton().file_list
        ret = {}
        for name, data in STOCK_TIMESERIES.read(file_list, value_list) :
            ret[name] = data['Adj Close']
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
    @classmethod
    @trace
    def _getList(cls, data, stocks, ret) :
        if ret is None :
           ret = pd.DataFrame()
        max_sharpe, min_dev = PORTFOLIO.find(data, stocks=stocks, portfolios=10000, period=FINANCE.YEAR)
        ret = ret.append(max_sharpe)
        ret = ret.append(min_dev)
        return ret
    @classmethod
    def getList(cls, stock_list,data_list) :
        ret = None
        for data, subset in cls.getStocks(stock_list) :
            ret = cls._getList(data,subset,ret)
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
        save_file = EXTRACT.singleton().output_file
        ret = INI.init()
        for key in sorted(data) :
            logging.info(key)
            value = data[key]
            INI.write_section(ret,key,**value)
        ret.write(open(save_file, 'w'))
        logging.info('writing to file {}'.format(save_file))

@exit_on_exception
@trace
def main() : 
    bySector, stock_list = EXTRACT.read()
    logging.info(bySector)
    data_list = EXTRACT.load(stock_list)
    stocks = sorted(data_list.keys())
    ret = TRANSFORM_PORTFOLIO.getList(stocks,data_list)
    ret = TRANSFORM_PORTFOLIO.to_ini(ret)
    LOAD.config(ret)

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.DEBUG)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')

   main()

