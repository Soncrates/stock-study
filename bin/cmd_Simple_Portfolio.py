#!/usr/bin/env python

import logging
import sys
import pandas as pd
from libCommon import INI_READ, INI_WRITE
from libUtils import combinations
from libDecorators import exit_on_exception, singleton
from libDebug import trace, cpu
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE, TRANSFORM_SHARPE
from newSharpe import PORTFOLIO

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
    var_names = ["config_file", "stock_data", 'output_file', 'env' ]

    def __init__(self) :
        values = get_globals(*VARIABLES.var_names)
        self.__dict__.update(**values)
        if len(self.env.argv) > 0 :
           self.config_file = self.env.argv[0]
        if len(self.env.argv) > 1 :
           self.output_file =self.env.argv[1]

class EXTRACT() :
    stock_data = []
    config_file = None
    @classmethod
    def flatten(cls, ret) :
        logging.debug(ret)
        ret = [item for sublist in ret for item in sublist]
        ret = sorted(set(ret))
        logging.info((len(ret),ret))
        return ret
    @classmethod
    def read(cls) :
        logging.info('reading file {}'.format(cls.config_file))
        ret = {}
        for path, section, stock_sector, stock_list in INI_READ.read(*[cls.config_file]) :
            if section not in ret :
               ret[section] = {}
            ret[section][stock_sector] = stock_list
        target='MERGED'
        ret = ret.get(target,ret)
        stock_list = cls.flatten(ret.values())
        return ret, stock_list
    @classmethod
    def load(cls, value_list) :
        ret = {}
        for name, data in STOCK_TIMESERIES.read(cls.stock_data, value_list) :
            ret[name] = data['Adj Close']
        return ret
class TRANSFORM_STOCK() :
    risk_free_rate = 0.02

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
    def _getSharpe(cls, data) :
        risk, returns = FINANCE.findRiskAndReturn(data, period=FINANCE.YEAR)
        ret = TRANSFORM_SHARPE.sharpe(risk,returns,cls.risk_free_rate)
        return ret
    
    @classmethod
    def getSharpe(cls, stock_list, data) :
        logging.info(data)
        logging.info(stock_list)
        ret = map(lambda d : cls._getSharpe(data[d]), stock_list)
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
    portfolio_iterations = 10000
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
        if isinstance(ret,dict) :
           ret = pd.DataFrame(ret)
        if isinstance(data,dict) :
           data = pd.DataFrame(data)
        max_sharpe, min_dev = PORTFOLIO.find(data, stocks=stocks, portfolios=cls.portfolio_iterations, period=FINANCE.YEAR)
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
    def config(cls,data,save_file) :
        INI_WRITE.write(save_file,**data)
        logging.info('writing to file {}'.format(save_file))

@exit_on_exception
@trace
def main() : 
    EXTRACT.config_file = VARIABLES().config_file
    EXTRACT.stock_data = VARIABLES().stock_data
    bySector, stock_list = EXTRACT.read()
    logging.info(bySector)
    data_list = EXTRACT.load(stock_list)
    stocks = sorted(data_list.keys())
    ret = TRANSFORM_PORTFOLIO.getList(stocks,data_list)
    ret = TRANSFORM_PORTFOLIO.to_ini(ret)
    LOAD.config(ret, VARIABLES().output_file)

if __name__ == '__main__' :
   import sys
   import logging
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.DEBUG)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   TRANSFORM_PORTFOLIO.portfolio_iterations = 10000
   TRANSFORM_PORTFOLIO.portfolio_iterations = 10
   config_file = '../test/testConfig/refined_stock_list.ini'
   output_file = '../test/testResults/refined_report.ini'
   stock_data = env.list_filenames('local/historical_prices/*pkl')

   main()

