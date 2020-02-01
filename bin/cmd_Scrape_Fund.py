#!/usr/bin/env python

import logging
import sys
from libCommon import INI, exit_on_exception, log_on_exception
from libDebug import trace, cpu
from libNASDAQ import NASDAQ, TRANSFORM_FUND as FUND
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libFinance import TRANSFORM_SHARPE as SHARPE, TRANSFORM_CAGR as CAGR

class EXTRACT() :
    _singleton = None
    def __init__(self, _env, config_list, file_list, output_file_by_type,background_file,local_dir,data_store) :
        self.env = _env
        self.config_list = config_list
        self.file_list = file_list
        self.output_file_by_type = output_file_by_type
        self.background_file = background_file
        self.local_dir = local_dir
        self.data_store = data_store
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
        target = 'background_file'
        background_file = globals().get(target,None)
        target = 'output_file_by_type'
        output_file_by_type = globals().get(target,None)
        if len(_env.argv) > 1 :
           output_file_by_type = _env.argv[1]
        target = 'ini_list'
        config_list = globals().get(target,[])
        if not isinstance(config_list,list) :
           config_list = list(config_list)
        target = "file_list"
        file_list = globals().get(target,[])
        target = "local_dir"
        local_dir = globals().get(target,None)
        target = "data_store"
        data_store = globals().get(target,[])
        cls._singleton = cls(_env,config_list,file_list, output_file_by_type,background_file,local_dir,data_store)
        return cls._singleton
    @classmethod
    def config() :
        ini_list = EXTRACT.instance().config_list
        logging.info("loading results {}".format(ini_list))
        for path, section, key, stock_list in INI.loadList(*ini_list) :
            yield path, section, key, stock_list
    @classmethod
    @log_on_exception
    def prices(cls, ticker) :
        data_store = cls.instance().data_store
        filename = '{}/{}.pkl'.format(data_store,ticker)
        name, data = STOCK_TIMESERIES.load(filename)
        return data

class TRANSFORM() :
    _prices = 'Adj Close'
    @classmethod
    def ticker(cls,entry) :
        target = 'Fund Symbol'
        ret = entry.get(target,None)
        return ret
    @classmethod
    def name(cls,entry) :
        target = 'Fund Name'
        ret = entry.get(target,None)
        return ret
    @classmethod
    def type(cls,entry) :
        target = 'Type'
        ret = entry.get(target,None)
        return ret
    @classmethod
    def category(cls,entry) :
        target = 'Category'
        ret = entry.get(target,None)
        return ret
    @classmethod
    def prices(cls, data) :
          cagr = 0
          stdev = 0
          _len = 0
          sharpe = 0
          growth = 0
          if data is None :
             return cagr, stdev, _len, sharpe, growth
          prices = data[cls._prices]
          cagr, growth = CAGR.find(prices)
          ret = SHARPE.find(prices, period=FINANCE.YEAR, span=0)
          target = 'risk'
          stdev = ret.get(target,stdev)
          target = 'sharpe'
          sharpe = ret.get(target,sharpe)
          target = 'len'
          _len = ret.get(target,_len)
          return cagr, stdev, _len, sharpe, growth

        
class LOAD() :

    @classmethod
    def config(cls, **config) :
        save_file = EXTRACT.instance().output_file_by_type
        ret = INI.init()
        for key in sorted(config) :
            value = config.get(key,[])
            INI.write_section(ret,key,**value)
        ret.write(open(save_file, 'w'))
        logging.info("results saved to {}".format(save_file))
    @classmethod
    def background(cls, **config) :
        save_file = EXTRACT.instance().background_file
        ret = INI.init()
        for key in sorted(config) :
            value = config.get(key,[])
            INI.write_section(ret,key,**value)
        ret.write(open(save_file, 'w'))
        logging.info("results saved to {}".format(save_file))

def process_prices(ticker_list) :
    cagr_list = []
    stdev_list = []
    len_list = []
    sharpe_list = []
    growth_list = []
    for ticker in ticker_list :
        prices = EXTRACT.prices(ticker)
        cagr, stdev, _len, sharpe, growth = TRANSFORM.prices(prices)
        cagr_list.append(cagr)
        stdev_list.append(stdev)
        len_list.append(_len)
        sharpe_list.append(sharpe)
        growth_list.append(cgrowth)
    cagr = dict(zip(ticker_list,cagr_list))
    stdev = dict(zip(ticker_list,stdev_list))
    sharpe = dict(zip(ticker_list,sharpe_list))
    _len = dict(zip(ticker_list,len_list))
    growth = dict(zip(ticker_list,growth_list))
    ret = { 'CAGR' : cagr, 'RISK' : stdev, 'SHARPE' : sharpe, 'LEN' : _len, 'GROWTH' : growth }
    return ret
        
def process_names(fund_list) :
    ticker_list = map(lambda x : TRANSFORM.ticker(x), fund_list)
    ticker_list = list(ticker_list)
    name_list = map(lambda fund : TRANSFORM.name(fund), fund_list)
    name_list = map(lambda name : name.replace('%', ' percent'), name_list)
    name_list = map(lambda name : name.replace(' Fd', ' Fund'), name_list)
    ret = dict(zip(ticker_list,name_list))
    return ret

def process_by_type(fund_list) :
    recognized = FUND._Type.values()
    logging.info(fund_list[0])
    ret = {}
    for i, fund in enumerate(fund_list) :
        section = TRANSFORM.type(fund)
        key = TRANSFORM.category(fund)
        name = TRANSFORM.ticker(fund)
        if section is None or len(section) == 0 :
            continue
        if key is None or len(key) == 0 :
            continue
        if section not in recognized :
           key = "{}_{}".format(section,key)
           section = 'UNKNOWN'
        if section not in ret :
           ret[section] = {}
        curr = ret[section]
        if key not in curr :
           curr[key] = []
        curr[key].append(name)
    return ret

@exit_on_exception
@trace
def main() : 
    fund_list = NASDAQ.init().fund_list()
    config = process_by_type(fund_list)
    LOAD.config(**config)

    names = process_names(fund_list)
    ticker_list = sorted(names.keys())
    background = process_prices(ticker_list)
    background['NAME'] = names
    LOAD.background(**background)

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')
   output_file_by_type = '../local/fund_by_type.ini'
   background_file = '../local/fund_background.ini'

   local_dir = '{}/local'.format(env.pwd_parent)
   data_store = '../local/historical_prices_fund'

   main()

