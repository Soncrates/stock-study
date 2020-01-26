#!/usr/bin/env python

import logging
import sys
from libCommon import exit_on_exception,INI
from libDebug import trace, cpu
from libNASDAQ import NASDAQ, HELPER_FUND as FUND
from libFinance import STOCK_TIMESERIES

class EXTRACT() :
    _singleton = None
    def __init__(self, _env, config_list, file_list, input_file, output_file,background_file,local_dir,data_store) :
        self.env = _env
        self.config_list = config_list
        self.file_list = file_list
        self.input_file = input_file
        self.output_file = output_file
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
        input_file = None
        if len(_env.argv) > 0 :
           input_file = _env.argv[0]
        target = 'background_file'
        background_file = globals().get(target,None)
        target = 'output_file'
        output_file = globals().get(target,None)
        if len(_env.argv) > 1 :
           output_file = _env.argv[1]
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
        _env.mkdir(data_store)
        cls._singleton = cls(_env,config_list,file_list, input_file, output_file,background_file,local_dir,data_store)
        return cls._singleton
    @classmethod
    def config() :
        ini_list = EXTRACT.instance().config_list
        logging.info("loading results {}".format(ini_list))
        for path, section, key, stock_list in INI.loadList(*ini_list) :
            yield path, section, key, stock_list

class LOAD() :

    @classmethod
    def config(cls, **config) :
        save_file = EXTRACT.instance().output_file
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

    @classmethod
    def read(cls, *stock_list) :
        data_store = EXTRACT.instance().data_store
        reader = STOCK_TIMESERIES.init()
        dud = []
        for stock in stock_list :
            filename = '{}/{}.pkl'.format(data_store,stock)
            ret = reader.extract_from_yahoo(stock)
            if ret is None :
               dud.append(stock)
               continue
            logging.debug(ret.tail(5))
            STOCK_TIMESERIES.save(filename, stock, ret)
        size = len(stock_list) - len(dud)
        logging.info("Total {}".format(size))
        if len(dud) > 0 :
           logging.warn(dud)

def process() :
    recognized = FUND._Type.values()
    stock_list, etf_list, alias = NASDAQ.init().stock_list()
    fund_list = NASDAQ.init().fund_list()
    logging.info(fund_list[0])
    ret = {}
    background = {}
    for i, fund in enumerate(fund_list) :
        section = fund['Type']
        key = fund['Category']
        name = fund['Fund Symbol']
        fullname = fund['Fund Name']
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
        fullname = fullname.replace('%',' percent')
        fullname = fullname.replace(' Fd',' Fund')
        background[name] = fullname
    stock_list = list(background.keys())
    background = {'FULL_NAME' : background }
    return ret, background, stock_list

@exit_on_exception
@trace
def main() : 
    ret,background,stock_list = process()
    LOAD.config(**ret)
    LOAD.background(**background)
    LOAD.read(*stock_list)

if __name__ == '__main__' :
   import sys
   import logging
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   #logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   ini_list = env.list_filenames('local/*.ini')
   file_list = env.list_filenames('local/historical_prices/*pkl')
   output_file = '../local/fund.ini'
   background_file = '../local/fund_background.ini'

   local_dir = '{}/local'.format(env.pwd_parent)
   data_store = '{}/historical_prices'.format(local_dir)
   data_store = '../local/historical_prices_fund'

   main()

