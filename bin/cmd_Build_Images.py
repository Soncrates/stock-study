#!/usr/bin/env python

import logging
from copy import deepcopy
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
#plt.style.use('fivethirtyeight')
#plt.style.use('ggplot')
#plt.style.use('seaborn-whitegrid')
fig, ax = plt.subplots()
ax.grid(which='major', linestyle='-', linewidth='0.5', color='gray')

from libCommon import INI, exit_on_exception
from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
from libDebug import trace
from libGraph import LINE, BAR, POINT, save, HELPER as GRAPH
from libSharpe import PORTFOLIO, HELPER as SHARPE
'''
   Graph portfolios to determine perfomance, risk, diversification
'''
class EXTRACT() :
    _background_cache = None
    _singleton = None
    def __init__(self, _env, local_dir, _data_store, input_file,output_file,background,_benchmark,repo) :
        self._env = _env
        self.local_dir = local_dir
        self._data_store = _data_store
        self.input_file = input_file
        self.output_file = output_file
        self.background = background
        self._benchmark = _benchmark
        self.repo = repo
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
        target = "local_dir"
        local_dir = globals().get(target,None)
        target = 'data_store'
        _data_store = globals().get(target,'')
        if not isinstance(_data_store,str) :
           _data_store = str(_data_store)
        target = 'input_file'
        input_file = globals().get(target,'')
        if not isinstance(input_file,list) :
           input_file = [input_file]
        if len(_env.argv) > 0 :
           input_file = _env.argv[0]
        target = 'output_file'
        output_file = globals().get(target,'')
        if len(_env.argv) > 1 :
           output_file = _env.argv[1]
        target = 'background'
        background = globals().get(target,[])
        target = 'benchmark'
        _benchmark = globals().get(target,[])
        if not isinstance(_benchmark,list) :
           _benchmark = list(_benchmark)
        target = "repo_stock"
        repo_stock = globals().get(target,[])
        target = "repo_fund"
        repo_fund = globals().get(target,[])
        repo = repo_stock + repo_fund
        cls._singleton = cls(_env,local_dir,_data_store,input_file,output_file,background,_benchmark,repo)
        return cls._singleton
    @classmethod
    def prep(cls) :
        data_store = cls.instance()._data_store
        logging.info('making data store {}'.format(data_store))
        cls.instance()._env.mkdir(data_store)
    @classmethod
    def readRepoData(cls, value_list) :
        repo = cls.instance().repo
        ret = {}
        for name, data in STOCK_TIMESERIES.read(repo, value_list) :
            logging.info((name,type(data),data.shape))
            ret[name] = data
        return ret
    @classmethod
    def readPrices(cls,stock_list) :
        ret = cls.readRepoData(stock_list)
        stock_list = sorted(ret)
        price_list = map(lambda stock : pd.DataFrame(ret[stock])['Adj Close'], stock_list)
        price_list = list(price_list)
        ret = dict(zip(stock_list, price_list))
        ret = pd.DataFrame(ret, columns=stock_list)
        logging.info(ret.head(3))
        logging.info(ret.tail(3))
        return ret
    @classmethod
    def readPortfolio(cls) :
        portfolio = cls.instance().input_file
        logging.info('reading file {}'.format(portfolio))
        ret = {}
        for path, section, key, weight in INI.loadList(*[portfolio]) :
            if section.startswith('dep_') :
               continue
            if section not in ret :
               ret[section] = {}
            ret[section][key] = float(weight[0])
        return ret
    @classmethod
    def readEnrich(cls) :
        enrich = cls.instance().background
        if not (cls._background_cache is None) :
           logging.info('reading cache {}'.format(enrich))
           return cls._background_cache
        logging.info('reading file {}'.format(enrich))
        ret = {}
        for path, section, key, ticker_list in INI.loadList(*enrich) :
            if 'fund' in path :
               key = '{} ({})'.format(section,key)
            for ticker in ticker_list :
                if ticker not in ret : 
                   ret[ticker] = {}
                ret[ticker]['Sector'] = key
        cls._background_cache = ret
        return ret
    @classmethod
    def readSector(cls,stock) :
        enrich = cls.readEnrich().get(stock,{})
        target = 'Sector'
        ret = enrich.get(target,None)
        if ret is None :
           ret = enrich.get('Category','Unknown')
        return ret
    @classmethod
    def readBenchmark(cls) :
        benchmark = cls.instance()._benchmark
        logging.info('reading file {}'.format(benchmark))
        ret = {}
        for path, section, key, stock_list in INI.loadList(*benchmark) :
            #if section not in ['MOTLEYFOOL', 'Index'] : continue
            if section not in ['PERSONAL', 'Index'] : continue
            if section == 'MOTLEYFOOL' :
               if 'NASDAQ' not in key : continue
               if 'FUND' not in key : continue
            if section == 'Index' :
               if '500' not in key : continue
            ret[key] = stock_list
        logging.debug(ret)
        return ret
class TRANSFORM_STOCK() :
    @classmethod
    def parseStockList(cls,data) :
        if isinstance(data,dict) :
           return sorted(data)
        ret = sorted(data.index.values)
        if not isinstance(ret[0],str) :
           ret = sorted(data.columns.values)
        logging.info(ret)
        return ret
    @classmethod
    def summarizeReturns(cls,data) :
        stock_list = cls.parseStockList(data)
        ret = map(lambda stock: FINANCE.findDailyReturns(data[stock]), stock_list)
        ret = dict(zip(stock_list,ret))
        return ret
    @classmethod
    def summarizeSharpe(cls,data) :
        stock_list = cls.parseStockList(data)
        ret = map(lambda d : SHARPE.find(data[d], period=FINANCE.YEAR), stock_list)
        ret = dict(zip(stock_list, ret))
        ret = pd.DataFrame(ret).T
        logging.info(ret)
        return ret
    @classmethod
    def summarizeCAGR(cls,data) :
        stock_list = cls.parseStockList(data)
        ret = map(lambda d : FINANCE.CAGR(data[d]), stock_list)
        ret = dict(zip(stock_list, ret))
        ret = pd.DataFrame([ret]).T
        logging.info(ret)
        return ret
    @classmethod
    def _summarizeBalance(cls,pd) :
        ret = pd.dropna(how='all')
        ret = ret.iloc[-1]/ret.iloc[0]
        ret = round(ret,2)
        return ret
    @classmethod
    def summarizeBalance(cls,data) :
        stock_list = cls.parseStockList(data)
        ret = map(lambda x : cls._summarizeBalance(data[x]), stock_list)
        ret = dict(zip(stock_list,ret))
        ret = pd.DataFrame([ret]).T
        return ret
    @classmethod
    def enrichSector(cls,ret) :
        stock_list = cls.parseStockList(ret)
        ret = map(lambda stock : EXTRACT.readSector(stock), stock_list)
        ret = dict(zip(stock_list,ret))
        ret = pd.Series(ret)
        logging.info(ret)
        return ret
    @classmethod
    def enrichWeight(cls,weight,ret) :
        stock_list = cls.parseStockList(ret)
        ret = map(lambda stock : weight.get(stock,0.0), stock_list)
        ret = dict(zip(stock_list,ret))
        ret = pd.Series(ret)
        logging.info(ret)
        return ret

class TRANSFORM_PORTFOLIO() :
    meta_columns = ['returns', 'risk', 'sharpe']
    @classmethod
    def getSummary(cls,weights) :
        ret = weights.round(2)
        logging.debug(ret)
        portfolio_columns = ['returns','risk','sharpe','CAGR','Initial Balance','Final Balance']

        returns = ret['weighted_returns'].to_dict()
        returns = sum(returns.values())
        returns = round(returns,2)
        risk = ret['weighted_risk'].to_dict()
        risk = sum(risk.values())
        risk = round(risk,2)
        sharpe = ret['weighted_sharpe'].to_dict()
        sharpe = sum(sharpe.values())
        sharpe = round(sharpe,2)
        CAGR = ret['weight']*ret['CAGR']
        CAGR = CAGR.to_dict()
        CAGR = sum(CAGR.values())
        CAGR = round(CAGR,2)
        initial = ret['weight']*ret['Initial Balance']
        initial = initial.to_dict()
        initial = sum(initial.values())
        initial = round(initial,2)
        final = ret['weight']*ret['Final Balance']
        final = final.to_dict()
        final = sum(final.values())
        final = round(final,2)
        ret = dict(zip(portfolio_columns,[returns,risk,sharpe,CAGR,initial,final]))

        logging.info(ret)
        return ret
    @classmethod
    def getPortfolioPrice(cls,weights,prices) :
        weights = pd.DataFrame([weights])
        logging.info(weights)
        logging.info(weights.shape)
        logging.info(prices.shape)
        ret = prices.fillna(0)
        ret = weights.dot(prices.T).dropna(how="all").T
        return ret
    @classmethod
    def parseWeights(cls, portfolio) :
        _ret = sorted(portfolio.keys())
        stock_list = filter(lambda x : x not in cls.meta_columns, _ret)
        stock_list = list(stock_list)
        logging.info((len(stock_list),stock_list))
        ret = map(lambda stock : portfolio[stock],stock_list)
        ret = dict(zip(stock_list,ret))
        return ret
    @classmethod
    def smartMassage(cls,data) :
        key_list = sorted(data.columns.values)
        key_list = filter(lambda ref : not ref.startswith('legend_'), key_list)
        key_list = list(key_list)
        logging.info((len(key_list),key_list))
        for ref in key_list :
            data[ref] = FINANCE.graphReturns(data[ref])
        return data

    @classmethod
    def _find(cls) :
        ret = EXTRACT.readPortfolio()
        logging.debug(ret)
        key_list = sorted(ret.keys())
        for curr, key in enumerate(key_list) :
            logging.info((curr+1, key,ret[key]))
            meta_name_list = LOAD.getNames(curr+1, key)
            yield key, ret[key], meta_name_list
    @classmethod
    def find(cls) :
        for name, weights, meta_name_list in cls._find() :
            weights = cls.parseWeights(weights)
            prices = EXTRACT.readPrices(weights.keys())
            prices[meta_name_list['legend']] = cls.getPortfolioPrice(weights,prices)

            logging.info((weights,meta_name_list))

            # Portfolio	Initial Balance	Final Balance	CAGR	Stdev	Best Year	Worst Year	Max. Drawdown	Sharpe Ratio	Sortino Ratio	US Mkt Correlation
            # 'Portfolio','Initial Balance','Final Balance','CAGR','Stdev','Sharpe Ratio','Sortino Ratio','US Mkt Correlation'
            meta = TRANSFORM_STOCK.summarizeSharpe(prices)
            meta['CAGR'] = TRANSFORM_STOCK.summarizeCAGR(prices)
            meta['sector'] = TRANSFORM_STOCK.enrichSector(meta)
            meta['weight'] = TRANSFORM_STOCK.enrichWeight(weights, meta)
            meta['weighted_returns'] = meta['returns']*meta['weight']
            meta['weighted_risk'] = meta['risk']*meta['weight']
            meta['weighted_sharpe'] = meta['sharpe']*meta['weight']
            meta['Final Balance'] = TRANSFORM_STOCK.summarizeBalance(prices)
            meta['Initial Balance'] = 10000
            meta['weighted_Initial_Balance'] = meta['Initial Balance']*meta['weight']
            meta['weighted_Final Balance'] = meta['weighted_Initial_Balance']*meta['Final Balance']
            meta['Final Balance'] = meta['Initial Balance']*meta['Final Balance']
            logging.info(meta.T)
            returns = TRANSFORM_STOCK.summarizeReturns(prices)
            yield weights, meta_name_list, prices, meta, returns

class LOAD() :

    name_format_1 = "portfolio_diversified_{}"
    name_format_2 = "portfolio_returns_{}"
    name_format_3 = 'legend_{}'
    @classmethod
    def config(cls,summary,portfolio) :
        save_file = EXTRACT.instance().output_file
        ret = INI.init()
        INI.write_section(ret,"summary",**summary)
        for key in portfolio.keys() :
            values = portfolio[key]
            if not isinstance(values,dict) :
               values = values.to_dict()
            INI.write_section(ret,key,**values)
        ret.write(open(save_file, 'w'))
        logging.info('saving file {}'.format(save_file))
    @classmethod
    def getNames(cls, curr, name) :
        name_diversified = cls.name_format_1.format(curr)
        name_returns = cls.name_format_2.format(curr)
        name_legend = cls.name_format_3.format(name)
        name_list = ['portfolio', 'diversified', 'returns', 'legend']
        return dict(zip(name_list,[name, name_diversified, name_returns,name_legend]))
    @classmethod
    def getWeights(cls,data) :
        _weights = data['weight'].to_dict()
        _sectors = data['sector'].to_dict()
        _omit_list = filter(lambda key : key.startswith('legend'), _weights.keys())
        for omit in list(_omit_list) :
            _weights.pop(omit,None)
            _sectors.pop(omit,None)
        logging.info(_weights)
        logging.info(_sectors)
        stock_list = sorted(_sectors)
        sector_list = set(_sectors.values())
        sector_list = sorted(list(sector_list))
        ret_weights = {}
        ret_sector = {}
        for sector in sector_list :
            _t = filter(lambda stock : _sectors[stock] == sector, stock_list)
            _t = list(_t)
            sub = map(lambda stock : _weights[stock],_t)
            _t = map(lambda stock : {'weight': round(_weights[stock]*100,2), 'ticker': stock},_t)
            _t = list(_t)
            ret_weights[sector] = _t
            ret_sector[sector] = round(sum(sub),2)
        logging.info(ret_sector)
        logging.info(ret_weights)
        return ret_weights, ret_sector
    @classmethod
    def getSharpes(cls,data) :
        logging.debug(data)
        #_weights = data['weight'].to_dict()
        _data = data.round(2)
        _sectors = _data['sector'].to_dict()
        _returns = _data['weighted_returns'].to_dict()
        _risk = _data['weighted_risk'].to_dict()
        _sharpe = _data['weighted_sharpe'].to_dict()
        _omit_list = filter(lambda key : key.startswith('legend'), _sectors.keys())
        for omit in list(_omit_list) :
            _sectors.pop(omit,None)
            _returns.pop(omit,None)
            _risk.pop(omit,None)
            _sharpe.pop(omit,None)

        logging.debug(_sectors)
        logging.debug(_returns)
        logging.debug(_risk)
        logging.debug(_sharpe)
        stock_list = sorted(_sectors)
        sector_list = set(_sectors.values())
        sector_list = sorted(list(sector_list))
        ret_sector = {}
        ret_stock = {}
        sector_columns = ['weighted returns','weighted risk','weighted sharpe']
        stock_columns = ['returns', 'risk', 'sharpe', 'len', 'weighted_returns', 'weighted_risk', 'weighted_sharpe']
        rename_columns = {"weighted_returns": "weighted returns"
                         , 'weighted_risk' : 'weighted risk'
                         , 'weighted_sharpe' : 'weighted sharpe'}
        for sector in sector_list :
            _t = filter(lambda stock : _sectors[stock] == sector, stock_list)
            _t = list(_t)
            _t_returns = map(lambda stock : _returns[stock],_t)
            _t_returns = round(sum(_t_returns),2)
            _t_risk = map(lambda stock : _risk[stock],_t)
            _t_risk = round(sum(_t_risk),2)
            _t_sharpe = map(lambda stock : _sharpe[stock],_t)
            _t_sharpe = round(sum(_t_sharpe),2)
            ret_sector[sector] = dict(zip(sector_columns,[_t_returns, _t_risk, _t_sharpe]))
            _wtf = _data[stock_columns]
            _wtf = _wtf.T[_t].T
            _wtf = _wtf.rename(columns=rename_columns)
            ret_stock[sector] = _wtf.round(2).T.to_dict()
        logging.info(ret_sector)
        logging.info(ret_stock)
        return ret_stock, ret_sector
class Group :
    def __init__(self) :
       self.graph = []
       self.data = []
       self.name = []
    def __call__(self) :
        return {'graph' : self.graph, 'name' : self.name, 'description' : self.data}
    def add(self, **kwargs) :
        target = 'name'
        name = kwargs.get(target,None)
        target = 'data'
        data = kwargs.get(target,None)
        target = 'graph'
        graph = kwargs.get(target,None)
        if name is not None :
            self.name.append(name)
        if data is not None :
            self.data.append(data)
        if graph is not None :
            self.graph.append(graph)
    @classmethod
    def appendDiversify(cls,name,data,ret) :
        ret_weights, ret_sector = LOAD.getWeights(data)
        ret.add(graph=ret_sector,data=ret_weights,name=name)
        return ret
    @classmethod
    def appendSharpe(cls,name,data,ret) :
        ret_stock, ret_sector = LOAD.getSharpes(data)
        ret.add(graph=ret_sector,data=ret_stock,name=name)
        return ret
    @classmethod
    def appendPrices(cls,name,data,ret) :
        prices = FINANCE.graphReturns(data)
        ret.add(graph=prices,name=name)
        return ret

def process() :
    EXTRACT.prep()

    _returns = Group()
    _enriched = Group()
    _sharpe = Group()
    graph_summary_list = {}
    graph_sharpe_list = {}
    text_CAGR_list = {}
    text_Initial_Balance_list = {}
    text_Final_Balance_list = {}
    text_Stdev_list = {}
    text_SharpeRatio_list = {}
    _portfolio_name_list = []
    for weights, names, prices, summary, returns in TRANSFORM_PORTFOLIO.find() :
        target_From = 'legend_{portfolio}'.format(**names)
        _portfolio_name_list.append(names['portfolio'])

        _enriched = Group.appendDiversify(names['diversified'],summary,_enriched)
        _sharpe = Group.appendSharpe(weights,summary,_sharpe)
        graph_sharpe_list[target_From] = TRANSFORM_PORTFOLIO.getSummary(summary)
        
        graph_summary_list[target_From] = prices[target_From]
        logging.info(prices.head(3))
        _returns = Group.appendPrices(names['returns'],prices, _returns)
        target_To = names['portfolio']
        logging.info(summary.T)
        logging.info(summary.T[target_From])
        text_CAGR_list[target_To] = summary.T[target_From]['CAGR']
        text_Initial_Balance_list[target_To] = 10000
        text_Final_Balance_list[target_To] = summary.T[target_From]['Final Balance']
        text_Stdev_list[target_To] = summary.T[target_From]['risk']
        text_SharpeRatio_list[target_To] = summary.T[target_From]['sharpe']
    graph_summary_list = pd.concat(graph_summary_list.values(),axis=1)

    diversified = _enriched()
    logging.info(graph_sharpe_list)

    bench_list = EXTRACT.readBenchmark()
    initial_Balance = 10000
    SNP = 'SNP500'
    snp = bench_list[SNP]
    gcps = snp[0]
    snp_prices = EXTRACT.readPrices(snp)
    logging.debug(snp_prices)
    snp_prices.rename(columns={gcps:SNP},inplace=True)
    logging.debug(snp_prices)
    snp_sharpe = TRANSFORM_STOCK.summarizeSharpe(snp_prices).T
    snp_CAGR = TRANSFORM_STOCK.summarizeCAGR(snp_prices).T
    target = SNP
    graph_summary_list[target] = snp_prices
    graph_sharpe_list[target] = snp_sharpe[SNP]
    text_CAGR_list[target] = snp_CAGR[SNP][0]
    text_Initial_Balance_list[target] = initial_Balance
    ratio = TRANSFORM_STOCK._summarizeBalance(snp_prices)[0]
    text_Final_Balance_list[target] = round(initial_Balance*ratio,2)
    text_Stdev_list[target] = snp_sharpe.T['risk'][0]
    text_SharpeRatio_list[target] = snp_sharpe.T['sharpe'][0]

    FUNDS = 'NASDAQMUTFUND'
    funds = bench_list[FUNDS]
    funds_prices = EXTRACT.readPrices(funds)
    funds_sharpe = TRANSFORM_STOCK.summarizeSharpe(funds_prices).T
    funds_CAGR = TRANSFORM_STOCK.summarizeCAGR(funds_prices).T
    funds_name_list = sorted(funds_prices)

    for name in funds_name_list :
        graph_summary_list[name] = funds_prices[name]
        graph_sharpe_list[name] = funds_sharpe[name]
        text_CAGR_list[name] = funds_CAGR[name][0]
        text_Initial_Balance_list[name] = initial_Balance
        ratio = TRANSFORM_STOCK._summarizeBalance(funds_prices[name])
        text_Final_Balance_list[name] = round(initial_Balance*ratio,2)
        text_Stdev_list[name] = funds_sharpe[name]['risk']
        text_SharpeRatio_list[name] = funds_sharpe[name]['sharpe']

    '''
    Final Massage
    '''
    logging.debug(graph_summary_list)
    graph_summary_list.fillna(method='backfill',inplace=True)
    graph_summary_list.fillna(1,inplace=True)
    graph_summary_list = graph_summary_list / graph_summary_list.iloc[0]
    graph_summary_list = TRANSFORM_PORTFOLIO.smartMassage(graph_summary_list)

    returns = _returns()
    returns['description_summary'] = _sharpe().get('graph',[])
    returns['description_details'] = _sharpe().get('description',[])

    ref = snp_prices / snp_prices.iloc[0]
    graph_list = returns['graph']
    for i, value in enumerate(graph_list) :
        value['reference_SNP'] = ref
        logging.info(value)

    diversified = _enriched()

    summary_text = [text_CAGR_list,text_Initial_Balance_list, text_Final_Balance_list, text_Stdev_list, text_SharpeRatio_list]
    rows = ['CAGR','Initial Balance','Final Balance','Stdev','Sharpe Ratio']
    rows = dict(zip([0,1,2,3,4],rows))
    summary_text = pd.DataFrame(summary_text)
    summary_text.rename(index=rows,inplace=True)
    summary_text = summary_text.to_dict()

    logging.info(diversified)
    logging.info(graph_summary_list)
    logging.info(summary_text)
    return returns, diversified, graph_summary_list, graph_sharpe_list, summary_text, _portfolio_name_list


@exit_on_exception
@trace
def main() :
   returns, diversified, graph_summary, graph_portfolio_sharpe_list, summary_text, portfolio_name_list = process()
   summary_path_list = []
   POINT.plot(graph_portfolio_sharpe_list,x='risk',y='returns',ylabel="Returns", xlabel="Risk", title="Sharpe Ratio")
   SHARPE = LINE.plot_sharpe(ratio=1)
   SHARPE.plot.line(style='b:', label='sharpe ratio 1',alpha=0.3)
   SHARPE = LINE.plot_sharpe(ratio=2)
   SHARPE.plot.line(style='r:',label='sharpe ratio 2',alpha=0.3)

   local_dir = EXTRACT.instance().local_dir
   path = "{}/images/portfolio_sharpe.png".format(local_dir)
   save(path,loc="lower right")
   summary_path_list.append(path)
   LINE.plot(graph_summary, title="Returns")
   GRAPH.tick_right()
   path = "{}/images/portfolio_summary.png".format(local_dir)
   save(path)
   summary_path_list.append(path)

   graph_list = diversified['graph']
   name_list = diversified['name']
   local_diversify_list = []
   for i, name in enumerate(portfolio_name_list) :
       graph = graph_list[i]
       #title = 'Sector Distribution for {}'.format(name)
       #title = title.replace('_diversified_','')
       #BAR.plot(graph,xlabel='Percentage',title=title)
       BAR.plot(graph,xlabel='Percentage')
       path = "{}/images/{}.png".format(local_dir,name_list[i])
       save(path)
       local_diversify_list.append(path)

   graph_list = returns['graph']
   name_list = returns['name']
   local_returns_list = []
   for i, name in enumerate(portfolio_name_list) :
       graph = graph_list[i]
       #title = 'Returns for {}'.format(name)
       #title = title.replace('_returns_','')
       #LINE.plot(graph, title=title)
       LINE.plot(graph)
       path = "{}/images/{}.png".format(local_dir,name_list[i])
       save(path, ncol=3)
       local_returns_list.append(path)

   summary = { "images" : summary_path_list , "captions" : ["Return over Risk", "portfolio returns"] }
   summary['table'] = summary_text
   portfolio = {}
   for i, value in enumerate(local_diversify_list) :
       images = [ local_diversify_list[i], local_returns_list[i] ]
       captions = [ "portfolio diversity {}", "portfolio returns {}"]
       captions = map(lambda x : x.format(portfolio_name_list[i]), captions)
       captions = map(lambda x : x.replace('_', ' '), captions)
       captions = list(captions)
       section = { "images" : images, "captions" : captions }
       section['description1'] = diversified['description'][i]
       section['description2'] = returns['description_summary'][i]
       section['description3'] = returns['description_details'][i]
       section['name'] = portfolio_name_list[i]
       key = "portfolio_{}".format(i)
       portfolio[key] = section

   LOAD.config(summary,portfolio)

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
   background = filter(lambda x : 'background' in x, ini_list)
   background = list(background)
   benchmark = filter(lambda x : 'benchmark' in x, ini_list)
   repo_stock = env.list_filenames('local/historical_prices/*pkl')
   repo_fund = env.list_filenames('local/historical_prices_fund/*pkl')

   local_dir = "{pwd_parent}/local".format(**vars(env))
   data_store = '{}/images'.format(local_dir)
   data_store = '../local/images'
   output_file = "{pwd_parent}/local/report_generator.ini".format(**vars(env))
   input_file = env.list_filenames('local/method*portfolios.ini')
   if len(input_file) > 0 :
      input_file = input_file[0]

   main()
