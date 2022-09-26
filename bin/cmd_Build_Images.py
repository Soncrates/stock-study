#!/usr/bin/env python

import logging as log
import matplotlib.pyplot as plt
#plt.style.use('fivethirtyeight')
#plt.style.use('ggplot')
#plt.style.use('seaborn-whitegrid')
fig, ax = plt.subplots()
ax.grid(which='major', linestyle='-', linewidth='0.5', color='gray')

from libBusinessLogicImages import process
from libCommon import INI_WRITE, find_files, LOG_FORMAT_TEST
from libDecorators import exit_on_exception
from libDebug import trace
from libGraph import LINE, BAR, POINT, save, HELPER as GRAPH
'''
   Graph portfolios to determine perfomance, risk, diversification
'''

class LOAD() :

    @classmethod
    def config(cls,output_file, summary,portfolio) :
        ret = {'summary' : summary}        
        for key,values in portfolio.items() :
            if not isinstance(values,dict) :
               values = values.to_dict()
            ret[key] = values
        INI_WRITE.write(output_file, ret)

@exit_on_exception
@trace
def main(**args) :
   for key in sorted(args) :
       log.info((key,args[key]))
   returns, diversified, graph_summary, graph_portfolio_sharpe_list, text_summary, portfolio_name_list = process(args['background'],args['data_repo_stock'],args['benchmark'],args['input_file'],args['category'])
   summary_path_list = []
   log.info(graph_portfolio_sharpe_list)
   POINT.plot(graph_portfolio_sharpe_list,x='RISK',y='RETURNS',ylabel="Returns", xlabel="Risk", title="Sharpe Ratio")
   SHARPE = LINE.plot_sharpe(ratio=1)
   SHARPE.plot.line(style='b:', label='sharpe ratio 1',alpha=0.3)
   SHARPE = LINE.plot_sharpe(ratio=2)
   SHARPE.plot.line(style='r:',label='sharpe ratio 2',alpha=0.3)

   local_dir = args['local_dir']
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
   summary['table'] = text_summary
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

   LOAD.config(args['output_file'],summary,portfolio)

if __name__ == '__main__' :

   import argparse
   from libUtils import ENVIRONMENT

   env = ENVIRONMENT.instance()

   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log.basicConfig(filename=log_filename, filemode='w', format=LOG_FORMAT_TEST, level=log.INFO)
   #log.basicConfig(stream=sys.stdout, format=log_msg, level=log.INFO)

   ini_list = find_files('{pwd_parent}/outputs/*.ini'.format(**vars(env)))
   ini_list += find_files('{pwd_parent}/local/*.ini'.format(**vars(env)))
   category = [ x for x in ini_list if 'stock_by_sector.ini' in x]
   background = [ x for x in ini_list if 'background.ini' in x ]
   benchmark = [ x for x in ini_list if 'benchmark' in x ]
   data_repo_stock = find_files('{pwd_parent}/local/historical_prices/*pkl'.format(**vars(env)))
   data_repo_fund = find_files('{pwd_parent}/local/historical_prices_fund/*pkl'.format(**vars(env)))
   data_repo_prices = find_files('{}/local/historical_*/*pkl'.format(env.pwd_parent))

   local_dir = "{pwd_parent}/outputs".format(**vars(env))
   data_store = '{}/images'.format(local_dir)
   _output = "{pwd_parent}/outputs/report_generator.ini".format(**vars(env))
   _input ='{}/outputs/portfolio*.ini'.format(local_dir)
   log.debug(_input)
   _input = find_files(_input)
   log.debug(_input)

   parser = argparse.ArgumentParser(description='Image Generator')
   parser.add_argument('--input', action='store', dest='input_file', type=str, default=_input, help='portfolios to read in')
   parser.add_argument('--output', action='store', dest='output_file', type=str, default=_output, help='store report meta')

   var_names = [ 'cli', "env", "local_dir", "data_store", "category","input_file",'output_file', 'background','benchmark','data_repo_stock']
   init = { key : value for (key,value) in globals().items() if key in var_names }
   init.update(**vars(parser.parse_args()))
   main(**init)
   #ret = EXTRACT.readSummary()
   #print(ret)
   #print(ret['RISK'].describe())
   #temp = ret.sort_values(['RISK'])
   #print(temp)
   #print(temp['RISK'].std())

