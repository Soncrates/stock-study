import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from libDebug import trace

class HELPER :
      GOLDEN_RATIO = 1.62
      LEGEND = 'legend_'
      RETURNS = '_returns_'
class LINE :
      @classmethod
      def validate(cls, **kwargs) :
          target = 'style'
          style = kwargs.get(target,'o')
          target = 'xlabel'
          xlabel = kwargs.get(target,None)
          target = 'ylabel'
          ylabel = kwargs.get(target,None)
          target = 'title'
          title = kwargs.get(target,None)
          logging.info((cls,style, xlabel, ylabel, title))
          return style, xlabel, ylabel, title
      @classmethod
      def plot(cls, lines,**kwargs) :
          style, xlabel, ylabel, title = cls.validate(**kwargs)
          cls._plot(lines)
          if xlabel is not None : plt.xlabel(xlabel)
          if ylabel is not None : plt.ylabel(ylabel)
          if title is not None : plt.title(title)
      @classmethod
      def _plot(cls, lines) :
          for data, label in cls._xy(lines) :
              if 'portfolio' in label :
                 label = label.replace('_returns_','_')
              if 'legend_' in label :
                 label = label.replace('legend_','')
              label = label.replace('_',' ')
              logging.debug(data.head(3))
              logging.debug(data.tail(3))
              logging.info(label)
              data.plot(label=label)
      @classmethod
      def _xy(cls, data) :
          for key in sorted(data.keys()) :
              yield data[key], key
class BAR :
      @classmethod
      def validate(cls, **kwargs) :
          target = 'style'
          style = kwargs.get(target,'o')
          target = 'xlabel'
          xlabel = kwargs.get(target,None)
          target = 'ylabel'
          ylabel = kwargs.get(target,None)
          target = 'title'
          title = kwargs.get(target,None)
          target = 'height'
          height = kwargs.get(target,7)
          target = 'width'
          width = kwargs.get(target,height*HELPER.GOLDEN_RATIO)
          logging.info((cls, style, xlabel, ylabel, title, height, width))
          return style, xlabel, ylabel, title, height, width
      @classmethod
      def plot(cls, bar, **kwargs) :
          style, xlabel, ylabel, title, height, width = cls.validate(**kwargs)
          plt.figure(figsize=(width, height))
          cls._plot(bar)
          if xlabel is not None : plt.xlabel(xlabel)
          if ylabel is not None : plt.ylabel(ylabel)
          if title is not None : plt.title(title)
      @classmethod
      def _plot(cls, bar) :
          label, data, pos = cls._xy(bar)
          logging.info((data,type(data)))
          logging.info((pos,type(pos)))
          plt.barh(pos, data, align='center', alpha=0.5)
          plt.yticks(pos, label)
      @classmethod
      def _xy(cls, data) :
          label_list = []
          data_list = []
          if len(data) == 0 :
             y_pos = np.arange(len(label_list))
             return label_list, data_list, y_pos
          sorted_list = sorted((value, key) for (key,value) in data.items())
          if sorted_list is None :
             y_pos = np.arange(len(label_list))
             return label_list, data_list, y_pos
          sorted_list.reverse() 
          for key, value in sorted_list :
              label_list.append(value)
              data_list.append(key)
          y_pos = np.arange(len(label_list))
          label_list = filter(lambda label : label is not None, label_list)
          label_list = map(lambda label : cls._label(label), label_list)
          if not isinstance(label_list,list) :
             label_list = list(label_list)
          return label_list, data_list, y_pos
      @classmethod
      def _label(cls, ret) :
          from textwrap import wrap
          if '_' not in ret : return ret
          ret =  ret.replace('_',' ')
          ret = '\n'.join(wrap(ret, 15))
          return ret

class POINT :
      @classmethod
      def validate(cls, **kwargs) :
          target = 'x'
          x_column_name = kwargs.get(target,'returns')
          target = 'y'
          y_column_name = kwargs.get(target,'risk')
          target = 'style'
          style = kwargs.get(target,'o')
          target = 'xlabel'
          xlabel = kwargs.get(target,None)
          target = 'ylabel'
          ylabel = kwargs.get(target,None)
          target = 'title'
          title = kwargs.get(target,None)
          target = 'labels'
          label_dict = kwargs.get(target,{})
          logging.info((cls, x_column_name, y_column_name, style, xlabel, ylabel, title, label_dict))
          return x_column_name, y_column_name, style, xlabel, ylabel, title, label_dict
      @classmethod
      def plot(cls, points, **kwargs) :
          logging.debug((points,type(points)))
          x_column_name, y_column_name, style, xlabel, ylabel, title, label_dict = cls.validate(**kwargs)
          cls._plot(points, label_dict, x_column_name, y_column_name, style)
          if xlabel is not None : 
             plt.xlabel(xlabel)
          if title is not None : 
             plt.title(title)
          if ylabel is not None : 
             plt.ylabel(ylabel)
      @classmethod
      def _plot(cls, points, label_dict, column_x, column_y, style) :
          for x, y, label in cls._xy(points,column_x,column_y) :
              #plt.scatter(x,y)
              #ax = point.plot(x='x', y='y', ax=ax, style='bx', label='point')
              label = label_dict.get(label,label)
              if 'portfolio' in label or 'legend_' in label :
                 label = label.replace('_returns_','_')
                 label = label.replace('legend_','')
                 label = label.replace('_',' ')
                 plt.plot(x,y,style, label=label)
                 continue
              plt.plot(x,y,style)
              plt.annotate(label, (x,y))
      @classmethod
      def _xy(cls, data,x,y) :
          for key in sorted(data.keys()) :
              pt = data[key]
              yield pt[x], pt[y], key

def save(path, **kwargs) :
    '''
    loc=None, 
    columnspacing=None, 
    ncol=1, 
    mode=None, 
    shadow=None, 
    title=None, 
    title_fontsize=None, 
    bbox_to_anchor=None, 
    bbox_transform=None
    '''
    plt.legend(**kwargs)
    plt.savefig(path)
    plt.clf()
    plt.cla()
    plt.close()

if __name__ == '__main__' :

   import sys
   import logging
   from libCommon import ENVIRONMENT
   from libFinance import STOCK_TIMESERIES, HELPER as FINANCE
   from libSharpe import HELPER as MONTECARLO

   env = ENVIRONMENT()

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   file_list = env.list_filenames('local/historical_prices/*pkl')
   ini_list = env.list_filenames('local/*.ini')

   portfolio_ini = filter(lambda ini : "portfolio" in ini, ini_list)
   portfolio_ini = filter(lambda ini : "sharpe" in ini, ini_list)
   logging.debug( portfolio_ini )

   def prep(tickers) :
       reader = STOCK_TIMESERIES.init()
       values = map(lambda ticker : reader.extract_from_yahoo(ticker), tickers)
       values = map(lambda data : pd.DataFrame(data)['Adj Close'], values)
       ret = dict(zip(tickers, values))
       ret = pd.DataFrame(ret, columns=tickers)
       return ret

   def demo_returns(data, path) :
       data =  FINANCE.graphReturns(data)
       logging.debug( data.describe() )
       data.rename(columns = {'^GSPC':'SNP500'}, inplace = True)
       LINE.plot(data, title="Returns")
       save(path)

   def demo_sharpe(data, path) :
       pt_list = map(lambda name : "legend_" + name, ticker_list)
       labels = dict(zip(ticker_list,pt_list))
       labels['^GSPC'] = 'SNP500'

       data = map(lambda stock : MONTECARLO.find(data[stock], span=2*FINANCE.YEAR, period=FINANCE.YEAR), data)
       data = dict(zip(ticker_list, data))
       data = pd.DataFrame(data, columns=ticker_list)
       logging.debug(data)
       POINT.plot(data,labels=labels,x='risk',y='returns',ylabel="Returns", xlabel="Risk", title="Sharpe Ratio")
       save(path, loc="lower right")

   ticker_list = ["FB", "AMZN", "AAPL", "NFLX", "GOOG", "BTZ", "^GSPC"]
   ticker_list = sorted(ticker_list)
   data = prep(ticker_list)
   path = "{pwd_parent}/local/example_returns.png".format(**vars(env))
   demo_returns(data, path)
   path = "{pwd_parent}/local/example_sharpes.png".format(**vars(env))
   demo_sharpe(data, path)
