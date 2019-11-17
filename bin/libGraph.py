import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

class CONSTANTS :
      GOLDEN_RATIO = 1.62
      LEGEND = 'legend_'
      RETURNS = '_returns_'
class LINE :
      @classmethod
      def plot(cls, lines,**kwargs) :
          target = 'style'
          style = kwargs.get(target,'o')
          target = 'xlabel'
          xlabel = kwargs.get(target,None)
          target = 'ylabel'
          ylabel = kwargs.get(target,None)
          target = 'title'
          title = kwargs.get(target,None)
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
      def plot(cls, bar, **kwargs) :
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
          width = kwargs.get(target,height*CONSTANTS.GOLDEN_RATIO)
          plt.figure(figsize=(width, height))
          cls._plot(bar)
          if xlabel is not None : plt.xlabel(xlabel)
          if ylabel is not None : plt.ylabel(ylabel)
          if title is not None : plt.title(title)
      @classmethod
      def _plot(cls, bar) :
          label, data, pos = cls._xy(bar)
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
      def plot(cls, points, **kwargs) :
          logging.debug(points)
          logging.info(type(points))
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
          cls._plot(points, x_column_name, y_column_name, style)
          if xlabel is not None : plt.xlabel(xlabel)
          if title is not None : plt.title(title)
          if ylabel is not None : plt.ylabel(ylabel)

      @classmethod
      def _plot(cls, points, column_x, column_y, style) :
          for x, y, label in cls._xy(points,column_x,column_y) :
              #plt.scatter(x,y)
              #ax = point.plot(x='x', y='y', ax=ax, style='bx', label='point')
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

   from glob import glob
   import os,sys
   from libCommon import TIMER

   pwd = os.getcwd()
   local = pwd.replace('bin','local')
   portfolio_ini = glob('{}/yahoo_sharpe_method1*portfolios.ini'.format(local))
   ini_list = glob('{}/*.ini'.format(local))
   file_list = glob('{}/historical_prices/*pkl'.format(local))

   dir = pwd.replace('bin','log')
   name = sys.argv[0].split('.')[0]
   log_filename = '{}/{}.log'.format(dir,name)
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   logging.info("started {}".format(name))
   elapsed = TIMER.init()
   logging.info("finished {} elapsed time : {} ".format(name,elapsed()))
