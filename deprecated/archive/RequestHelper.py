import urllib
from urllib import urlopen
import json
import csv

class UrlHelper :
      @staticmethod
      def parse_request(key,config) :
          key_list = filter(lambda k : key in k, config.keys())
          value_list = map(lambda k : config[k], key_list)
          return dict(zip(key_list,value_list))

      @staticmethod
      def transform_request(config,stock) :
          params = 'param' 
          request = filter(lambda k : params not in k, config.keys())
          params = filter(lambda k : params in k, config.keys())
          if isinstance(request,list) and len(request) > 0 : request=request[0]
          if isinstance(params,list) and len(params) > 0 : params=params[0]
          request = config[request].copy()
          params = config[params].copy()
          key_list = params.keys()
          value_list = params.values()
          value_list = map(lambda k : k.format(stock), value_list)
          params = dict(zip(key_list,value_list))
          q = params.pop('q','')
          q=urllib.quote(q)
          params = urllib.urlencode(params,True)
          if q :
             params = 'q={0}&{1}'.format(q,params)
          url = 'url'
          ret = request.pop(url,url)
          request[url] = ret+params
          return request
      @staticmethod
      def open(**kwargs) :
          results = 'results'
          url = 'url'
          results = kwargs.pop(results,results)
          url = kwargs.pop(url,url)
          
          try:
              response = urlopen(url)
              response = response.read()
              return response
          except Exception, e:
             print e
          return response

class MacroTrend :
      @staticmethod
      def parse(response) :
          ret = response.split("\n")
          ret = filter(lambda k : "Historical" not in k, ret)
          ret = filter(lambda k : "MacroTrend" not in k, ret)
          ret = filter(lambda k : "damages" not in k, ret)
          ret = filter(lambda k : len(k)>0, ret)
          ret = "\n".join(ret)          
          return ret
