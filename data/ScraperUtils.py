class ScraperUtils (object) :
  class YahooPandaDataService(object) :
    def __init__(self,strict=True) :
      y1,y2,r = TimeUtil.get_year_parameters()
      self.default_year = y1
      self.strict = strict
    def __call__(self,stock, year=None) :
      import pandas.io.data as pdd
      if year is None : year = self.default_year
      try :
          ret = pdd.DataReader(stock, data_source='yahoo', start='{}/1/1'.format(year))
      except IOError :
          return None
      if not self.strict : return ret
      first = ret.index.tolist()[0]
      if year != first.year : return None
      if 1 != first.month : return None
      return ret
  class NasDaqPandaHelpers(object) :
    class monad_service(object) :
        _headers_ = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'Accept-Encoding': 'none',
            'Accept-Language': 'en-US,en;q=0.8',
            'Connection': 'keep-alive',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
          }
        def __call__(self,exchange) : # from nasdaq.com
          url = "http://www.nasdaq.com/screening/companies-by-name.aspx?letter=0&exchange=%s&render=download" % exchange
          return WebUtils.invoke_url(url,self._headers_,raw=True)
    class monad_parse(object) :
        def __call__(self,csv) :
            import pandas as pd
            import io
            ret = pd.read_csv(io.BytesIO(csv), encoding='utf8', sep=",",index_col="Symbol")
            return ret
    class monad_format(object) :
      def __call__(self,ret,black_list,exchange) :
          import pandas as pd
          ret['Exchange'] = pd.Series(exchange, index=ret.index)
          if black_list is not None  : 
            for key in black_list:
              if key in ret.columns.tolist() : ret = ret.drop(key, 1)
          temp = ret.to_dict()
          if black_list is not None  : 
            for key in black_list:
              if key in temp.keys() : del temp[key]
          if len(temp.keys()) < len(ret.columns.tolist()) :
            ret = pd.DataFrame.from_dict(temp)
          return ret  
  class NasDaqPandaDataService(object) :
    _exchanges_=["nyse", "nasdaq"]
    _black_list_=['Summary Quote','MarketCap','LastSale','IPOyear','Unnamed: 8']
    def __init__(self,strict=True, exchanges = None, white_list = None, black_list = None) :
        self.monad_service = NasDaqPandaHelpers.monad_service()
        self.monad_parse   = NasDaqPandaHelpers.monad_parse()
        self.monad_format  = NasDaqPandaHelpers.monad_format()
        if black_list is None : black_list = self._black_list_
        if exchanges is None : exchanges = self._exchanges_
        self.white_list = white_list
        self.black_list = black_list
        self.exchanges = exchanges
    def __call__(self,exchanges=None,black_list=None) :
        if exchanges is None : exchanges = self._exchanges_
        if black_list is None : black_list = self._black_list_
        ret = None
        for exchange in exchanges : 
          if ret is None : ret = self.monad_service(exchange)
          else : ret = b"".join([ret, self.monad_service(exchange)])
        ret = self.monad_parse(ret)
        ret = self.monad_format(ret,black_list,exchange)
        return ret.reindex()
