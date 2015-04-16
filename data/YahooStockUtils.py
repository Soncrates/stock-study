class YahooStockUtils(object) :
  """
  example uses
  service = ConcurrentActor(YahooStockUtils.CacheService())
  print(service('yhoo'))
  print(service(['yhoo','mfst','tivo']))
  """
  @staticmethod
  def get_data(stock,year,strict=True) :
      import pandas.io.data as pdd
      try :
          ret = pdd.DataReader(stock, data_source='yahoo', start='{}/1/1'.format(year))
      except IOError :
          return None
      if not strict : return ret
      first = ret.index.tolist()[0]
      if year != first.year : return None
      if 1 != first.month : return None
      return ret
  class DataService(object) :
    def __init__(self,service=YahooStockUtils.get_data,strict=True) :
      y1,y2,r = TimeUtil.get_year_parameters()
      self.default_year = y1
      self.strict = strict
      self.service = service
    def __call__(self,stock, year=None) :
      if year is None : year = self.default_year
      return self.service(stock,year,self.strict)
  class CacheService(object) :
    from functools import lru_cache
    def __init__(self,service = YahooStockUtils.DataService()) :
      self.fresh = {}
      self.cache = {}
      self.service = service
    @lru_cache(maxsize=100)
    def __call__(self,stock) :
      if stock not in self.cache.keys() or not self.fresh[stock](): 
        self.cache[stock] = self.service(stock)
        self.fresh[stock] = TimeUtil.ExpireTimer(24*60) 
      return self.cache[stock]
