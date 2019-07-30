import pandas as pd
def one_month_many_years(key,dataSeries):
    lastday = getLastDay(key[0],key[1])
    days = range(1,lastday+1)
    ret=dict.fromkeys(days,None)
    for x in sorted(dataSeries.index) :
        ret[x.day]=dataSeries[x]
    return ret
def gen_normalize_by_month_1(stock,pct,month):
    pct=pct.ix[pct.index.month==month]
    dataGroup = pd.groupby(pct,by=[pct.index.month,pct.index.year])
    for key in dataGroup.groups :
        _month = dataGroup.get_group(key)
        _month = (_month + 1).cumprod()
        _month['normalized']=(_month[stock]-_month['spx']) + 1
        yield key, _month, _month['normalized']
def gen_normalize_by_month(stock,pct, month) :
    for key,_month, normalized in gen_normalize_by_month_1(stock,pct, month) :
        label = '{},{}-{}'.format(key[1],key[0],stock)
        temp=one_month_many_years(key,normalized)
        yield key[1], _month, label,temp 

class CompareStock(object) :
  @staticmethod
  def init(**kwargs) :
      target = 'baseline'
      baseline_name = kwargs.get(target,'SPY')
      target = 'contender
      contender_name = kwargs.get(target,'AAA')
      service = StockService()
      basline_data = service(baseline_name)
      contender_data = service(contender_name)     
      ret = CompareStock(service,baseline_name, baseline_data,contender_name,contender_data)
      print str(ret)
      return ret
  def __init__(self,baseline_name, baseline_data,contender_name,contender_data)
      self.spy_name = baseline_name
      self.contender_name = contender_name
      self.spy = baseline_data
      self.contender = contender_data
      self.spy2 = baseline_data.reindex(contender_data.index)
  def __normalize(self,both) :
      ret_1 = both.pct_change()
      ret_2 = ret_1[self.contender]-ret_1[self.spy]
      return self.contender,ret_1,ret_2
  def byAdjClose(self) :
      target = 'Adj Close'
      data = {self.spy_name: self.spy[target], self.contender_name: self.contender[target]}
      both = pd.DataFrame(data=data)
      return self.__normalize(both)
  def byVolume(self) :
      target = 'Volume'
      data = {self.spy_name: self.spy[target], self.contender_name: self.contender[target]}
      both = pd.DataFrame(data=data)
      return self.__normalize(both)
