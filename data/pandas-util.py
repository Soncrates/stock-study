def format_nasdaq(ret,unwanted_keys,exchange) :
  import pandas as pd
  ret['Exchange'] = pd.Series(exchange, index=ret.index)
  if unwanted_keys is not None  : 
    for key in unwanted_keys:
      if key in ret.columns.tolist() : ret.drop(key, 1)
  temp = ret.to_dict()
  if unwanted_keys is not None  : 
    for key in unwanted_keys:
      if key in temp.keys() : del temp[key]
  if len(temp.keys()) < len(ret.columns.tolist()) :
    ret = pd.DataFrame.from_dict(temp)
  return ret
def parse_csv(csv) :
    import pandas as pd
    import io
    ret = pd.read_csv(io.BytesIO(csv), encoding='utf8', sep=",",index_col="Symbol")
    return ret
def get_yahoo_historical(stock,year,strict=True) :
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
