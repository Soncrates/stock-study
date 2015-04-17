class YQLV(object) :
    RSS_NEWS_URL = 'http://finance.yahoo.com/rss/headline?s='
    YAHOO_URL = 'http://query.yahooapis.com/v1/public/yql'
    DATATABLES_URL = 'store://datatables.org/alltableswithkeys'
    quotes = 'yahoo.finance.quotes'
    options = 'yahoo.finance.options'
    quoteslist = 'yahoo.finance.quoteslist'
    sectors = 'yahoo.finance.sectors'
    industry = 'yahoo.finance.industry'
class YQLBase(object) :
    class Query(object):
        def __call__(self, yql):
            import urllib.parse, json
            queryString = urllib.parse.urlencode({'q': yql, 'format': 'json', 'env': YQLV.DATATABLES_URL})
            url = YQLV.YAHOO_URL + '?' + queryString
            ret = WebUtils.invoke_url(url)
            ret = json.loads(ret)
            return ret
        def __is_valid_response(self, response, field):
            return 'query' in response and 'results' in response['query'] and field in response['query']['results']
        def validate_response(self, response, tagToCheck):
            if self.__is_valid_response(response, tagToCheck):
                ret = response['query']['results'][tagToCheck]
            else:
                if 'error' in response:
                    raise IOError('YQL query failed with error: "%s".' % response['error']['description'])
                else:
                    raise IOError('YQL response malformed.')
            return ret
    class Stock(object) :
        def __init__(self,query,table,tag) :
            self.query = query
            self.table = table
            self.tag = tag
        def __call__(self,symbol, columns='*') :
            #yql = 'select title, link, description, pubDate from rss where url=\'%s%s\'' % (YQLNews.RSS_URL,symbol)
            yql = 'select %s from %s where symbol in (\'%s\')' %(columns, self.table, symbol)
            ret = self.query(yql)
            return self.query.validate_response(ret, self.tag)
class YQL(object) :
    class StockQuote(YQLBase.Stock) :
        def __init__(self) :
            Stock.__init__(self,Query(),YQLV.quotes,'quote')
    class StockOptions(YQLBase.Stock) :
        def __init__(self) :
            Stock.__init__(self,Query(),YQLV.options,'optionsChain')
    class StockQuoteSummary(YQLBase.Stock) :
        def __init__(self) :
            Stock.__init__(self,Query(),YQLV.quoteslist,'quote')
    class StockNameBySector(YQLBase.Stock) :
        def __init__(self) :
            Stock.__init__(self,Query(),YQLV.sectors,'sector')
        def __call__(self) :
            yql = 'select * from %s' % self.table
            ret = self.query(yql)
            return self.query.validate_response(ret, self.tag)
    class StockByIndustry(YQLBase.Stock) :
        def __init__(self) :
            Stock.__init__(self,Query(),YQLV.industry,'industry')
        def __call__(self,id) :
            yql = 'select * from %s where id =\'%s\'' % (self.table, id)
            ret = self.query(yql)
            return self.query.validate_response(ret, self.tag)    
    class NewsFeed(object) :
        def __init__(self,query=YQLBase.Query()) :
            self.query = query
        def __call__(self,symbol) :
            if '^' in symbol : return None
            yql = 'select title,link, pubDate from rss where url=\'%s%s\'' % (YQLV.RSS_NEWS_URL,symbol)
            ret = self.query(yql)
            if ret is None  :return None
            if 'query' in ret : ret = ret['query']
            if ret is None  :return None
            if 'results' in ret : ret = ret['results']
            if ret is None  :return None
            if 'item' in ret : ret = ret['item']
            if ret is None  :return None
#            print (type(ret))
            if isinstance(ret,list) :
                if 'title' in ret[0] and ret[0]['title'].find('not found') > 0:
                    raise IOError('Feed for %s does not exist.' % symbol)
            elif isinstance(ret,dict) :
                if 'error' in ret :
                    raise IOError('Failed to read feed for %s : %s ' % (symbol,ret['error'])) 
                else :
                    print(ret.keys())
            else :
                print(type(ret))
            return self._transform(ret)
        def __transform(self,rss) :
            if rss is None : return rss
            for item in rss :
                del (item['title'])
                url = item['link'].split('*')[1]
                soup = YahooUtils.format_noodle(url)
                if soup is None : soup = YahooUtils.format_yahoo_finance(url)
                if soup is None : soup = YahooUtils.format_biz_yahoo(url)
                if soup is None : soup = YahooUtils.format_investopedia(url)
                if soup is None : soup = YahooUtils.format_generic(url)
                item['link'] = soup
            return rss
