class YQLQuery(object):
    _YAHOO_URL = 'http://query.yahooapis.com/v1/public/yql'
    _DATATABLES_URL = 'store://datatables.org/alltableswithkeys'
    _quotes = 'yahoo.finance.quotes'
    _options = 'yahoo.finance.options'
    _quoteslist = 'yahoo.finance.quoteslist'
    _sectors = 'yahoo.finance.sectors'
    _industry = 'yahoo.finance.industry'
    def __call__(self, yql):
        import urllib.parse, json
        queryString = urllib.parse.urlencode({'q': yql, 'format': 'json', 'env': self._DATATABLES_URL})
        url = self._YAHOO_URL + '?' + queryString
        ret = invoke_url(url)
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
class YQLStock(object) :
    def __init__(self,query,table,tag) :
        self.query = query
        self.table = table
        self.tag = tag
    def __call__(self,symbol, columns='*') :
        yql = 'select %s from %s where symbol in (\'%s\')' %(columns, self.table, symbol)
        print(yql)
        ret = self.query(yql)
        return self.query.validate_response(ret, self.tag)
class YQLStockQuote(YQLStock) :
    def __init__(self) :
        YQLStock.__init__(self,YQLQuery(),YQLQuery._quotes,'quote')
class YQLStockOptions(YQLStock) :
    def __init__(self) :
        YQLStock.__init__(self,YQLQuery(),YQLQuery._options,'optionsChain')
class YQLStockQuoteSummary(YQLStock) :
    def __init__(self) :
        YQLStock.__init__(self,YQLQuery(),YQLQuery._quoteslist,'quote')
class YQLStockNameBySector(YQLStock) :
    def __init__(self) :
        YQLStock.__init__(self,YQLQuery(),YQLQuery._sectors,'sector')
    def __call__(self) :
        yql = 'select * from %s' % self.table
        ret = self.query(yql)
        return self.query.validate_response(ret, self.tag)
class YQLStockByIndustry(YQLStock) :
    def __init__(self) :
        YQLStock.__init__(self,YQLQuery(),YQLQuery._industry,'industry')
    def __call__(self,id) :
        yql = 'select * from %s where id =\'%s\'' % (self.table, id)
        ret = self.query(yql)
        return self.query.validate_response(ret, self.tag)
class YQLNews(object) :
    RSS_URL = 'http://finance.yahoo.com/rss/headline?s='
    def __init__(self,query=YQLQuery()) :
        self.query = query
    def __call__(self,symbol) :
        yql = 'select title, link, description, pubDate from rss where url=\'%s%s\'' % (YQLNews.RSS_URL,symbol)
        ret = self.query(yql)['query']['results']['item']
        if ret[0]['title'].find('not found') > 0:
            raise IOError('Feed for %s does not exist.' % symbol)
        return ret
