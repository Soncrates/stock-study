class WebUtils(object) :
    @staticmethod
    def invoke_url(url,headers=None, raw=False) :
      import requests
      if headers is not None :
        ret = requests.get(url, headers=headers)        
      else :
        ret = requests.get(url)
      if not raw : ret = ret.text
      else : ret = ret.content
      return ret
    @staticmethod
    def format_as_soup(url_response) :
      from bs4 import BeautifulSoup
      return BeautifulSoup(url_response)
    @staticmethod
    def parse_number(s):
        ret=""
        try:
            ret = float(s)
        except ValueError:
            return s
        if ret - int(ret) == 0 : return int(ret)
        return ret
    @staticmethod
    def parse_html_p_text(url) :
        html = WebUtils.invoke_url(url)
        soup = WebUtils.format_as_soup(html)
        ret =  []
        for p in soup.body.findAll('p') :
            ret.append(p.text)
        return ret
    @staticmethod
    def format_noodle(url) :
        if 'www.noodls.com' not in url : return None
        ret = WebUtils.parse_html_p_text(url)
        return "|".join(ret)
    @staticmethod
    def format_yahoo_finance(url) :
        if 'finance.yahoo.com' not in url : return None
        ret = WebUtils.parse_html_p_text(url)
        return "|".join(ret)
    @staticmethod
    def format_biz_yahoo(url) :
        if 'biz.yahoo.com' not in url : return None
        ret = WebUtils.parse_html_p_text(url)
        return "|".join(ret)
    @staticmethod
    def format_investopedia(url) :
        if 'www.investopedia.com' not in url : return None
        ret = WebUtils.parse_html_p_text(url)
        return "|".join(ret)
    @staticmethod
    def format_generic(url) :
        html = WebUtils.invoke_url(url)
        soup = WebUtils.format_as_soup(html)
        return soup.body.prettify()

class YahooParse(object) :
    @staticmethod
    def finance_1(soup) :
      factor = 1
      thousands = soup.body.findAll(text= "All numbers in thousands")
      if thousands : factor = 1000
      table = soup.find("table", { "class" : "yfnc_tabledata1" })
      prev = ''
      for cell in table.findAll(YahooParse.validtag):
          text = cell.find(text=True)
          if not text : continue
          text = text.replace(u'\xa0', u' ')
          text = text.strip()
          if len(text) == 0: continue
          if text == prev : continue
          prev=text
          yield text,factor
    @staticmethod
    def finance_2(text) :
      text = " ".join(text.split()).replace(',', "")
      if len(text) == 0 : return ''
      if text[0] == "(":
        text_list = list(text)
        text_list[0] = "-"
        text_list[-1] = ""
        text = "".join(text_list)
      return parse_number(text)
    @staticmethod
    def validtag(tag) :
        if tag.name not in ['td','strong'] : return False
        return True
    @staticmethod
    def finance(soup) :
      for text,factor in YahooParse.finance_1(soup) :
        text = YahooParse.finance_2(text)
        if isinstance(text, str) :  yield text
        else : yield str(text*factor)
    @staticmethod
    def get_stock_daily(stock,year,strict=True) :
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
