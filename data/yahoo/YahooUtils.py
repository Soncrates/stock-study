class YahooUtils(object) :
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
class YahooFinance(object) : 
    @staticmethod
    def get_cash_flow(ticker) :
      return WebUtils.invoke_url('http://finance.yahoo.com/q/cf?s=%s&annual' % ticker)
    @staticmethod
    def get_income_statement(ticker) :
      return WebUtils.invoke_url('http://finance.yahoo.com/q/is?s=%s&annual' % ticker)
    @staticmethod
    def get_balance_sheet(ticker) :
      return WebUtils.invoke_url('http://finance.yahoo.com/q/bs?s=%s&annual' % ticker)
    @staticmethod
    def get_analyst_estimates(ticker) :
      return WebUtils.invoke_url('http://finance.yahoo.com/q/ae?s=%s+Analyst+Estimates' % ticker)
    @staticmethod
    def get_rss_stock(ticker) :
      return WebUtils.invoke_url('http://finance.yahoo.com/rss/headline?s=%s' % ticker)
    @staticmethod
    def get_rss_industry(ticker) :
      return WebUtils.invoke_url('http://finance.yahoo.com/rss/industry?s=%s' % ticker)
class YahooParse(object) :
    @staticmethod
    def finance_1(soup) :
      factor = 1
      thousands = soup.body.findAll(text= "All numbers in thousands")
      if thousands : factor = 1000
      table = soup.find("table", { "class" : "yfnc_tabledata1" })
      if table is None : return
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
      return WebUtils.parse_number(text)
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
