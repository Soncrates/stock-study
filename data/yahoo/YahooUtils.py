class WebUtils(object) :
  class Scrape(object) :
    from functools import lru_cache
    @lru_cache(maxsize=100)
    def __call__(self,url,headers=None, raw=False) :
      #print(url)
      return WebUtils.invoke_url(url,headers,raw)
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
  def format_as_soup(url_response, raw=False) :
    from bs4 import BeautifulSoup
    ret = BeautifulSoup(url_response)
    if not raw : 
      for script in ret(["script", "style"]):
            script.extract() # Remove these two elements from the BS4 object
    return ret
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
  def cleanText(text) :
      unwanted = [u'\n',u'\t',u'\xa0',u'\xe2',u'\x80',u'\x93',u'\x99']
      for dd in unwanted :
        text = text.replace(dd, u' ')
      lines = list(map(lambda line : line.strip(), text.splitlines()))
      chunks = (phrase.strip() for line in lines for phrase in line.split("  ")) # break multi-headlines into a line each
      def chunk_space(chunk):
        chunk_out = chunk + ' ' # Need to fix spacing issue
        return chunk_out  
      ret = ''.join(chunk_space(chunk) for chunk in chunks if chunk).encode('utf-8') # Get rid of all blank lines and ends of line
      ret = ret.decode('unicode_escape')
      ret = ret.encode('ascii', 'ignore')
      return ret      
  @staticmethod
  def parse_text(url) :
      import re
      html = WebUtils.invoke_url(url)
      soup = WebUtils.format_as_soup(html,raw=False)
      ret = soup.get_text() # Get the text from this
      ret = WebUtils.cleanText(ret).decode('utf-8')
      ret = " ".join(re.findall('\w+',ret.lower()))
      return ret
  @staticmethod
  def parse_html_p_text(url) :
      html = WebUtils.invoke_url(url)
      soup = WebUtils.format_as_soup(html)
      ret =  []
      for p in soup.body.findAll('p') :
          text = p.text
          text = text.replace(u'\xa0', u' ')
          text = text.strip()
          ret.append(text)
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
        
