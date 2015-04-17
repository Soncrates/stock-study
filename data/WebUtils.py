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
