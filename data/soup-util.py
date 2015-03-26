def format_as_soup(url_response) :
  from bs4 import BeautifulSoup
  return BeautifulSoup(url_response)
def parse_yahoo_1(soup) :
  factor = 1
  thousands = soup.body.findAll(text= "All numbers in thousands")
  if thousands : factor = 1000
  table = soup.find("table", { "class" : "yfnc_tabledata1" })
  prev = ''
  for cell in table.findAll(parse_yahoo3):
      text = cell.find(text=True)
      if not text : continue
      text = text.replace(u'\xa0', u' ')
      text = text.strip()
      if len(text) == 0: continue
      if text == prev : continue
      prev=text
      yield text,factor
def parse_yahoo_2(text) :
  text = " ".join(text.split()).replace(',', "")
  if len(text) == 0 : return ''
  if text[0] == "(":
    text_list = list(text)
    text_list[0] = "-"
    text_list[-1] = ""
    text = "".join(text_list)
  return parse_number(text)
def parse_yahoo3(tag) :
    if tag.name not in ['td','strong'] : return False
    return True
def parse_yahoo(soup) :
  for text,factor in parse_yahoo_1(soup) :
    text = parse_yahoo_2(text)
    if isinstance(text, str) :  yield text
    else : yield str(text*factor)
def parse_html_p_text(url) :
    html = invoke_url(url)
    soup = format_as_soup(html)
    ret =  []
    for p in soup.body.findAll('p') :
        ret.append(p.text)
    return ret
def format_noodle(url) :
    if 'www.noodls.com' not in url : return None
    ret = parse_html_p_text(url)
    return "|".join(ret)
def format_yahoo_finance(url) :
    if 'finance.yahoo.com' not in url : return None
    ret = parse_html_p_text(url)
    return "|".join(ret)
def format_biz_yahoo(url) :
    if 'biz.yahoo.com' not in url : return None
    ret = parse_html_p_text(url)
    return "|".join(ret)
def format_investopedia(url) :
    if 'www.investopedia.com' not in url : return None
    ret = parse_html_p_text(url)
    return "|".join(ret)
def format_generic(url) :
    html = invoke_url(url)
    soup = format_as_soup(html)
    return soup.body.prettify()
