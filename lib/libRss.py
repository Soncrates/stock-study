from flask import Flask
import feedparser
import time

app = Flask('app')

class CONSTANTS :
      nasdaq_rss = 'https://www.nasdaq.com/feed/rssoutbound?symbol={}'
      fields = ['title', 'link', 'published','published_parsed','summary','authors','tags','nasdaq_tickers']
      old = 68000000
      @classmethod
      def is_old(cls,entry) :
          ret = time.time() - time.mktime(entry.published_parsed)
          return ret > cls.old
      @classmethod
      def narrow(cls,entry) :
          ret = {}
          for field in cls.fields :
              ret[field] = entry.get(field,'NA')
          return ret

class FACTORY() :
      feed = feedparser.parse("https://www.sebi.gov.in/sebirss.xml")
      feed = CONSTANTS.nasdaq_rss.format('xpo')
      feed = feedparser.parse(feed)
      feed_title = feed['feed']['title']
      feed_entries = feed.entries

      @classmethod
      def entries(cls) :
          for entry in cls.feed_entries:
              if CONSTANTS.is_old(entry) :
                 continue
              ret = CONSTANTS.narrow(entry)
              print (ret['published_parsed'])
              yield ret
               # article_author = entry.author
      @classmethod
      def to_url(cls) :
          yield cls.feed_title, "",""
          for ret in cls.entries() :
              a = "{title}[{link}]".format(**ret)
              b = "Published at {published}" .format(**ret)
              #yield a,b
              yield ret['nasdaq_tickers'], ret['summary'], ret['published']
              #print "Published by {}" .format(article_author)


@app.route('/')
def hello_world():
  msg = []
  for a,b,c in FACTORY.to_url() :
    msg.append(a)
    msg.append(b)
    msg.append(c)
  msg = "<BR>".join(msg)
  return msg

app.run(host='0.0.0.0', port=8080)
