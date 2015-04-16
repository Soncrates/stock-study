class ParsingUtils(object) :
  class WordBag(object) :
      import re
      _lowercase = re.compile('[a-z]+')
      _stemmer = None
      _stop_words_alphabet = set(['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 'v'] )
      _stop_words = set(
      ["'ll", "a's", 'able', 'about', 'above', 'abroad', 'abst', 'accordance', 'according', 'accordingly', 'across', 
       'act', 'actually', 'added', 'adj', 'affected', 'affecting', 'affects', 'after', 'afterwards', 'again', 'against', 
       'ago', 'ah', 'ahead', "ain't", 'all', 'allow', 'allows', 'almost', 'alone', 'along', 'alongside', 'already', 'also', 
       'although', 'always', 'am', 'amid', 'amidst', 'among', 'amongst', 'amoungst', 'amount', 'an', 'and', 'announce', 
       'another', 'any', 'anybody', 'anyhow', 'anymore', 'anyone', 'anything', 'anyway', 'anyways', 'anywhere', 'apart', 
       'apparently', 'appear', 'appreciate', 'appropriate', 'approximately', 'are', 'aren', "aren't", 'arent', 'arise', 
       'around', 'as', 'aside', 'ask', 'asking', 'associated', 'at', 'auth', 'available', 'away', 'awfully', 
       'back', 'backward', 'backwards', 'be', 'became', 'because', 'become', 'becomes', 'becoming', 'been', 'before', 
       'beforehand', 'begin', 'beginning', 'beginnings', 'begins', 'behind', 'being', 'believe', 'below', 'beside', 'besides', 
       'best', 'better', 'between', 'beyond', 'bill', 'biol', 'both', 'bottom', 'brief', 'briefly', 'but', 'by', 
       "c'mon", "c's", 'ca', 'call', 'came', 'can', "can't", 'cannot', 'cant', 'caption', 'cause', 'causes', 'certain', 
       'certainly', 'changes', 'clearly', 'co', 'co.', 'com', 'come', 'comes', 'computer', 'con', 'concerning', 'consequently', 
       'consider', 'considering', 'contain', 'containing', 'contains', 'corresponding', 'could', "couldn't", 'couldnt', 'course', 
       'cry', 'currently', 
       'dare', "daren't", 'date', 'de', 'definitely', 'describe', 'described', 'despite', 'detail', 'did', "didn't", 
       'different', 'directly', 'do', 'does', "doesn't", 'doing', "don't", 'done', 'down', 'downwards', 'due', 'during', 
       'each', 'ed', 'edu', 'effect', 'eg', 'eight', 'eighty', 'either', 'eleven', 'else', 'elsewhere', 'empty', 'end', 
       'ending', 'enough', 'entirely', 'especially', 'et', 'et-al', 'etc', 'even', 'ever', 'evermore', 'every', 'everybody', 
       'everyone', 'everything', 'everywhere', 'ex', 'exactly', 'example', 'except', 
       'fairly', 'far', 'farther', 'few', 'fewer', 'ff', 'fifteen', 'fifth', 'fify', 'fill', 'find', 'fire', 'first', 
       'five', 'fix', 'followed', 'following', 'follows', 'for', 'forever', 'former', 'formerly', 'forth', 'forty', 'forward', 
       'found', 'four', 'from', 'front', 'full', 'further', 'furthermore', 
       'gave', 'get', 'gets', 'getting', 'give', 'given', 'gives', 'giving', 'go', 'goes', 'going', 'gone', 'got', 
       'gotten', 'greetings', 
       'had', "hadn't", 'half', 'happens', 'hardly', 'has', "hasn't", 'hasnt', 'have', "haven't", 'having', 'he', "he'd", 
       "he'll", "he's", 'hed', 'hello', 'help', 'hence', 'her', 'here', "here's", 'hereafter', 'hereby', 'herein', 'heres', 
       'hereupon', 'hers', 'herse"', 'herself', 'hes', 'hi', 'hid', 'him', 'himse"', 'himself', 'his', 'hither', 'home', 
       'hopefully', 'how', "how's", 'howbeit', 'however', 'hundred', 
       "i'd", "i'll", "i'm", "i've", 'id', 'ie', 'if', 'ignored', 'im', 'immediate', 'immediately', 'importance', 
       'important', 'in', 'inasmuch', 'inc', 'inc.', 'indeed', 'index', 'indicate', 'indicated', 'indicates', 'information', 
       'inner', 'inside', 'insofar', 'instead', 'interest', 'into', 'invention', 'inward', 'is', "isn't", 'it', "it'd", 
       "it'll", "it's", 'itd', 'its', 'itse"', 'itself', 
       'just', 
       'keep', 'keep', 'keeps', 'kept', 'kg', 'km', 'know', 'known', 'knows', 
       'largely', 'last', 'lately', 'later', 'latter', 'latterly', 'least', 'less', 'lest', 'let', "let's", 'lets', 
       'like', 'liked', 'likely', 'likewise', 'line', 'little', 'look', 'looking', 'looks', 'low', 'lower', 'ltd', 
       'made', 'mainly', 'make', 'makes', 'many', 'may', 'maybe', "mayn't", 'me', 'mean', 'means', 'meantime', 
       'meanwhile', 'merely', 'mg', 'might', "mightn't", 'mill', 'million', 'mine', 'minus', 'miss', 'ml', 'more', 
       'moreover', 'most', 'mostly', 'move', 'mr', 'mrs', 'much', 'mug', 'must', "mustn't", 'my', 'myse"', 'myself', 
       'na', 'name', 'namely', 'nay', 'nd', 'near', 'nearly', 'necessarily', 'necessary', 'need', "needn't", 
       'needs', 'neither', 'never', 'neverf', 'neverless', 'nevertheless', 'new', 'next', 'nine', 'ninety', 'no', 
       'no-one', 'nobody', 'non', 'none', 'nonetheless', 'noone', 'nor', 'normally', 'nos', 'not', 'noted', 'nothing', 
       'notwithstanding', 'novel', 'now', 'nowhere', 
       'obtain', 'obtained', 'obviously', 'of', 'off', 'often', 'oh', 'ok', 'okay', 'old', 'omitted', 'on', 'once', 
       'one', "one's", 'ones', 'only', 'onto', 'opposite', 'or', 'ord', 'other', 'others', 'otherwise', 'ought', "oughtn't", 
       'our', 'ours', 'ourselves', 'out', 'outside', 'over', 'overall', 'owing', 'own', 
       'page', 'pages', 'part', 'particular', 'particularly', 'past', 'per', 'perhaps', 'placed', 'please', 'plus', 
       'poorly', 'possible', 'possibly', 'potentially', 'pp', 'predominantly', 'present', 'presumably', 'previously', 
       'primarily', 'probably', 'promptly', 'proud', 'provided', 'provides', 'put', 
       'que', 'quickly', 'quite', 'qv', 
       'ran', 'rather', 'rd', 're', 'readily', 'really', 'reasonably', 'recent', 'recently', 'ref', 'refs', 'regarding', 
       'regardless', 'regards', 'related', 'relatively', 'research', 'respectively', 'resulted', 'resulting', 'results', 
       'right', 'round', 'run', 
       'said', 'same', 'saw', 'say', 'saying', 'says', 'sec', 'second', 'secondly', 'section', 'see', 'seeing', 
       'seem', 'seemed', 'seeming', 'seems', 'seen', 'self', 'selves', 'sensible', 'sent', 'serious', 'seriously', 'seven', 
       'several', 'shall', "shan't", 'she', "she'd", "she'll", "she's", 'shed', 'shes', 'should', "shouldn't", 'show', 'showed', 
       'shown', 'showns', 'shows', 'side', 'significant', 'significantly', 'similar', 'similarly', 'since', 'sincere', 'six', 
       'sixty', 'slightly', 'so', 'some', 'somebody', 'someday', 'somehow', 'someone', 'somethan', 'something', 'sometime', 
       'sometimes', 'somewhat', 'somewhere', 'soon', 'sorry', 'specifically', 'specified', 'specify', 'specifying', 'still', 
       'st','stop', 'strongly', 'sub', 'substantially', 'successfully', 'such', 'sufficiently', 'suggest', 'sup', 'sure', 'system', 
       "t's", 'take', 'taken', 'taking', 'tell', 'ten', 'tends', 'th', 'than', 'thank', 'thanks', 'thanx', 'that', "that'll", 
       "that's", "that've", 'thats', 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'thence', 'there', "there'd", 
       "there'll", "there're", "there's", "there've", 'thereafter', 'thereby', 'therefore', 'therein', 'theres', 'thereupon', 
       'these', 'they', "they'd", "they'll", "they're", "they've", 'thick', 'thin', 'thing', 'things', 'think', 'third', 'thirty', 
       'this', 'thorough', 'thoroughly', 'those', 'though', 'three', 'through', 'throughout', 'thru', 'thus', 'till', 'to', 
       'together', 'too', 'took', 'top', 'toward', 'towards', 'tried', 'tries', 'truly', 'try', 'trying', 'twelve', 'twenty', 
       'twice', 'two', 
       'un', 'under', 'underneath', 'undoing', 'unfortunately', 'unless', 'unlike', 'unlikely', 'until', 'unto', 'up', 'upon', 
       'upwards', 'us', 'use', 'used', 'useful', 'uses', 'using', 'usually', 
       'value', 'various', 'versus', 'very', 'via', 'viz', 'vs', 
       'want', 'wants', 'was', "wasn't", 'way', 'we', "we'd", "we'll", "we're", "we've", 'welcome', 'well', 'went', 'were', 
       "weren't", 'what', "what'll", "what's", "what've", 'whatever', 'when', "when's", 'whence', 'whenever', 'where', "where's", 
       'whereafter', 'whereas', 'whereby', 'wherein', 'whereupon', 'wherever', 'whether', 'which', 'whichever', 'while', 'whilst', 
       'whither', 'who', "who'd", "who'll", "who's", 'whoever', 'whole', 'whom', 'whomever', 'whose', 'why', "why's", 'will', 
       'willing', 'wish', 'with', 'within', 'without', "won't", 'wonder', 'would', "wouldn't", 
       'yes', 'yet', 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves', 
       'zero'])
      _week_days = set(['mon','monday','tues','tuesday','wed','wednesday','thur','thursday','fri','friday'])
      _months = set(['month','january','jan','february','feb','march','mar',
                     'april','may','june','jun',
                     'july','jul','august','aug','september','sept',
                     'october','oct','november','nov','december','dec'])
      _seasons = set(['winter','spring','summer','autumn'])
      _sports = set(['sports','sport','soccer','golf','skiing','ski','football','basketball','baseball','tennis','rugby',
                     'hockey','volleyball','cricket','boxing','yacht','sailing','cycling'])
      _stop_words |=  _week_days | _months | _seasons
      _stop_words |=  _sports
      
      @staticmethod
      def tokenize(content) :
          import re,nltk
  #        tokens = [word for sent in nltk.sent_tokenize(content) for word in nltk.word_tokenize(sent)]
  #"""
  #LookupError: 
  #**********************************************************************
  #  Resource 'tokenizers/punkt/english.pickle' not found.  Please
  #  use the NLTK Downloader to obtain the resource:  >>>
  #  nltk.download()
  #"""
          if WordBag._stemmer is None :
              from nltk.stem.snowball import SnowballStemmer
              WordBag._stemmer = SnowballStemmer("english")
              WordBag._stop_words |=  set([WordBag._stemmer.stem(t) for t in WordBag._stop_words])
  #            print (sorted(WordBag._stop_words))
          tokens = WordBag._lowercase.findall(content.lower())
  #        print(tokens)
          stem = [WordBag._stemmer.stem(t) for t in tokens]
  #        print(ret)
          return tokens,stem
      @staticmethod
      def parse(content, white_list=None, black_list=None) :
          import pandas as pd
  #        import nltk
          original_tokens, tokens = WordBag.tokenize(content)
          ret = pd.DataFrame(data = {'words':tokens , 'original':original_tokens})
          if white_list is not None and len(white_list) > 0:
              ret['want'] = ret.words.apply(lambda x : x in white_list)
              ret = ret[ret.want == True]
          if black_list is None: 
              ret['want'] = ret.words.apply(lambda x : x not in WordBag._stop_words)
              ret = ret[ret.want == True]
          if len(black_list) > 0 :
              ret['want'] = ret.original.apply(lambda x : x not in black_list)
              ret = ret[ret.want == True]
          #        ret = ret[ret.words not in WordBag._common_words]
          # 'Series' objects are mutable, thus they cannot be hashed
          #        stop_words = sorted(nltk.corpus.stopwords.words('english'))[:25]
          #        ret = ret[ret.words not in stop_words]
          ret = pd.DataFrame(data={'words':ret.words,'original':ret.original})
          ret = ret.drop_duplicates()
          ret['word_length'] = ret.words.apply(lambda x: len(x))
          ret = ret[ret.word_length >= 1]
          ret['word_frequency'] = ret.words.apply(lambda x : tokens.count(x))
          return ret
      @staticmethod
      def execute(feeditems, white_list=None,black_list=None) :
          wordbag = {}
          ret_ = []
          content_ = []
          for item in feeditems :           
              ret = WordBag.parse(item,white_list,black_list)
              keys    = ret['original'].values
              values  = ret['words'].values
              wordbag.update(dict(zip(keys,values)))
              ret_.append(ret)
          return ret_, wordbag
