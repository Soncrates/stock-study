from collections import Counter
import os
import re
import logging
import pandas as pd

from libUtils import combinations

class WORDS() :
    stop_words = set(
      ["'ll", "a", "a's", 'able', 'about', 'above', 'abroad', 'abst', 'accordance', 'according', 'accordingly', 'across',
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
    stop_words |=  _week_days | _months | _seasons
    stop_words |=  _sports
    digit = re.compile("^[0-9]+$")
    w = re.compile("[^\w]")
    plurals = re.compile("s$")
    @classmethod
    def word_extraction(cls, sentence):
        ret = cls.w.sub(" ",  sentence).split()
        ret = [w.lower() for w in ret if w.lower() not in cls.stop_words]
        return ret
    @classmethod
    def word_transform(cls, sentence):
        plurals = [w for w in sentence if cls.plurals.search(w)]
        plurals = map(lambda x : x[0:-1], plurals)
        plurals = list(plurals)
        ret = [w for w in sentence if not cls.plurals.search(w)]
        ret.extend(plurals)
        ret = [w for w in ret if not cls.digit.search(w)]
        ret = [w for w in ret if len(w) > 1]
        return ret
    @classmethod
    def bag_of_words(cls, token_list, _sentences):
        ret = Counter()
        for token in token_list :
            word_list = [w for w in _sentences if token in w]
            ret[token] = len(word_list)
        ret = pd.DataFrame.from_dict(ret, orient='index').reset_index()
        ret.rename(columns={0:'count','index':'word'},inplace=True)
        return ret
    @classmethod
    def limit_bag_of_words(cls, ret):
        target = 'count'
        ret = ret[ret[target] > 1]
        while len(ret) > 20 :
           ret = ret[ret[target] < ret[target].max()]
           if len(ret) < 20 :
              return ret
           ret = ret[ret[target] > ret[target].min()]
           if len(ret) < 20 :
              return ret
        return ret
    @classmethod
    def locate_word(cls, token, sentence):
        if token not in sentence :
           return -1
        word_list = cls.word_extraction(sentence)
        if token in word_list :
           return word_list.index(token)
        word_list = word_list[::-1]
        ret = len(word_list)-1
        while token not in word_list[ret] and ret > 0:
              ret -= 1
        return ret
        
    @classmethod
    def word_distance(cls, token_list, sentence_list):
        token_pairs = list(combinations(token_list,size=2))
        ret = {}
        for sentence in sentence_list :
            for a,b in token_pairs :
                dist_a = cls.locate_word(a,sentence)
                if dist_a < 0 :
                   continue
                dist_b = cls.locate_word(b,sentence)
                if dist_b < 0 :
                   continue
                if dist_a > dist_b :
                   dist = dist_a - dist_b
                else :
                   dist = dist_b - dist_a
                #logging.info((a,b,dist ))
                if a not in ret :
                   ret[a] = {}
                if b not in ret[a] :
                   ret[a][b] = []
                if b not in ret :
                   ret[b] = {}
                if a not in ret[b] :
                   ret[b][a] = []
                ret[a][b].append(dist)
                ret[b][a].append(dist)
        return ret 
    @classmethod
    def tokenize(cls, sentence_list) :
        word_list = []
        for sentence in sentence_list :
            _word_list = cls.word_extraction(sentence)
            word_list.extend(_word_list)
        token_list = cls.word_transform(word_list) 
        token_list = sorted(list(set(token_list)))
        
        bow = cls.bag_of_words(token_list,word_list)
        bow = cls.limit_bag_of_words(bow)

        distance = cls.word_distance(token_list, sentence_list)
        logging.info(distance)
        return token_list, bow

