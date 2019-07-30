import ConfigParser

class DictIni :
      @staticmethod
      def load_ini(filename) :
          ret=ConfigParser.ConfigParser()
          ret.read(filename)
          return ret

      @staticmethod
      def to_Dict(ini) :
          ret = ini.__dict__
          ret = ret['_sections'].copy()
          ret = dict(ret)
          for k in ret:
              ret[k].pop('__name__', None)
          return ret
    
if __name__ == "__main__" :
   print "hello"