import logging
import pandas as pd

class RISK :
      @staticmethod
      def shave(data, size) :
          ret = data.sort_values(['risk']).head(size)
          logging.info(ret.sort_values(['risk']).head(5))
          return ret
      @staticmethod
      def trim(data) :
          desc = data.describe()
          risk =  desc['risk']['75%']
          ret = data[data['risk'] <= risk]
          logging.info(ret.sort_values(['risk']).head(5))
          return ret
      @staticmethod
      def cut(data) :
          desc = data.describe()
          risk =  desc['risk']['25%']
          ret = data[data['risk'] <= risk]
          logging.info(ret.sort_values(['risk']).head(5))
          return ret
class SHARPE :
      @staticmethod
      def shave(data, size) :
          ret = data.sort_values(['sharpe']).tail(size)
          logging.info(ret.sort_values(['sharpe']).head(5))
          return ret
      @staticmethod
      def trim(data) :
          desc = data.describe()
          sharpe =  desc['sharpe']['25%']
          ret = data[data['sharpe'] >= sharpe]
          logging.info(ret.sort_values(['sharpe']).tail(5))
          return ret
      @staticmethod
      def cut(data) :
          desc = data.describe()
          sharpe =  desc['sharpe']['75%']
          ret = data[data['sharpe'] >= sharpe]
          logging.info(ret.sort_values(['sharpe']).tail(5))
          return ret
class RETURNS :
      @staticmethod
      def shave(data, size) :
          ret = data.sort_values(['returns']).tail(size)
          logging.info(ret.sort_values(['returns']).head(5))
          return ret
      @staticmethod
      def trim(data) :
          desc = data.describe()
          returns =  desc['returns']['25%']
          ret = data[data['returns'] >= returns]
          logging.info(ret.sort_values(['returns']).tail(5))
          return ret
      @staticmethod
      def cut(data) :
          desc = data.describe()
          returns =  desc['returns']['75%']
          ret = data[data['returns'] >= returns]
          logging.info(ret.sort_values(['returns']).tail(5))
          return ret

