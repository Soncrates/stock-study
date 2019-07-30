class ConcurrencyUtils(object) :
  class ConcurrencyService(object) :
      def __init__(self,monad) :
          self.monad=monad          
      def single_arg(self,monad,args, max_workers=5) :
          import concurrent.futures
          ret = {}
          log_std = []
          log_err = []
          # We can use a with statement to ensure threads are cleaned up promptly
          with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
              futures = {executor.submit(monad, arg): arg for arg in args}
              for fidx in concurrent.futures.as_completed(futures):
                  key = futures[fidx]
                  try:
                      data = fidx.result()
                      ret[key] = data
                  except Exception as exc:
                      log_err.append('%r generated an exception: %s' % (key[0:80], exc))
                  else:
                      response = 0
                      if data is not None :
                        response = len(data)
                      log_std.append('%r response is %d bytes' % (key[0:80], response))
          print({'std':log_std,'err':log_err})
          return ret
      def tuple_arg(self,monad,args) :
          raise Error('tuple_arg is not implemented! %s' % type(args))
      def __call__(self, args) :
          if args is None : return self.monad()
          if isinstance(args,str) : return self.monad(args)
          if not hasattr(args, '__iter__') : raise TypeError('args has no iterator %s' % type(args))
          if not hasattr(args, '__getitem__') : args = list(args)
          ret = self.single_arg
          if len(args) > 0 :
            if not TypeUtils.is_single(args[0]) : ret = self.tuple_arg
          return ret(self.monad,args)
  class CacheService(object) :
    def __init__(self,service = print, config = None) :
      self._cache_time = 24*60
      self._throttle_time = 10
      if config is not None : 
          self._cache_time = config['duration_cache']
          self._throttle_time = config['duration_throttle']
      self._fresh = {}
      self._cache = {}
      self._throttled = set([])
      self._skip = {}
      self.service = ConcurrencyService(service)
    def clear(self) :
        self._cache.clear()
        self._fresh.clear()
        self._throttled.clear()
        self._skip.clear()
    def __getitem__(self, key):
      return self._cache[key]
    def __setitem__(self, key, item):
        if item is None : 
            self._skip[key] = TimeUtil.ExpireTimer(self._throttle_time) 
            self._throttled.add(key)
        else :
            self._fresh[key] = TimeUtil.ExpireTimer(self._cache_time) 
            self._cache[key] = item
        return item
    def __contains__(self,key = None) :
      ret = True
      if key is not None and not hasattr(key,'__hash__') : 
        ret = False
      elif key is not None and not TypeUtils.is_single(key):
        ret = False
      else : 
        ret = key in self._cache.keys() and self._fresh[key]()
#      print(list(self.cache.keys()))
      return ret
    def is_throttled(self,key) :
      ret = True
      if key is not None and not hasattr(key,'__hash__') : 
        ret = False
      elif key is not None and not TypeUtils.is_single(key):
        ret = False
      else : 
        ret = key in self._throttled and self._skip[key]()
      return ret
    def __call__(self,key = None) :
      ret = None
      if TypeUtils.is_single(key) : 
        if not self.__contains__(key) and not self.is_throttled(key): 
            ret = self.service(key)
            self.__setitem__(key,ret)
        if self.__contains__(key) : 
            ret = self.__getitem__(key)
      else :
        cached = filter(lambda k : self.__contains__(k) or self.is_throttled(k), key)
        not_cached = set(key) - set(cached)
        temp = self.service(not_cached)
        if temp is not None and not TypeUtils.is_single(temp):
          for k,v in temp.items() :
            self.__setitem__(k,v)
        ret = {}
        for k in key :
          if not self.__contains__(k) : continue
          v = self.__getitem__(k)
          if v is None : continue
          ret[k] = v
      return ret
