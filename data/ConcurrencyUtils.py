class ConcurrencyUtils(object) :
  class ConcurrentActor(object) :
      def __init__(self,monad) :
          self.monad=monad
      @staticmethod
      def test_types() :
          '''
          We want type detection, to determine if we should treat elements as atomic or as a collection of objects
          str, bytearray, bytes, and buffers should all be treated as an argument, but based on type how do we distinguish
          between str and list?
          So far the best answer is hasattr(type, 'istitle')
          but this test will help in case the next big change
          '''
          import collections
          #memoryview
          StringTypes = [str, bytearray, bytes]
          StringCount = len(StringTypes)
          StringAttr = map(lambda x : set(dir(x)), StringTypes)
          StringAttr = flatten_collection(StringAttr)
  
          SequenceTypes = [list, tuple, range]
          SetTypes = [ set, frozenset ]
          MappingTypes = [dict]
          CollectionTypes = [collections.Counter, collections.deque, collections.OrderedDict, collections.defaultdict]
          ContainerTypes = SequenceTypes + SetTypes + MappingTypes + CollectionTypes
          ContainerCount = len(ContainerTypes)
          ContainerAttrs = map(lambda x : set(dir(x)), ContainerTypes)
          ContainerAttrs = flatten_collection(ContainerAttrs)
  
          Strings = filter(lambda x : StringAttr.count(x) == StringCount, StringAttr.copy())
          Containers = filter(lambda x : ContainerAttrs.count(x) == ContainerCount, ContainerAttrs.copy())
          Strings = set(Strings)
          Containers = set(Containers)
  
          print (sorted(Strings))
          print (sorted(Containers))
          print (sorted(Containers - Strings))
          print (sorted(Strings - Containers))
          _temp = ContainerTypes + StringTypes
          print(_temp)
          test = filter(lambda x: hasattr(x, 'istitle'),_temp)
          print(list(test))
          
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
                      log_std.append('%r response is %d bytes' % (key[0:80], len(data)))
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
          if not isinstance(args[0], str) :
              if not hasattr(args[0], 'istitle') : ret = self.tuple_arg
          return ret(self.monad,args)
