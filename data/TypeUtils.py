class TypeUtils(object) :
    collection_flags = set(['clear','__array_interface__','__dict__'])
    @staticmethod
    def is_single(__object__) :
      if __object__ is None : return True
      if isinstance(__object__,str) : return True
      #buffer, bytearray, bytes
      if hasattr(__object__, 'istitle') : return True
      ret = set(dir(__object__)) & TypeUtils.collection_flags
      return len(ret) == 0 
    @staticmethod
    def test_complex_types() :
        import pandas as pd
        import numpy
        a= set(dir(dict))
        #{'popitem', 'setdefault', 'clear', 'fromkeys'}
        a= set(dir(list))
        #{'__reversed__', 'reverse', 'clear', 'extend', 'remove'}
        #a= set(dir(numpy.ndarray))
        #['__array_finalize__', '__array_interface__', '__array_prepare__', '__array_priority__', '__array_struct__', '__copy__', '__deepcopy__', '__divmod__', '__float__', '__iand__', '__ifloordiv__', '__ilshift__', '__imod__', '__index__', '__int__', '__ior__', '__irshift__', '__ixor__', '__lshift__', '__pos__', '__rdivmod__', '__rlshift__', '__rrshift__', '__rshift__', 'argmax', 'argmin', 'argpartition', 'argsort', 'base', 'byteswap', 'choose', 'compress', 'conj', 'conjugate', 'ctypes', 'data', 'diagonal', 'dtype', 'dump', 'dumps', 'fill', 'flags', 'flat', 'flatten', 'getfield', 'imag', 'item', 'itemset', 'itemsize', 'nbytes', 'newbyteorder', 'nonzero', 'partition', 'ptp', 'put', 'ravel', 'real', 'repeat', 'reshape', 'resize', 'round', 'searchsorted', 'setfield', 'setflags', 'size', 'strides', 'tobytes', 'tofile', 'tolist', 'tostring', 'trace', 'view']
        a= set(dir(str))
        b= set(dir(pd.DataFrame))
        print(sorted(a))
        print("a-b")
        print(sorted(a-b))
        print("b-a")
        print(sorted(b-a))
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
