from time import time as now, sleep
import inspect
import logging
import sys
import cProfile, pstats
import functools
try:
    from StringIO import StringIO
except ImportError:
    from io import StringIO

from libUtils import TIMER

class REFLECTION(object) :
    @staticmethod
    def reflection1(obj):
        ret = {"__isfunction__" : inspect.isfunction(obj)
             , "__ismethod__" : inspect.ismethod(obj)
             , "__isstaticmethod__" : isinstance(obj,staticmethod)
             , "__isclassmethod__" : isinstance(obj,classmethod)
        }
        ret.update( { key : str(getattr(obj,key,None)) for key in dir(obj) if key.startswith('__is')} )
        name = None
        if hasattr(obj, '__name__'):
           name = obj.__name__
        elif obj.__isstaticmethod__ or obj.__isclassmethod__  :
           name = [obj.__class__, obj.__func__] 
           name = [ x.__name__ for x in name if hasattr(x,"__name__") ]
           name = '.'.join(name)
        ret['__name__'] = name
        logging.debug(ret)
        return ret
    @staticmethod
    def reflection2(obj):
        key_list = dir(obj)
        if hasattr(obj,'__dict__') :
           key_list = obj.__dict__.keys()
        logging.debug(key_list)
        #ret = { key : str(getattr(obj,key)) for key in key_list if '__' not in key and key not in str(getattr(obj,key)) }
        ret = { key : str(getattr(obj,key)) for key in key_list if not key.startswith('__')  }
        return ret

class WRAPPER(object) :
    def __init__(self, f):
        functools.update_wrapper(self, f)
        _name = "WRAPPER"
        if hasattr(self, '__name__') :
            _name = self.__name__
        test = REFLECTION.reflection1(f)
        self.__isfunction__ = test.get('__isfunction__')
        self.__ismethod__ = test.get('__ismethod__')
        self.__isstaticmethod__ = test.get('__isstaticmethod__')
        self.__isclassmethod__ = test.get('__isclassmethod__')
        self.__name__ = test.get("__name__")
        self.f = f
        if self.__isstaticmethod__ and hasattr(f,'__func__') :
           self.f = f.__func__

    def __str__(self):
        ret = REFLECTION.reflection2(self)
        ret = [ "{} : {}".format(key,ret[key]) for key in ret]
        return '\n'.join(ret)

class trace(WRAPPER):
    msg_enter = "Entering {}"
    msg_exit = "Exiting {}"
    msg_time = "Execution speed for {} : {}"
    msg_doc = "Docs for {} : {}"
    def __init__(self, f):
        self.__name__ = 'trace'
        WRAPPER.__init__(self,f)
        self.__doc__ = inspect.getdoc(f)
    def __call__(self, *largs, **kvargs):
        _locals = locals()
        _locals = trace._trim(**_locals)
        _msg_enter, _msg_exit, _msg_time, _msg_doc = trace.init(self.__name__)
        logging.debug(_msg_enter)
        if self.__doc__ is not None :
           _msg = _msg_doc.format(self.__doc__)
           logging.debug( _msg )
           del _msg
        logging.debug( _locals )
        _timer = TIMER.init()
        ret = self.f(*largs, **kvargs)
        _total_time = _timer()
        logging.debug( ret )
        _msg = _msg_time.format(_total_time)
        logging.info(_msg)
        logging.debug(_msg_exit)
        return ret
    @classmethod
    def init(cls, *largs, **kwargs) :
        name = largs[0] 
        msg_enter = cls.msg_enter.format(name)
        msg_exit = cls.msg_exit.format(name)
        msg_time = cls.msg_time.format(name, "{}")
        msg_doc = cls.msg_doc.format(name, "{}")
        return msg_enter, msg_exit, msg_time, msg_doc
    @classmethod
    def _trim(cls, **ret) :
        target = "self"
        ret.pop(target,None)
        return ret

class cpu(WRAPPER):
    sortby = [ 'cumulative','calls']
    #sortby = [ 'calls','cumulative']
    #sortby = [ 'calls','pcalls','file','module','cumulative']

    def __init__(self, f):
        self.__name__ = 'cpu'
        WRAPPER.__init__(self,f)

    def __call__(self, *largs, **kvargs):
        _l = logging.getLogger().getEffectiveLevel()
        if _l != logging.DEBUG :
           return self.f(*largs, **kvargs)
        if self.__isstaticmethod__ :
           return self.f(*largs, **kvargs)
        _t = cProfile.Profile()
        ret = _t.runcall(self.f, *largs, **kvargs)
        _s = StringIO()
        _ps = pstats.Stats(_t, stream=_s).sort_stats(*cpu.sortby)
        _ps.print_stats()
        logging.debug( _s.getvalue())
        return ret

def _debug_helper(value) :
    if isinstance(value,list) and len(value) > 10 :
       value = value[:10]
    return value
     
def debug_object(obj):
    msg = vars(obj)
    msg = { key : _debug_helper(msg[key]) for key in sorted(msg) if not key.startswith('__') }
    for i, key in enumerate(sorted(msg)) :
        logging.info((i,key,msg[key]))
    msg = dir(obj)
    msg = { key : _debug_helper(msg[key]) for key in sorted(msg) if not key.startswith('__') }
    for i,key in enumerate(sorted(msg)):
        logging.info((i,key))

def pprint(msg) :
    for i, key in enumerate(sorted(msg)) :
        value = msg[key]
        if isinstance(value,list) and len(value) > 10 :
           value = value[:10]
        logging.info((i,key, value))

'''
TODO : optional parameters are not working in delay

def delay(func, *largs):
    print(locals())
    wait =5 
    if len(largs) > 0 :
       wait = largs[0]

    @functools.wraps(func)
    def func_wrapper(*args, **kwargs):
        sleep(wait)
        return func(*args, **kwargs)
    return func_wrapper

def dep_delay(func=None, **kvargs ):
    wait = kvargs.get('wait',5)
    print(wait)
    if func is None:
        return functools.partial(sleep, seconds=wait, msg='No function')

    @functools.wraps(func)
    def delay_wrapper(*args, **kwargs):
        sleep(wait)
        return func(*args, **kwargs)
    return delay_wrapper
'''
if __name__ == '__main__' :

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)
   #logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.INFO)

   @cpu
   @trace
   def func1(t):
       """sample docstring for test"""
       print("inside func1()")
       return 1

   @trace
   @cpu
   def func2(**kvargs):
       print("inside func2()")
       return 2

   print(func1(6))
   print(func2(test=4))
   '''
   class B(object) :
       @cpu
       @trace
       @staticmethod
       def func5(**kvargs) :
           print("inside func5()")
           return 5
       @trace
       @cpu
       @staticmethod
       def func6(**kvargs) :
           print("inside func6()")
           return 6
   print(B.func5(itest=6))
   print(B.func6(test=4))
   '''
   class A(object) :
       @cpu
       @trace
       def func3(self,**kvargs) :
           print("inside func3()")
           return 3

       @trace
       @cpu
       def func4(self, **kvargs) :
           print("inside func4()")
           return 4

   a = A()
   print(a.func4(4))
   print(a.func3(itest=6))
