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

class WRAPPER(object) :
    def __init__(self, f):
        functools.update_wrapper(self, f)
        _name = "WRAPPER"
        if hasattr(self, '__name__') :
            _name = self.__name__
        #for i, key in enumerate(dir(f)) :
        #    if '__' in key and key in str(getattr(f,key)) : continue
        #    print ("{} {}".format(_name, i), key, getattr(f,key))

        self.__isfunction__ = getattr(f,'__isfunction__',None)
        self.__ismethod__ = getattr(f,'__ismethod__',None)
        self.__isstaticmethod__ = getattr(f,'__isstaticmethod__',None)
        self.__isclassmethod__ = getattr(f,'__isclassmethod__',None)
        if self.__isfunction__ is None : inspect.isfunction(f)
        if self.__ismethod__ is None : inspect.ismethod(f)
        if self.__isstaticmethod__ is None : isinstance(f,staticmethod)
        if self.__isclassmethod__ is None : isinstance(f,classmethod)

        if hasattr(f, '__name__'):
           self.__name__ = f.__name__
        elif self.__isstaticmethod__ or self.__isclassmethod__  :
           self.__name__ = [f.__class__, f.__func__]
           self.__name__ = filter(lambda x : hasattr(x,'__name__'), self.__name__)
           self.__name__ = map(lambda x : x.__name__, self.__name__)
           self.__name__ = '.'.join(self.__name__)

        self.f = f
        if self.__isstaticmethod__ and hasattr(f,'__func__') :
           self.f = f.__func__

    def __str__(self):
        key_list = dir(self)
        if hasattr(self,'__dict__') :
           key_list = self.__dict__.keys()
        key_list = filter(lambda key : '__' not in key or key not in str(getattr(self,key)),key_list)
        value_list = map(lambda key : '{} : {}'.format(key, getattr(self,key)), key_list)
        return '\n'.join(value_list)

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

def debug_object(obj):
    if logging.DEBUG == logging.root.level:
        return
    msg = { key : value for (key,value) in vars(obj).items() if not key.startswith('__') }
    for i, key in enumerate(sorted(msg)) :
        value = msg[key]
        if isinstance(value,list) and len(value) > 10 :
           value = value[:10]
        logging.info((i,key, value))
    msg = [ value for value in dir(obj) if not value.startswith('__') ]
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
