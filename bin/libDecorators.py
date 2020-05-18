import logging
import sys
import functools

def exit_on_exception(func):
    """function decorator to log on exception and exit"""
    @functools.wraps(func)
    def exit_program(*args, **kwargs):
        try:
           return func(*args, **kwargs)
        except Exception as e :
           logging.error(e, exc_info=True)
           sys.exit(e)
    exit_program.__name__ = func.__name__
    return exit_program

def log_on_exception(func):
    """function decorator to log on exception and return None"""
    @functools.wraps(func)
    def exception_guard(*args, **kwargs):
        ret = None
        try:
           ret = func(*args, **kwargs)
        except Exception as e :
           logging.error(e, exc_info=True)
        finally :
           return ret
    exception_guard.__name__ = func.__name__
    return exception_guard

def singleton_get(obj, target=None):
    """Find an object in global"""
    """WARNING you can not log from here!!!!!!!!!!!!!"""
    if target is None :
       target = "__init__"
    target = getattr(obj,target,None)
    ret = None
    if not target :
       return target, ret
    target = str(target)
    return target, globals().get(target,None)

def singleton(cls):
    """Class decorator to call __init__ only once"""
    """WARNING you can not log from here!!!!!!!!!!!!!"""

    @functools.wraps(cls)
    def wrap_single(*args, **kwargs):
        target, ret = singleton_get(cls)
        if not (ret is None) :
           return ret
        ret = cls(*args, **kwargs)
        globals()[target] = ret
        return ret
    wrap_single.__name__ = cls.__name__
    return wrap_single

def cache(func):
    """function decorator tp keep a cache of previous function call returns"""
    @functools.wraps(func)
    def wrapper_cache(*args, **kwargs):
        cache_key = args + tuple(kwargs.items())
        if cache_key not in wrapper_cache.cache:
            wrapper_cache.cache[cache_key] = func(*args, **kwargs)
        return wrapper_cache.cache[cache_key]
    wrapper_cache.__name__ = func.__name__
    wrapper_cache.cache = dict()
    return wrapper_cache

def http_200(func):
    '''function Decorator to check status code from REST call '''
    @functools.wraps(func)
    def wrap_200(*args, **kwargs):
        ret = func(*args, **kwargs)
        ret.raise_for_status()
        return ret
    wrap_200.__name__ = func.__name__
    return wrap_200

'''
import functools

@functools.lru_cache(maxsize=4)
def fibonacci(num):
    if num < 2:
        return num
    return fibonacci(num - 1) + fibonacci(num - 2)

@lru_cache(maxsize=None)
def fib(n):
    if n < 2:
        return n
    return fib(n-1) + fib(n-2)

>>> [fib(n) for n in range(16)]
[0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610]

>>> fib.cache_info()
CacheInfo(hits=28, misses=16, maxsize=None, currsize=16)
'''

'''
unused
def accepts(*types):
    def check_accepts(f):
        assert len(types) == f.__code__.co_argcount
        def new_f(*args, **kwds):
            for (a, t) in zip(args, types):
                assert isinstance(a, t), \
                       "arg %r does not match %s" % (a,t)
            return f(*args, **kwds)
        new_f.__name__ = f.__name__
        return new_f
    return check_accepts
'''
'''

def debug(func):
    """Print the function signature and return value"""
    @functools.wraps(func)
    def wrapper_debug(*args, **kwargs):
        args_repr = [repr(a) for a in args]                      # 1
        kwargs_repr = [f"{k}={v!r}" for k, v in kwargs.items()]  # 2
        signature = ", ".join(args_repr + kwargs_repr)           # 3
        logging.debug(f"Calling {func.__name__}({signature})")
        value = func(*args, **kwargs)
        logging.debug(f"{func.__name__!r} returned {value!r}")           # 4
        return value
    wrapper_debug.__name__ = func.__name__
    return wrapper_debug

import math
from decorators import debug

# Apply a decorator to a standard library function
math.factorial = debug(math.factorial)
'''

'''
'''

'''
from dataclasses import dataclass, field
from typing import List

@dataclass
class PlayingCard:
    rank: str
    suit: str

RANKS = '2 3 4 5 6 7 8 9 10 J Q K A'.split()
SUITS = '♣ ♢ ♡ ♠'.split()

def make_french_deck():
    return [PlayingCard(r, s) for s in SUITS for r in RANKS]

@dataclass
class Deck:
    cards: List[PlayingCard]
    #cards: List[PlayingCard] = field(default_factory=make_french_deck)

    def __repr__(self):
        cards = ', '.join(f'{c!s}' for c in self.cards)
        return f'{self.__class__.__name__}({cards})'

'''
'''
def repeat(_func=None, *, num_times=2):
    def decorator_repeat(func):
        @functools.wraps(func)
        def wrapper_repeat(*args, **kwargs):
            for _ in range(num_times):
                value = func(*args, **kwargs)
            return value
        return wrapper_repeat

    if _func is None:
        return decorator_repeat
    else:
        return decorator_repeat(_func)

'''
'''
import functools
import time

def slow_down(_func=None, *, rate=1):
    """Sleep given amount of seconds before calling the function"""
    def decorator_slow_down(func):
        @functools.wraps(func)
        def wrapper_slow_down(*args, **kwargs):
            time.sleep(rate)
            return func(*args, **kwargs)
        return wrapper_slow_down

    if _func is None:
        return decorator_slow_down
    else:
        return decorator_slow_down(_func)

@slow_down(rate=2)
def countdown(from_number):
    if from_number < 1:
        print("Liftoff!")
    else:
        print(from_number)
        countdown(from_number - 1)

'''

'''
class Cell(object):
    def __init__(self):
        self._alive = False
    @property
    def alive(self):
        return self._alive
    def set_state(self, state):
        self._alive = bool(state)
    set_alive = partialmethod(set_state, True)
    set_dead = partialmethod(set_state, False)
...
>>> c = Cell()
>>> c.alive
False
>>> c.set_alive()
>>> c.alive
True

'''
if __name__ == "__main__" :

   import sys
   import logging

   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(stream=sys.stdout, format=log_msg, level=logging.DEBUG)

   @singleton
   class EXAMPLE_01() :
         def __init__(self, *args, **kwargs):
             print(self.__init__)
             print(type(self.__init__))

   @singleton
   class EXAMPLE_02() :
         def __init__(self, *args, **kwargs):
             print(self.__init__)
             print(type(self.__init__))
   test = EXAMPLE_02()
   test = EXAMPLE_01()
   test = EXAMPLE_01()
   test = EXAMPLE_02()
   test = singleton_get(EXAMPLE_02)
   print (test)
