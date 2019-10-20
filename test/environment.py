#!/usr/bin/python

def main() :
    try :
        return _main()
    except Exception as e :
        logging.error(e, exc_info=True)

def _main() :
    env = ENVIRONMENT()
    print(env)
    print(env.list_filenames(extension='local/*.ini'))

if __name__ == '__main__' :

   import sys
   import logging
   sys.path.append(sys.path[0].replace('test','bin'))
   from libCommon import ENVIRONMENT

   env = ENVIRONMENT()
   log_filename = '{pwd_parent}/log/{name}.log'.format(**vars(env))
   log_msg = '%(module)s.%(funcName)s(%(lineno)s) %(levelname)s - %(message)s'
   logging.basicConfig(filename=log_filename, filemode='w', format=log_msg, level=logging.INFO)

   logging.info("started {}".format(env.name))
   from libCommon import TIMER
   elapsed = TIMER.init()
   main()
   logging.info("finished {} elapsed time : {}".format(name,elapsed()))

