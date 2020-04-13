import logging
import os
import unittest

import context
from context import log_file, log_msg

if __name__ == '__main__':

   logging.basicConfig(filename=log_file, filemode='w', format=log_msg, level=logging.INFO)

   #suite = unittest.TestLoader().discover(os.path.dirname(__file__)+'/tests')
   start_dir = os.path.dirname(__file__)
   suite = unittest.TestLoader().discover(start_dir)
   runner = unittest.TextTestRunner()
   runner.run(suite)
