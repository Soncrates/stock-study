# -*- coding: utf-8 -*-
"""
Created on Tue Mar 15 00:44:11 2022

@author: emers
"""

import subprocess
import sys
import os
import logging as log

import libCommon as COMMON

my_env = os.environ.copy()
my_env["PATH"] = "/usr/sbin:/sbin:" + my_env["PATH"]

log_file = COMMON.build_args(*sys.argv).replace('.py','') + '.log'
log.basicConfig(filename=log_file, format=COMMON.LOG_FORMAT_TEST, level=log.DEBUG)

log.info(my_env)
log.info(my_env["PATH"])
    
try :
    process=subprocess.Popen(['bash','./cmd_Scrape_Data.sh'],stdout=subprocess.PIPE,shell=True, env=my_env)
    out_value=process.communicate()[0]
    repr(out_value)
except Exception as e:
        import traceback
        log.error('Error at %s', 'module', exc_info=e)
