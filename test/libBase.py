# ./test/testCommon.py
''' There are some assumptions made by this unittest
the directory structure
+ ./
|    files -> lib*.py 
+----./local/*
|    |    files -> *.ini
|    |    files -> *.json
|    |    files ->*.csv
+----./log/*
|    |    files -> *.log
+----./test/*
     |    files -> test*.py
     +----./test_input/*
     |        see ../local
     +----./test_output/*
'''
import sys
sys.path.append('../')
import logging as log
import unittest
import libCommon as TEST

class TestFILL_IN_THE_BLANK(unittest.TestCase) :
   def setUp(self) : pass
    def testEnviron(self) :
        log.debug(TEST.load_environ())
    def FindFiles(self) :
        log.debug(TEST.find_files('test*.py'))
        log.debug(TEST.find_files('test_input/*'))
        log.debug(TEST.find_files('test_input/'))
    def testBuildArgs(self) :
        expected = 'test102020'
        results = TEST.build_arg('test',10,2020)
        log.debug(results)
        self.assertTrue( results == expected)

        expected = "test102020{'something': 10}"
        results = TEST.build_arg('test',10,2020,{'something' : 10})
        log.debug(results)
        self.assertTrue( results == expected)
    def testBuidlPath(self) :
        expected = 'test/10/2020'
        results = TEST.build_path('test',10,2020)
        log.debug(results)
        self.assertTrue( results == expected)

        expected = "test/10/2020/{'something': 10}"
        results = TEST.build_path('test',10,2020,{'something' : 10})
        log.debug(results)
        self.assertTrue( results == expected)
    def testBuildCommand(self) :
        expected = 'test 10 2020'
        results = TEST.build_command('test',10,2020)
        log.debug(results)
        self.assertTrue( results == expected)

        expected = "test 10 2020 {'something': 10}"
        results = TEST.build_command('test',10,2020,{'something' : 10})
        log.debug(results)
        self.assertTrue( results == expected)

    def testJson(self) :
        log.debug(TEST.pretty_print(TEST.load_json('test_input/json_test.json')))
    def testConfig(self) :
        log.debug(TEST.pretty_print(TEST.load_config('test_input/conf_test.ini')))

if __name__ == '__main__' :
   log_file = TEST.build_arg(*sys.argv).replace('.py','') + '.log'
   log_file = TEST.build_path('../log',log_file)
   TEST.remove_file(log_file)
    log.basicConfig(filename=log_file, format=TEST.LOG_FORMAT_TEST, level=log.DEBUG)
    unittest.main()
