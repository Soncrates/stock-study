#!/usr/bin/env bash
python=python3
set -eux
INPUT_1=../test/testConfigs/raw_401K.ini 
OUTPUT_1=../test/testResults/raw_401K.ini
OUTPUT_2=../test/testResults/raw_report_401K.ini
OUTPUT_3=../test/testResults/raw_401K.pdf

$python cmd_Simple_Portfolio.py $INPUT_1 $OUTPUT_1
$python cmd_Build_Images.py $OUTPUT_1 $OUTPUT_2
$python cmd_Build_Report.py $OUTPUT_2 $OUTPUT_3

INPUT_1=../test/testConfigs/step01_401K.ini 
OUTPUT_1=../test/testResults/step01_401K.ini
OUTPUT_2=../test/testResults/step01_report_401K.ini
OUTPUT_3=../test/testResults/step01_401K.pdf

$python cmd_Simple_Portfolio.py $INPUT_1 $OUTPUT_1
$python cmd_Build_Images.py $OUTPUT_1 $OUTPUT_2
$python cmd_Build_Report.py $OUTPUT_2 $OUTPUT_3

INPUT_1=../test/testConfigs/step02_401K.ini 
OUTPUT_1=../test/testResults/step02_401K.ini
OUTPUT_2=../test/testResults/step02_report_401K.ini
OUTPUT_3=../test/testResults/step02_401K.pdf

$python cmd_Simple_Portfolio.py $INPUT_1 $OUTPUT_1
$python cmd_Build_Images.py $OUTPUT_1 $OUTPUT_2
$python cmd_Build_Report.py $OUTPUT_2 $OUTPUT_3
