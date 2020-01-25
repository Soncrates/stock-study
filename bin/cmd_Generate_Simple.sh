#!/usr/bin/env bash
python=python3
INPUT_1=../test/testResults/refined_stocklist.ini
OUTPUT_1=../test/testResults/refined_portfolio.ini
OUTPUT_2=../test/testResults/refined_report_portfolio.ini
OUTPUT_3=../test/testResults/refined_portfolio.pdf
set -eux
$python cmd_Simple_Portfolio.py $INPUT_1 $OUTPUT_1
$python cmd_Build_Images.py $OUTPUT_1 $OUTPUT_2
$python cmd_Build_Report.py $OUTPUT_2 $OUTPUT_3
