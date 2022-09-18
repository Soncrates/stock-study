#!/bin/bash
python=python3
python="C:\Users\emers\miniconda3\envs\spyder-env\python.exe"
function validate {
  SECTOR=$1
  INPUT_PORTFOLIO=../outputs/portfolio_${SECTOR}.ini
  OUTPUT_REPORT=../outputs/report_${SECTOR}.ini
  INPUT_PDF=../outpus/report_${SECTOR}.ini
  OUTPUT_PDF=../outputs/portfolio_${SECTOR}.pdf
}
function generate {
  $python cmd_Method05.py --suffix $SECTOR --entity stocks --iterations 100
  # Execution speed : minutes : 19.0, seconds : 14.81
  $python cmd_Build_Images.py --input $INPUT_PORTFOLIO --output $OUTPUT_REPORT
  $python cmd_Build_Report.py $INPUT_PDF $OUTPUT_PDF
}


set -eux

validate platinum
generate
#validate gold
#generate
