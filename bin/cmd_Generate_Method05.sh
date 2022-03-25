#!/bin/bash
python=python3
python="C:\Users\emers\miniconda3\envs\spyder-env\python.exe"
function validate {
  SECTOR=$1
  INPUT_PORTFOLIO=../local/portfolio_${SECTOR}.ini
  OUTPUT_REPORT=../local/report_${SECTOR}.ini
  INPUT_PDF=../local/report_${SECTOR}.ini
  OUTPUT_PDF=../local/portfolio_${SECTOR}.pdf
}
function generate {
  $python cmd_Method05.py --suffix $SECTOR --entity stock
  # Execution speed : minutes : 19.0, seconds : 14.81
  $python cmd_Build_Images.py $INPUT_PORTFOLIO $OUTPUT_REPORT
  $python cmd_Build_Report.py $INPUT_PDF $OUTPUT_PDF
}


set -eux

#validate Basic_Materials
#generate 
#validate Communication_Services
#generate
#validate Consumer_Cyclical
#generate
#validate Consumer_Defensive
#generate
#validate Energy
#generate
#validate Financial_Services
#generate
#validate Healthcare
#generate
#validate Industrials
#generate
#validate Real_Estate
#generate
#validate Technology
#generate
#validate Utilities
#generate
##validate Open-end_Mutual_Fund
#generate
validate platinum
generate
#validate gold
#generate
