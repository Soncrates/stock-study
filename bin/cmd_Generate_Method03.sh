#!/bin/bash
python=python3
function validate {
  SECTOR=$1
  INPUT_PORTFOLIO=../local/portfolio_${SECTOR}.ini
  OUTPUT_REPORT=../local/report_${SECTOR}.ini
  INPUT_PDF=../local/report_${SECTOR}.ini
  OUTPUT_PDF=../local/portfolio_${SECTOR}.pdf
}
function generate {
  $python cmd_Build_Images.py $INPUT_PORTFOLIO $OUTPUT_REPORT
  $python cmd_Build_Report.py $INPUT_PDF $OUTPUT_PDF
}

function portfolio {
  set -x
  $python cmd_Method03_step01.py
  # Execution speed : minutes : 4.0, seconds : 21.81
  $python cmd_Method03_step02.py
  # Execution speed : minutes : 1.0, seconds : 20.53
  $python cmd_Method03_step03.py
  # Execution speed : minutes : 50.0, seconds : 28.9

}

set -eu
portfolio

validate Basic_Materials
generate 
validate Communication_Services
generate
validate Consumer_Cyclical
generate
validate Consumer_Defensive
generate
validate Energy
generate
validate Financial_Services
generate
validate Healthcare
generate
validate Industrials
generate
validate Real_Estate
generate
validate Technology
generate
validate Utilities
generate
