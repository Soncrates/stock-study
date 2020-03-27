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
  $python cmd_Method04_step01.py
  # Execution speed : minutes : 19.0, seconds : 14.81

}

set -eux
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
