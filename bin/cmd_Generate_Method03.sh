#!/bin/bash
python=python3
function generate {
	python cmd_Build_Images.py $1 $2
	python cmd_Build_Report.py $2 $3
}

function portfolio {
  set -x
  $python cmd_Method03_step01.py
  # Execution speed : minutes : 6.0, seconds : 16.88
  $python cmd_Method03_step02.py
  # Execution speed : seconds : 33.67
  $python cmd_Method03_step03.py
  # Execution speed : minutes : 36.0, seconds : 5.12
}

set -eu
portfolio

generate ../local/portfolio_Basic_Materials.ini ../local/report_Basic_Materials.ini ../local/portfolio_Basic_Materials.pdf
generate ../local/portfolio_Communication_Services.ini ../local/report_Communication_Services.ini ../local/portfolio_Communication_Service.pdf
generate ../local/portfolio_Consumer_Cyclical.ini ../local/report_Consumer_Cyclical.ini ../local/portfolio_Consumer_Cyclical.pdf
generate ../local/portfolio_Consumer_Defensive.ini ../local/report_Consumer_Defensive.ini ../local/portfolio_Consumer_Defensive.pdf
generate ../local/portfolio_Energy.ini ../local/report_Energy.ini ../local/portfolio_Energy.pdf
generate ../local/portfolio_Financial_Services.ini ../local/report_Financial_Services.ini ../local/portfolio_Financial_Services.pdf
generate ../local/portfolio_Healthcare.ini ../local/report_Healthcare.ini ../local/portfolio_Healthcare.pdf
generate ../local/portfolio_Industrials.ini ../local/report_Industrials.ini ../local/portfolio_Industrials.pdf
generate ../local/portfolio_Real_Estate.ini ../local/report_Real_Estate.ini ../local/portfolio_Real_Estate.pdf
generate ../local/portfolio_Technology.ini ../local/report_Technology.ini ../local/portfolio_Technology.pdf
generate ../local/portfolio_Utilities.ini ../local/report_Utilities.ini ../local/portfolio_Utilities.pdf
