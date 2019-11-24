#!/bin/bash
function generate {
	python cm_Build_Images.py $1 $2
	python cm_Build_Report.py $2 $3
}

python cmd_Method01_step01.py
python cmd_Method01_step02.py
python cmd_Method01_step03.py

generate ../local/method01_step03_sharpe_portfolios.ini ../local/report_Method01.ini ../local/portfolio_Method01.pdf
