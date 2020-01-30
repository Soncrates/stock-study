#!/bin/bash
python=python3
function scrape {
   set -x
   $python cmd_Scrape_Benchmarks.py
   #Execution speed : , seconds : 18.88
   $python cmd_Scrape_Tickers.py
   #Execution speed : hours : 1.0, minutes : 16.0, seconds : 59.57
   $python cmd_Scrape_Fund.py
   #Execution speed : hours : 6.0, minutes : 16.0, seconds : 59.57
   $python cmd_Scrape_Stock.py
   #Execution speed : minutes : 26.0, seconds : 21.44
   $python cmd_Scrape_Stock_Sector.py
   #Execution speed : minutes : 26.0, seconds : 21.44
}

set -eu
scrape
