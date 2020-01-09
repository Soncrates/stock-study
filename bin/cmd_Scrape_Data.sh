#!/bin/bash
python=python3
function scrape {
   set -x
   $python cmd_Scrape_BackGround.py
   #Execution speed : minutes : 26.0, seconds : 21.44
   $python cmd_Scrape_TimeSeries.py
   #Execution speed : hours : 1.0, minutes : 16.0, seconds : 59.57
}

set -eu
scrape
