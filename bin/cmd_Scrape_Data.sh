#!/bin/bash
set -eu
python=python3
function scrape {
   set -x
   $python cmd_Scrape_BackGround.py
   $python cmd_Scrape_TimeSeries.py
}

scrape
