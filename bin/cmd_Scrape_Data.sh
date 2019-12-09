#!/bin/bash
function scrape {
   ./cmd_Scrape_BackGround.py
   ./cmd_Scrape_TimeSeries.py
}

scrape
