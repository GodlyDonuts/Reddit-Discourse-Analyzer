#!/bin/bash
echo "Installing requirements..."
pip install -r requirements.txt

echo "Starting the program..."
python3 reddit_scraper.py r/MiddleEast --historical