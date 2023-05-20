"""
scrape_ppp_foia.py
script to scrape the SBA PPP FOIA site for the latest data
data is available at https://data.sba.gov/dataset/ppp-foia
"""
import os
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from datetime import datetime

date_format = "%Y-%m-%dT%H:%M:%S%z"

# Create a directory to store the data if needed
data_path = Path("./data/ppp_foia")
data_path.mkdir(parents=True, exist_ok=True)
data_date_file = data_path / 'DATA_DATE.txt'

# Get data date for data already downloaded if it exists
if os.path.exists(data_date_file):
    with open(data_date_file, "r") as f:
        stored_dd = datetime.strptime(f.readline(), date_format)
else:
    stored_dd = None
print(f'stored data date: {stored_dd}')

# Get the current date date from the website
site = "https://data.sba.gov/dataset/ppp-foia"
response = requests.get(site)

soup = BeautifulSoup(response.content, "html.parser")

span_tag = soup.find("span", {"class": "automatic-local-datetime"})
data_datetime = span_tag["data-datetime"]

current_dd = datetime.strptime(data_datetime, date_format)
print("current data date:", current_dd)

new_data_available = stored_dd is None or current_dd > stored_dd
if new_data_available:
    print("Newer data available, downloading...")
else:
    print("Current data already downloaded.")
    exit()

# Iterate over all listed data sources
ul_tag = soup.find("ul", {"class": "resource-list"})

# Find all links within the unordered list
a_tags = ul_tag.find_all("a")

# Iterate over the links and print their href attribute values
data_sources = []
for a in a_tags:
    href = a.get("href")
    if len(href) > 8 and href[:5] == "https":
        data_sources.append(href)

print("Number of data sources:", len(data_sources))

# download the file
for url in data_sources:
    filename = f'{data_path}/{url.split("/")[-1]}'
    if os.path.exists(filename):
        print(f'{url.split("/")[-1]} File already exists')
        continue
    else:
        response = requests.get(url)
        if response.status_code == 200:
            with open(filename, "wb") as f:
                f.write(response.content)
                print(f'{url.split("/")[-1]} File downloaded successfully')
        else:
            print("File download failed")

# Write the current date to a file
with open(data_date_file, "w") as f:
    f.write(current_dd.strftime(date_format))
