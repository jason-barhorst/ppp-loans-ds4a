"""
scrape_ppp_foia.py
script to scrape the SBA PPP FOIA site for the latest data
data is available at https://data.sba.gov/dataset/ppp-foia
This will be useful later to create an airflow DAG to automate the process
"""
import os

import requests
from bs4 import BeautifulSoup
from datetime import datetime

site = "https://data.sba.gov/dataset/ppp-foia"
response = requests.get(site)

soup = BeautifulSoup(response.content, "html.parser")

span_tag = soup.find("span", {"class": "automatic-local-datetime"})
data_datetime = span_tag["data-datetime"]

current_date = datetime.strptime(data_datetime, "%Y-%m-%dT%H:%M:%S%z")
print("current_date:", current_date)

# TODO: Check if this date is newer than the previous download
old_date = datetime.strptime("2023-04-05T14:27:21+0000", "%Y-%m-%dT%H:%M:%S%z")
print("old_date:", old_date)

if current_date > old_date:
    print("Newer data available")
else:
    print("You have the current data")
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
    filename = f'./data/PPP-FOIA/{url.split("/")[-1]}'
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
