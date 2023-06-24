import requests
import os
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep

# Create a directory to store the data if needed
data_path = Path("./data/NAICS")
data_path.mkdir(parents=True, exist_ok=True)

options = webdriver.ChromeOptions()
options.add_argument("--headless")

driver = webdriver.Chrome(options=options)

site = "https://www.census.gov/naics"
driver.get(site)
driver.find_element(By.ID, "naics-downloadables-toggle-btn").click()
sleep(1.5)

# Find download links:
xpath = "//a[contains(@href, '2022') or contains(@href, '2017')][contains(@href, '.xlsx') or contains(@href, '.pdf')]"  # noqa: E501
elements = driver.find_elements(By.XPATH, xpath)
for element in elements:
    url = element.get_attribute("href")
    if url is not None:
        data_filename = url.split("/")[-1].replace("%20", "_")
        filename = data_path / data_filename
        if os.path.exists(filename):
            continue
        else:
            response = requests.get(url)
            if response.status_code == 200:
                with open(filename, "wb") as f:
                    f.write(response.content)
                    print(f'{data_filename} File downloaded successfully')
            else:
                print("File download failed")
