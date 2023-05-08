import requests
import os

from selenium import webdriver
from selenium.webdriver.common.by import By
from time import sleep

options = webdriver.ChromeOptions()
options.add_argument("--headless")

driver = webdriver.Chrome(options=options)

site = "https://www.census.gov/naics"
driver.get(site)
driver.find_element(By.ID, "naics-downloadables-toggle-btn").click()
sleep(1.5)

# Find download links:
elements = driver.find_elements(By.XPATH, "//a[contains(@href, '2022') or contains(@href, '2017')][contains(@href, '.xlsx') or contains(@href, '.pdf')]")
for element in elements:
    url = element.get_attribute("href")
    filename = f'./data/NAICS/{url.split("/")[-1]}'.replace("%20", "_")
    if os.path.exists(filename):
        continue
    else:
        response = requests.get(url)
        if response.status_code == 200:
            with open(filename, "wb") as f:
                f.write(response.content)
                print(f'{url.split("/")[-1]} File downloaded successfully')
        else:
            print("File download failed")
