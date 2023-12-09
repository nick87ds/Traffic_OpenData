from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options

import time
from random import randint

import re

import json

paline_fermata = {}

####################################################

with open('Linee.txt', 'r+') as f:
    linee = f.readlines()

lista_linee = "".join(linee).split("\n")

####################################################

# Set up Selenium Chrome options
chrome_options = Options()
# chrome_options.add_argument("--headless")  # Run in headless mode (without opening browser window)
chrome_options.add_argument("--incognito")

# Set up Chrome driver with the path to your chromedriver executable
chrome_driver_path = "/usr/lib/chromium-browser/chromedriver"
service = Service(chrome_driver_path)
driver = webdriver.Chrome(service=service, options=chrome_options)

for linea in lista_linee:

    if len(linea) > 0:

        url = f"https://muoversiaroma.it/it/linea?numero={linea}"

        try:
            driver.get(url)
        
            # Wait for 5 seconds for the page to load
            time.sleep(randint(3,7))
            
            # Find all <a> tags with class name "clickable"
            clickable_links = driver.find_elements(By.CSS_SELECTOR, "a.clickable")
            
            # Extract titles from each found link
            for link in clickable_links:
                numero_fermata = link.get_attribute("title")
                numero_fermata = re.findall("[0-9]+", numero_fermata)[0]

                nome_fermata = link.get_attribute("textContent")
                
                # Substitute None or empty text with "No_Name"
                if not nome_fermata:
                    nome_fermata = "No_Name"

                if numero_fermata:
                    print(f"N: {numero_fermata} - Fermata: {nome_fermata}")

                try:
                    paline_fermata[str(numero_fermata)] = nome_fermata
                except:
                    pass
            
            # Print the current URL (for demonstration)
            print(f"Scraped {len(clickable_links)} titles from: {url}")
            
        except Exception as e:
            print(f"Error scraping {url}: {e}")

# Close the browser session
driver.quit()


# Save scraped data
with open('id_paline_fermata.json', 'w') as fp:
    json.dump(paline_fermata, fp)

print(" ------------------ Fine !!! ------------------ ")