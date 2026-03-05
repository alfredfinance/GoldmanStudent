import time
import random
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from datetime import datetime
from selenium import webdriver
from time import sleep
import random
import os
import unicodedata
import re
import httpx
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import json
from selenium.webdriver.chrome.options import Options
import os
import requests
from enum import Enum
from typing import List, Optional, Any
from google.cloud import bigquery
from google.oauth2 import service_account



# Set up Selenium options (headless mode for efficiency)
options = Options()
options.add_argument("--headless=new")  # modern headless mode
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--remote-debugging-port=9222")  # helps Chrome bind properly in CI
options.add_argument("--window-size=1920,1080")

# Initialize WebDriver
driver = webdriver.Chrome(options=options)

base_url = "https://higher.gs.com/campus?&"
max_pages = 5
job_urls = []

wait_time = random.uniform(5, 10)

for page in range(1, max_pages + 1):
    url = f"{base_url}page={page}&sort=POSTED_DATE"
    driver.get(url)  # Open the page
    time.sleep(wait_time)
    print(url)
        
    # Wait for job items to be present (Optional: You can add WebDriverWait if needed)
    job_links = driver.find_elements(By.CSS_SELECTOR, "a.text-decoration-none")
        
    for link in job_links:
        href = link.get_attribute("href")
        job_urls.append(href)

driver.quit()  # Close the browser when done

print(f"Collected {len(job_urls)} job URLs")

#------------------------CHECK DUPLICATES URL DANS BIGQUERY--------------------------------------------------

# Load JSON from GitHub secret
key_json = json.loads(os.environ["BIGQUERY"])

# Create credentials from dict
credentials = service_account.Credentials.from_service_account_info(key_json)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=key_json["project_id"]
)

# Query existing URLs from your BigQuery table
query = """
    SELECT url
    FROM `databasealfred.alfredFinance.goldmanSachs`
    WHERE url IS NOT NULL
"""
query_job = client.query(query)

# Convert results to a set for fast lookup
existing_urls = {row.url for row in query_job}

print(f"Loaded {len(existing_urls)} URLs from BigQuery")

# Filter job_urls
job_urls = [url for url in job_urls if url not in existing_urls]

print(f"✅ Remaining job URLs to scrape: {len(job_urls)}")


#------------------------ FIN CHECK DUPLICATES URL DANS BIGQUERY--------------------------------------------------


# Set up Selenium options (headless mode for efficiency)
options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

# Initialize WebDriver
driver = webdriver.Chrome(options=options)

# Initialize an empty list to store job data
job_data = []


for job_url in job_urls:
    driver.get(job_url)

    def get_text(selector, multiple=False):
        """Helper function to extract text from an element."""
        try:
            if multiple:
                return [elem.text.strip() for elem in driver.find_elements(By.CSS_SELECTOR, selector)]
            return driver.find_element(By.CSS_SELECTOR, selector).text.strip()
        except NoSuchElementException:
            return "" if not multiple else []

    def extract_job_description():
        blocks = driver.find_elements(
            By.CSS_SELECTOR,
            'div[data-testid="job-description-html"] p, '
            'div[data-testid="job-description-html"] li'
        )

        lines = []
        for block in blocks:
            text = block.text.strip()
            if text:
                lines.append(text)

        return "\n\n".join(lines)

    description = extract_job_description()
    division = get_text('div[data-testid="opportunity-field-division"] span.gs-text:last-child')
    location = get_text('div[data-testid="opportunity-field-cities"] span.gs-text:last-child')
    experienceLevel = get_text('div[data-testid="opportunity-field-corporateTitle"] span.gs-text:last-child')
    scrappedDateTime = datetime.datetime.now().isoformat()
    scrappedDate = datetime.datetime.now().strftime("%Y-%m-%d")
    scrappedHour = datetime.datetime.now().strftime("%H")
    scrappedMinutes = datetime.datetime.now().strftime("%M")
    title = driver.find_element(By.XPATH, '(//div[@data-gs-uitk-component="col"]//span[@data-gs-uitk-component="text"])[1]').text.strip()
    
    print(title)


    # Append extracted data to list
    job_data.append({
        "title": title,
        "location": location,
        "scrappedDateTime": scrappedDateTime,
        "description": description,
        "division": division,
        "experienceLevel": experienceLevel,
        "url": job_url,
        "source":"Goldman Sachs",
        "scrappedDate": scrappedDate,
        "scrappedHour": scrappedHour,
        "scrappedMinutes": scrappedMinutes,
        "scrappedDateTimeText": scrappedDateTime
    })

# Convert list to Pandas DataFrame
df_jobs = pd.DataFrame(job_data)

# Convert scraped results into a DataFrame
new_data = df_jobs

#---------UPLOAD TO BIGQUERY-------------------------------------------------------------------------------------------------------------

# Load JSON from GitHub secret
key_json = json.loads(os.environ["BIGQUERY"])

# Create credentials from dict
credentials = service_account.Credentials.from_service_account_info(key_json)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=key_json["project_id"]
)

table_id = "databasealfred.alfredFinance.goldmanSachs"

# CONFIG WITHOUT PYARROW
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

# Convert DataFrame → list of dict rows (JSON compatible)
rows = new_data.to_dict(orient="records")

# Upload
job = client.load_table_from_json(
    rows,
    table_id,
    job_config=job_config
)

job.result()
