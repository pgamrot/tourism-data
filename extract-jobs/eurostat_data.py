import sdmxthon
import pandas as pd
import sys
import os

DOWNLOAD_DIR = "./tmp"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

data_id = sys.argv[1]
url = f"https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{data_id}/?format=SDMX_2.1_STRUCTURED"
message_data = sdmxthon.read_sdmx(url)

data_content = list(message_data.payload.keys())[0]
dataset = message_data.content[data_content]
df = dataset.data
df.to_csv(f'{DOWNLOAD_DIR}/{data_id}.csv')