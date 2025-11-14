import requests
import pandas as pd
from io import StringIO
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

MAP_KEY = os.getenv("MAP_KEY")
SOURCE = os.getenv("SOURCE", "VIIRS_SNPP_NRT")
AREA = os.getenv("AREA", "world")
DAY_RANGE = os.getenv("DAY_RANGE", "1")

url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{MAP_KEY}/{SOURCE}/{AREA}/{DAY_RANGE}"
response = requests.get(url)
if response.status_code == 200:
    data = pd.read_csv(StringIO(response.text))
    print(data.head())
    print(f"Retrieved {len(data)} records.")
else:
    print(f"Error: {response.status_code} - {response.text}")
