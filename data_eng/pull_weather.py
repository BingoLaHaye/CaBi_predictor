# Pull weather data
# Gets hourly temperature and precipitation data for Washington DC from Open-Meteo

# Inputs: Nothing
# Output: weather_hourly.parquet

# reference:
# https://medium.com/@jesusantoniofloresbriones/how-to-fetch-weather-data-from-an-api-in-python-and-automate-it-on-a-linux-server-9494c4e74e5b

# Libraries
import time
import requests
import pandas as pd
from datetime import date
from pathlib import Path

# inputs
lat   =  38.9072           # Washington, DC
long  = -77.0369
start = date(2021, 1, 1)
end   = date(2026, 3, 31)
output_file = "C:/Users/bingo/OneDrive - Georgia Institute of Technology/CSE7643/project/weather_hourly.parquet"
base_url   = "https://archive-api.open-meteo.com/v1/archive"
PAUSE      = 0.5   

# Define Methods
def fetch_year(session: requests.Session, start: date, end: date) -> pd.DataFrame:
    """Fetch one year of hourly data from Open-Meteo."""
    params = {"latitude": lat, 
              "longitude": long,
              "start_date": start.isoformat(),
              "end_date": end.isoformat(),
              "hourly":"temperature_2m,precipitation",
              "temperature_unit": "fahrenheit",
              "precipitation_unit": "inch",
              "timezone": "America/New_York",
              "wind_speed_unit":"mph",
    }

    response = session.get(base_url, params=params, timeout=30)
    response.raise_for_status()
    body = response.json()

    hourly = body["hourly"]
    df = pd.DataFrame({
        "datetime":   pd.to_datetime(hourly["time"]),
        "temp_f":     hourly["temperature_2m"],
        "precip_in":  hourly["precipitation"],
    })
    return df

# Execution

session = requests.Session()

print(f"Period  : {start} → {end}")

all_years  = []
current = start

# Chunk year-by-year
while current <= end:
    year_end = min(date(current.year, 12, 31), end)
    print(f"Fetching {current.year} … ", end="", flush=True)
    df = fetch_year(session, current, year_end)
    all_years.append(df)
    print(f"{len(df):,} hours")

    current = date(current.year + 1, 1, 1)
    time.sleep(PAUSE)

# combine data
combined = pd.concat(all_years, ignore_index=True)
combined = combined.sort_values("datetime").reset_index(drop=True)

# round
combined["temp_f"] = combined["temp_f"].round(1)
combined["precip_in"] = combined["precip_in"].round(3)


# save
print(f"/nWriting '{output_file}'")
combined.to_parquet(output_file, index=False, engine="pyarrow", compression="snappy")

