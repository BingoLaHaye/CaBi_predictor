# Pull weather data
# Gets hourly temperature and precipitation data for Washington DC from Open-Meteo

# Inputs: Nothing
# Output: weather_hourly.parquet

# Libraries
import time
import requests
import pandas as pd
from datetime import date
from pathlib import Path

# inputs
LATITUDE   =  38.9072           # Washington, DC
LONGITUDE  = -77.0369
START_DATE = date(2021, 1, 1)
END_DATE   = date(2025, 1, 1)
OUTPUT_FILE = Path("weather_hourly.parquet")
BASE_URL   = "https://archive-api.open-meteo.com/v1/archive"
PAUSE      = 0.5   

# Define Methods
def fetch_year(session: requests.Session, start: date, end: date) -> pd.DataFrame:
    """Fetch one year of hourly data from Open-Meteo."""
    params = {
        "latitude":           LATITUDE,
        "longitude":          LONGITUDE,
        "start_date":         start.isoformat(),
        "end_date":           end.isoformat(),
        "hourly":             "temperature_2m,precipitation",
        "temperature_unit":   "fahrenheit",
        "precipitation_unit": "inch",
        "timezone":           "America/New_York",
        "wind_speed_unit":    "mph",
    }

    resp = session.get(BASE_URL, params=params, timeout=30)
    resp.raise_for_status()
    body = resp.json()

    hourly = body["hourly"]
    df = pd.DataFrame({
        "datetime":   pd.to_datetime(hourly["time"]),
        "temp_f":     hourly["temperature_2m"],
        "precip_in":  hourly["precipitation"],
    })
    return df

# Execution

session = requests.Session()

print(f"Period  : {START_DATE} → {END_DATE}")

frames  = []
current = START_DATE

# Chunk year-by-year
while current <= END_DATE:
    year_end = min(date(current.year, 12, 31), END_DATE)
    print(f"Fetching {current.year} … ", end="", flush=True)
    try:
        df = fetch_year(session, current, year_end)
        frames.append(df)
        print(f"{len(df):,} hours")
    except requests.HTTPError as e:
        print(f"ERROR: {e}")

    current = date(current.year + 1, 1, 1)
    time.sleep(PAUSE)

if not frames:
    raise SystemExit("No data returned")

# combine data
combined = pd.concat(frames, ignore_index=True)
combined = combined.sort_values("datetime").reset_index(drop=True)

# round
combined["temp_f"] = combined["temp_f"].round(1)
combined["precip_in"] = combined["precip_in"].round(3)


# save
print(f"\nWriting '{OUTPUT_FILE}'")
combined.to_parquet(OUTPUT_FILE, index=False, engine="pyarrow", compression="snappy")

