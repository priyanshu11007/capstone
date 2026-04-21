"""
scripts/generate_sample_data.py

Generates realistic sample weather data for all 5 cities
to verify the raw layer structure works before real API calls.

Run: python scripts/generate_sample_data.py
"""
import json
import random
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ingestion.base import BaseExtractor


# WMO weather codes (simplified)
WEATHER_CODES = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Foggy",
    51: "Light drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    71: "Slight snow",
    80: "Slight showers",
    95: "Thunderstorm",
}

CITIES = [
    {"name": "London",     "lat": 51.5074, "lon": -0.1278,  "country": "GB"},
    {"name": "Manchester", "lat": 53.4808, "lon": -2.2426,  "country": "GB"},
    {"name": "Birmingham", "lat": 52.4862, "lon": -1.8904,  "country": "GB"},
    {"name": "Leeds",      "lat": 53.8008, "lon": -1.5491,  "country": "GB"},
    {"name": "Glasgow",    "lat": 55.8642, "lon": -4.2518,  "country": "GB"},
]

START_DATE = date(2022, 1, 1)
END_DATE   = date(2024, 12, 31)


def _city_climate(city_name: str, month: int) -> dict:
    """Return approximate seasonal climate params per city."""
    base = {
        "London":     {"temp_base": 10, "temp_amp": 7,  "rain_base": 2.0},
        "Manchester": {"temp_base": 9,  "temp_amp": 6,  "rain_base": 2.8},
        "Birmingham": {"temp_base": 9,  "temp_amp": 7,  "rain_base": 2.3},
        "Leeds":      {"temp_base": 8,  "temp_amp": 7,  "rain_base": 2.5},
        "Glasgow":    {"temp_base": 8,  "temp_amp": 5,  "rain_base": 3.5},
    }
    c = base.get(city_name, {"temp_base": 9, "temp_amp": 7, "rain_base": 2.5})
    import math
    seasonal_offset = c["temp_amp"] * math.sin((month - 4) * math.pi / 6)
    return {
        "mean_temp": c["temp_base"] + seasonal_offset,
        "rain_base": c["rain_base"],
    }


def generate_city_weather(city: dict) -> dict:
    dates, t_max, t_min, t_mean, precip, rain, snow, wind, codes = (
        [], [], [], [], [], [], [], [], []
    )

    current = START_DATE
    while current <= END_DATE:
        climate = _city_climate(city["name"], current.month)
        mean = climate["mean_temp"] + random.gauss(0, 2)
        amp  = random.uniform(3, 8)

        dates.append(str(current))
        t_mean.append(round(mean, 1))
        t_max.append(round(mean + amp / 2, 1))
        t_min.append(round(mean - amp / 2, 1))

        rain_mm = max(0, random.gauss(climate["rain_base"], climate["rain_base"]))
        snow_mm = max(0, random.gauss(0.3, 0.5)) if mean < 2 else 0.0
        precip.append(round(rain_mm + snow_mm, 1))
        rain.append(round(rain_mm, 1))
        snow.append(round(snow_mm, 1))
        wind.append(round(random.uniform(5, 40), 1))

        # Pick a weather code probabilistically
        if snow_mm > 1:
            code = 71
        elif rain_mm > 5:
            code = 65
        elif rain_mm > 2:
            code = 63
        elif rain_mm > 0.5:
            code = random.choice([51, 61, 80])
        else:
            code = random.choice([0, 1, 2, 3])
        codes.append(code)

        current += timedelta(days=1)

    return {
        "latitude":  city["lat"],
        "longitude": city["lon"],
        "timezone":  "Europe/London",
        "daily_units": {
            "time":                   "iso8601",
            "temperature_2m_max":     "°C",
            "temperature_2m_min":     "°C",
            "temperature_2m_mean":    "°C",
            "precipitation_sum":      "mm",
            "rain_sum":               "mm",
            "snowfall_sum":           "mm",
            "windspeed_10m_max":      "km/h",
            "weathercode":            "WMO",
        },
        "daily": {
            "time":                   dates,
            "temperature_2m_max":     t_max,
            "temperature_2m_min":     t_min,
            "temperature_2m_mean":    t_mean,
            "precipitation_sum":      precip,
            "rain_sum":               rain,
            "snowfall_sum":           snow,
            "windspeed_10m_max":      wind,
            "weathercode":            codes,
        },
        "_meta": {
            "city":         city["name"],
            "country":      city["country"],
            "lat":          city["lat"],
            "lon":          city["lon"],
            "extracted_at": "SAMPLE_DATA",
            "source":       "open_meteo_archive_SAMPLE",
        },
    }


def main():
    import datetime

    for city in CITIES:
        print(f"Generating sample weather for {city['name']} ...", end=" ")
        data = generate_city_weather(city)

        out_dir = Path("data_lake/raw/weather/open_meteo") / f"city={city['name']}"
        out_dir.mkdir(parents=True, exist_ok=True)

        ts = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        fname = f"weather_{city['name'].lower()}_2022-01-01_2024-12-31_{ts}.json"
        out_path = out_dir / fname

        with open(out_path, "w") as f:
            json.dump(data, f, indent=2)

        days = len(data["daily"]["time"])
        print(f"✔  {days} days → {out_path}")

    print("\nSample data generation complete.")
    print("Raw layer structure:")
    for p in sorted(Path("data_lake/raw").rglob("*.json")):
        print(f"  {p}")


if __name__ == "__main__":
    main()
