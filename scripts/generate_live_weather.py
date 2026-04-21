"""
scripts/generate_live_weather.py

Generates LIVE synthetic daily weather records that exactly mirror the
historical Open-Meteo JSON schema already in your raw layer.

What it does
------------
1. BACKFILL  – produces one JSON file per city for each of the past 5 days
               (same format as historical: data_lake/raw/weather/open_meteo/city=<X>/)
2. DAILY RUN – called with --mode daily to generate tomorrow's data
               (intended to be scheduled; tomorrow = today + 1 day)

Output schema (mirrors historical open_meteo JSON):
    latitude, longitude, timezone, daily_units, daily{time, temperature_2m_max,
    temperature_2m_min, temperature_2m_mean, precipitation_sum, rain_sum,
    snowfall_sum, windspeed_10m_max, weathercode}, _meta

Usage
-----
    # Backfill last 5 days (run once)
    python scripts/generate_live_weather.py --mode backfill

    # Generate tomorrow's data (run daily via cron / scheduler)
    python scripts/generate_live_weather.py --mode daily

    # Generate for a specific date (for testing)
    python scripts/generate_live_weather.py --mode daily --date 2026-04-21
"""

import argparse
import json
import math
import random
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

# ── ensure project root on path ────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))

# ── WMO weather codes (same codes used by historical data) ──────────────────
WEATHER_CODES = {
    0:  "Clear sky",
    1:  "Mainly clear",
    2:  "Partly cloudy",
    3:  "Overcast",
    45: "Foggy",
    51: "Light drizzle",
    61: "Slight rain",
    63: "Moderate rain",
    65: "Heavy rain",
    71: "Slight snow",
    80: "Slight showers",
    95: "Thunderstorm",
}

# ── Cities – same 5 as historical data ─────────────────────────────────────
CITIES = [
    {"name": "London",     "lat": 51.5074, "lon": -0.1278,  "country": "GB"},
    {"name": "Manchester", "lat": 53.4808, "lon": -2.2426,  "country": "GB"},
    {"name": "Birmingham", "lat": 52.4862, "lon": -1.8904,  "country": "GB"},
    {"name": "Leeds",      "lat": 53.8008, "lon": -1.5491,  "country": "GB"},
    {"name": "Glasgow",    "lat": 55.8642, "lon": -4.2518,  "country": "GB"},
]

# ── Climate profiles per city (mean temp °C & rain base mm/day) ─────────────
CLIMATE = {
    "London":     {"temp_base": 10, "temp_amp": 7,  "rain_base": 2.0},
    "Manchester": {"temp_base": 9,  "temp_amp": 6,  "rain_base": 2.8},
    "Birmingham": {"temp_base": 9,  "temp_amp": 7,  "rain_base": 2.3},
    "Leeds":      {"temp_base": 8,  "temp_amp": 7,  "rain_base": 2.5},
    "Glasgow":    {"temp_base": 8,  "temp_amp": 5,  "rain_base": 3.5},
}


# ── helpers ─────────────────────────────────────────────────────────────────

def _seasonal_temp(city_name: str, month: int) -> float:
    """Return expected mean temperature for a city in a given month."""
    c = CLIMATE.get(city_name, {"temp_base": 9, "temp_amp": 6, "rain_base": 2.5})
    offset = c["temp_amp"] * math.sin((month - 4) * math.pi / 6)
    return c["temp_base"] + offset


def _generate_day(city_name: str, dt: date) -> dict:
    """Return one day's weather values for a city."""
    mean    = _seasonal_temp(city_name, dt.month) + random.gauss(0, 2)
    amp     = random.uniform(3, 8)
    t_max   = round(mean + amp / 2, 1)
    t_min   = round(mean - amp / 2, 1)
    t_mean  = round(mean, 1)

    rb      = CLIMATE.get(city_name, {}).get("rain_base", 2.5)
    rain_mm = max(0.0, round(random.gauss(rb, rb), 1))
    snow_mm = max(0.0, round(random.gauss(0.3, 0.5), 1)) if mean < 2 else 0.0
    precip  = round(rain_mm + snow_mm, 1)
    wind    = round(random.uniform(5, 40), 1)

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

    return {
        "time":                str(dt),
        "temperature_2m_max":  t_max,
        "temperature_2m_min":  t_min,
        "temperature_2m_mean": t_mean,
        "precipitation_sum":   precip,
        "rain_sum":            rain_mm,
        "snowfall_sum":        snow_mm,
        "windspeed_10m_max":   wind,
        "weathercode":         code,
    }


def build_weather_payload(city: dict, dates: list[date]) -> dict:
    """Build a JSON payload identical to the Open-Meteo historical format."""
    days = [_generate_day(city["name"], d) for d in dates]

    return {
        "latitude":    city["lat"],
        "longitude":   city["lon"],
        "timezone":    "Europe/London",
        "daily_units": {
            "time":                "iso8601",
            "temperature_2m_max":  "°C",
            "temperature_2m_min":  "°C",
            "temperature_2m_mean": "°C",
            "precipitation_sum":   "mm",
            "rain_sum":            "mm",
            "snowfall_sum":        "mm",
            "windspeed_10m_max":   "km/h",
            "weathercode":         "WMO",
        },
        "daily": {
            "time":                [d["time"]                for d in days],
            "temperature_2m_max":  [d["temperature_2m_max"]  for d in days],
            "temperature_2m_min":  [d["temperature_2m_min"]  for d in days],
            "temperature_2m_mean": [d["temperature_2m_mean"] for d in days],
            "precipitation_sum":   [d["precipitation_sum"]   for d in days],
            "rain_sum":            [d["rain_sum"]            for d in days],
            "snowfall_sum":        [d["snowfall_sum"]        for d in days],
            "windspeed_10m_max":   [d["windspeed_10m_max"]   for d in days],
            "weathercode":         [d["weathercode"]         for d in days],
        },
        "_meta": {
            "city":         city["name"],
            "country":      city["country"],
            "lat":          city["lat"],
            "lon":          city["lon"],
            "extracted_at": datetime.utcnow().isoformat(),
            "source":       "open_meteo_live_SYNTHETIC",
            "mode":         "live",
            "record_count": len(dates),
        },
    }


def write_for_city(city: dict, dates: list[date], label: str) -> Path:
    """Write one JSON file per city into data_lake/raw/weather/open_meteo/city=<X>/"""
    payload  = build_weather_payload(city, dates)
    out_dir  = Path("data_lake/raw/weather/open_meteo") / f"city={city['name']}"
    out_dir.mkdir(parents=True, exist_ok=True)

    # filename encodes the date range, same as historical naming convention
    start_str = str(dates[0])
    end_str   = str(dates[-1])
    ts        = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    fname     = f"weather_{city['name'].lower()}_{start_str}_{end_str}_{ts}.json"
    out_path  = out_dir / fname

    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)

    return out_path


# ── modes ────────────────────────────────────────────────────────────────────

def run_backfill():
    """Generate the past 5 days for all cities (run once to seed live layer)."""
    today      = date.today()
    backfill_dates = [today - timedelta(days=i) for i in range(5, 0, -1)]  # 5 days ago → yesterday

    print(f"[BACKFILL] Generating weather for {len(backfill_dates)} days: "
          f"{backfill_dates[0]} → {backfill_dates[-1]}")

    written = []
    for city in CITIES:
        path = write_for_city(city, backfill_dates, label="backfill")
        written.append(path)
        print(f"  ✔  {city['name']:12s} → {path}")

    print(f"\n[BACKFILL] Done. {len(written)} files written.")
    return written


def run_daily(target_date: date = None):
    """Generate data for tomorrow (or a specific date) for all cities."""
    if target_date is None:
        target_date = date.today() + timedelta(days=1)

    print(f"[DAILY] Generating weather for {target_date} (all cities)")

    written = []
    for city in CITIES:
        path = write_for_city(city, [target_date], label="daily")
        written.append(path)
        print(f"  ✔  {city['name']:12s} → {path}")

    print(f"\n[DAILY] Done. {len(written)} files written.")
    return written


# ── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Generate live synthetic weather data (mirrors historical Open-Meteo schema)"
    )
    parser.add_argument(
        "--mode",
        choices=["backfill", "daily"],
        required=True,
        help="backfill = last 5 days  |  daily = tomorrow (or --date)",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        help="Override target date for daily mode (YYYY-MM-DD). Default = tomorrow.",
    )
    args = parser.parse_args()

    if args.mode == "backfill":
        run_backfill()
    elif args.mode == "daily":
        target = date.fromisoformat(args.date) if args.date else None
        run_daily(target)


if __name__ == "__main__":
    main()
