"""
Weather Data Scraper
Source: data.timesrecordnews.com
Target: Coos County, Oregon - Avg Temp, Min Temp, Max Temp, Precipitation

Usage:
    # Scrape a single month:
    python weather_scraper.py --start 1934-05 --end 1934-05

    # Scrape a range of months:
    python weather_scraper.py --start 1934-05 --end 1935-12

    # Save output to a custom CSV file:
    python weather_scraper.py --start 1934-05 --end 1934-12 --output my_weather_data.csv

Install dependencies:
    pip install requests beautifulsoup4
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
import argparse
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

BASE_URL = "https://data.timesrecordnews.com/weather-data/coos-county-oregon/41011/{date}/table/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

CSV_COLUMNS = ["date", "avg_temp_f", "min_temp_f", "max_temp_f", "precipitation_in"]


def scrape_month(year: int, month: int) -> list[dict]:
    """Scrape weather data for a single month."""
    date_str = f"{year}-{month:02d}-01"
    url = BASE_URL.format(date=date_str)

    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  [ERROR] Failed to fetch {url}: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table")

    if not table:
        print(f"  [WARN] No table found for {year}-{month:02d}")
        return []

    # Parse header row to map column positions
    headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]

    col_map = {}
    for i, h in enumerate(headers):
        if "avg" in h and "temp" in h:
            col_map["avg_temp_f"] = i
        elif "min" in h and "temp" in h:
            col_map["min_temp_f"] = i
        elif "max" in h and "temp" in h:
            col_map["max_temp_f"] = i
        elif "precip" in h:
            col_map["precipitation_in"] = i
        elif "date" in h or h == "day":
            col_map["date"] = i

    if not col_map:
        print(f"  [WARN] Could not identify columns for {year}-{month:02d}. Headers found: {headers}")
        return []

    rows = []
    for tr in table.find_all("tr")[1:]:  # Skip header row
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not cells:
            continue

        def get_cell(key):
            idx = col_map.get(key)
            if idx is not None and idx < len(cells):
                val = cells[idx].replace("—", "").replace("-", "").strip()
                return val if val else None
            return None

        # Build the date string from the day cell
        day_val = get_cell("date")
        try:
            day = int(day_val) if day_val else None
            row_date = f"{year}-{month:02d}-{day:02d}" if day else None
        except (ValueError, TypeError):
            row_date = day_val  # fallback to raw value

        row = {
            "date": row_date,
            "avg_temp_f": get_cell("avg_temp_f"),
            "min_temp_f": get_cell("min_temp_f"),
            "max_temp_f": get_cell("max_temp_f"),
            "precipitation_in": get_cell("precipitation_in"),
        }

        # Skip rows with no useful data
        if any(v is not None for v in list(row.values())[1:]):
            rows.append(row)

    print(f"  [OK] {year}-{month:02d}: {len(rows)} rows scraped")
    return rows


def scrape_range(start: date, end: date, output_file: str, delay: float = 1.0):
    """Scrape weather data across a range of months and save to CSV."""
    all_rows = []
    current = start.replace(day=1)

    while current <= end:
        print(f"Scraping {current.year}-{current.month:02d}...")
        rows = scrape_month(current.year, current.month)
        all_rows.extend(rows)
        current += relativedelta(months=1)
        time.sleep(delay)  # Be polite to the server

    if not all_rows:
        print("No data scraped. Check the URL or date range.")
        return

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nDone! {len(all_rows)} total rows saved to: {output_file}")


def parse_month(s: str) -> date:
    """Parse a YYYY-MM string into a date object."""
    try:
        return datetime.strptime(s, "%Y-%m").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format '{s}'. Use YYYY-MM (e.g. 1934-05)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape weather data from Times Record News")
    parser.add_argument("--start", type=parse_month, default="1934-05",
                        help="Start month in YYYY-MM format (default: 1934-05)")
    parser.add_argument("--end", type=parse_month, default="1934-05",
                        help="End month in YYYY-MM format (default: 1934-05)")
    parser.add_argument("--output", default="weather_data.csv",
                        help="Output CSV filename (default: weather_data.csv)")
    parser.add_argument("--delay", type=float, default=1.0,
                        help="Seconds to wait between requests (default: 1.0)")

    args = parser.parse_args()

    if args.start > args.end:
        parser.error("--start must be before or equal to --end")

    scrape_range(args.start, args.end, args.output, args.delay)