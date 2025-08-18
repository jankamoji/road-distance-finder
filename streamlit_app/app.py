# app.py

<the full Python app code remains unchanged>

# ----------------------
# requirements.txt
# ----------------------
# streamlit
# pandas
# numpy
# requests
# openpyxl
# xlsxwriter
# folium
# streamlit-folium

# ----------------------
# README.md (one-page)
# ----------------------
# Road Distance Finder - Browser App

## What it does
Uploads three Excel files (Sites, Airports, Seaports). The app preselects the Top-N nearest airports/ports by Haversine and then calls OpenRouteService (driving-car) to find the true nearest by road (distance and time). Optionally computes road distance/time to an editable reference location (default: Bedburg, Germany).

## Inputs
- Sites.xlsx → sheet "Sites": Site Name, Latitude, Longitude
- Airports.xlsx → sheet "Airports": Airport Name, IATA (optional), Latitude, Longitude
- Seaports.xlsx → sheet "Seaports": Seaport Name, UNLOCODE (optional), Latitude, Longitude
- OpenRouteService API key (kept in session only)

## Outputs
- Interactive table with: Site Name, Latitude, Longitude, Nearest Airport, Distance to Airport (km), Time to Airport (min), Nearest Seaport, Distance to Seaport (km), Time to Seaport (min), Distance/Time to Reference (if enabled).
- Downloads: CSV and XLSX. Decimal dot; km/min units.

## How to deploy (no local setup)
- **Streamlit Community Cloud**: create a public repo with `app.py` and `requirements.txt` → "New app" → select repo → deploy.
- **Hugging Face Spaces**: new Space → SDK = Streamlit → upload the same two files (plus this README) → Deploy.

## Settings (sidebar)
- Top-N candidates (default 3)
- Pause after X API calls (default 35) and Pause seconds (default 60) for rate-limit compliance
- Reference toggle and coordinates (default: Bedburg 51.0126, 6.5741)
- Clear cache

## Performance and quotas
- Calls ORS only for Top-N candidates per site (+ reference if enabled).
- Visible progress bar and API call counter; configurable pauses.
- Session-level memoization avoids repeated origin-destination calls within a run.

## Validation and errors
- Strict header checks; latitude/longitude range checks.
- Per-site processing log with graceful error capture.
- If routing fails for a candidate, it is skipped; if all fail, the field shows ERROR.

## Privacy
- API key is stored only in session memory and never written to disk or exports.
- Uploads are processed in memory; downloads contain only computed results.

## Notes
- ORS expects coordinates as (lon, lat). The app handles conversion internally.
- Distances returned by ORS are already in km; durations are converted to minutes.
- Large lists (thousands of airports/ports) are supported; Top-N prefilter keeps API usage low.
