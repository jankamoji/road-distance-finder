import io
import time
import math
import json
import traceback
from typing import Tuple, Dict, Any, List

import numpy as np
import pandas as pd
import requests
import streamlit as st
from streamlit.runtime.uploaded_file_manager import UploadedFile

try:
    from streamlit_folium import st_folium
    import folium
    _HAS_MAP = True
except Exception:
    _HAS_MAP = False

APP_TITLE = "Road Distance Finder"
DEFAULT_REF = {"name": "Bedburg, Germany", "lat": 51.0126, "lon": 6.5741}
REQUIRED_SITES_COLS = ["Site Name", "Latitude", "Longitude"]
REQUIRED_AIRPORTS_COLS = ["Airport Name", "Latitude", "Longitude"]
REQUIRED_SEAPORTS_COLS = ["Seaport Name", "Latitude", "Longitude"]

# ---------------------- Utilities ----------------------

def haversine_km(lat1, lon1, lat2, lon2):
    """Vectorized haversine distance in km between arrays lat1/lon1 and lat2/lon2."""
    R = 6371.0088
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2.0)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda/2.0)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    return R * c

@st.cache_data(show_spinner=False)
def template_files() -> Dict[str, bytes]:
    """Generate Excel templates (as bytes) with example rows."""
    out = {}
    # Sites.xlsx
    df_sites = pd.DataFrame([
        {"Site Name": "Example Plant A", "Latitude": 52.2297, "Longitude": 21.0122}, # Warsaw
        {"Site Name": "Example Plant B", "Latitude": 48.1486, "Longitude": 17.1077}, # Bratislava
        {"Site Name": "Example Plant C", "Latitude": 50.1109, "Longitude": 8.6821},  # Frankfurt
    ])
    b = io.BytesIO()
    with pd.ExcelWriter(b, engine="xlsxwriter") as xw:
        df_sites.to_excel(xw, sheet_name="Sites", index=False)
    out["Sites.xlsx"] = b.getvalue()

    # Airports.xlsx
    df_airports = pd.DataFrame([
        {"Airport Name": "Frankfurt Airport", "IATA": "FRA", "Latitude": 50.0379, "Longitude": 8.5622},
        {"Airport Name": "Warsaw Chopin Airport", "IATA": "WAW", "Latitude": 52.1657, "Longitude": 20.9671},
        {"Airport Name": "Vienna International Airport", "IATA": "VIE", "Latitude": 48.1103, "Longitude": 16.5697},
        {"Airport Name": "Prague Václav Havel", "IATA": "PRG", "Latitude": 50.1008, "Longitude": 14.26},
        {"Airport Name": "Amsterdam Schiphol", "IATA": "AMS", "Latitude": 52.3105, "Longitude": 4.7683},
    ])
    b = io.BytesIO()
    with pd.ExcelWriter(b, engine="xlsxwriter") as xw:
        df_airports.to_excel(xw, sheet_name="Airports", index=False)
    out["Airports.xlsx"] = b.getvalue()

    # Seaports.xlsx
    df_ports = pd.DataFrame([
        {"Seaport Name": "Rotterdam", "UNLOCODE": "", "Latitude": 51.9490, "Longitude": 4.1420},
        {"Seaport Name": "Hamburg", "UNLOCODE": "", "Latitude": 53.5461, "Longitude": 9.9661},
        {"Seaport Name": "Antwerp", "UNLOCODE": "", "Latitude": 51.2637, "Longitude": 4.3866},
        {"Seaport Name": "Gdynia", "UNLOCODE": "", "Latitude": 54.5333, "Longitude": 18.5500},
        {"Seaport Name": "Valencia", "UNLOCODE": "", "Latitude": 39.4400, "Longitude": -0.3167},
    ])
    b = io.BytesIO()
    with pd.ExcelWriter(b, engine="xlsxwriter") as xw:
        df_ports.to_excel(xw, sheet_name="Seaports", index=False)
    out["Seaports.xlsx"] = b.getvalue()

    return out

# ---------------------- ORS Routing ----------------------

def _route_cache_key(origin: Tuple[float, float], dest: Tuple[float, float]) -> str:
    return f"{origin[0]:.6f},{origin[1]:.6f}->{dest[0]:.6f},{dest[1]:.6f}"

def get_route_ors(api_key: str,
                  origin: Tuple[float, float],
                  dest: Tuple[float, float],
                  timeout_s: int = 20,
                  max_retries: int = 3,
                  backoff_s: float = 2.0,
                  route_cache: Dict[str, Dict[str, float]] = None
                  ) -> Tuple[float, float]:
    """Call ORS driving-car route between origin (lat,lon) and dest (lat,lon).
    Returns (distance_km, duration_min). Uses in-session cache.
    """
    if route_cache is None:
        route_cache = {}
    key = _route_cache_key(origin, dest)
    if key in route_cache:
        return route_cache[key]["distance_km"], route_cache[key]["duration_min"]

    url = "https://api.openrouteservice.org/v2/directions/driving-car"
    headers = {"Authorization": api_key, "Content-Type": "application/json"}
    body = {
        "coordinates": [ [origin[1], origin[0]], [dest[1], dest[0]] ],
        "units": "km"
    }

    attempt = 0
    last_err = None
    while attempt <= max_retries:
        try:
            resp = requests.post(url, headers=headers, data=json.dumps(body), timeout=timeout_s)
            if resp.status_code == 200:
                data = resp.json()
                summary = data["features"][0]["properties"]["summary"]
                distance_km = float(summary["distance"])  # already in km
                duration_min = float(summary["duration"]) / 60.0
                route_cache[key] = {"distance_km": distance_km, "duration_min": duration_min}
                return distance_km, duration_min
            elif resp.status_code == 429:
                time.sleep(backoff_s * (attempt + 1))
            else:
                last_err = f"HTTP {resp.status_code}: {resp.text[:200]}"
                break
        except Exception as e:
            last_err = str(e)
        attempt += 1
        time.sleep(backoff_s)

    raise RuntimeError(last_err or "Routing failed")

# ---------------------- Validation ----------------------

def _validate_columns(df: pd.DataFrame, required_cols: List[str]) -> List[str]:
    missing = [c for c in required_cols if c not in df.columns]
    return missing


def _validate_latlon(lat: pd.Series, lon: pd.Series) -> str:
    try:
        if not (np.isfinite(lat).all() and np.isfinite(lon).all()):
            return "Latitude/Longitude contain non-numeric values"
        if not (((lat >= -90) & (lat <= 90)).all() and ((lon >= -180) & (lon <= 180)).all()):
            return "Latitude must be in [-90,90] and Longitude in [-180,180]"
        return ""
    except Exception:
        return "Latitude/Longitude validation failed"

# ---------------------- Processing ----------------------

def process_batch(sites: pd.DataFrame,
                  airports: pd.DataFrame,
                  seaports: pd.DataFrame,
                  api_key: str,
                  topn: int,
                  include_ref: bool,
                  ref_lat: float,
                  ref_lon: float,
                  pause_every: int,
                  pause_secs: float,
                  progress_hook=None) -> Tuple[pd.DataFrame, List[Dict[str, Any]], int]:
    """Returns (results_df, logs, api_call_count)"""

    # Clean and coerce types
    sites = sites.copy()
    airports = airports.copy()
    seaports = seaports.copy()

    for col in ["Latitude", "Longitude"]:
        sites[col] = pd.to_numeric(sites[col], errors="coerce")
        airports[col] = pd.to_numeric(airports[col], errors="coerce")
        seaports[col] = pd.to_numeric(seaports[col], errors="coerce")

    # Validation
    err = _validate_latlon(sites["Latitude"], sites["Longitude"]) or           _validate_latlon(airports["Latitude"], airports["Longitude"]) or           _validate_latlon(seaports["Latitude"], seaports["Longitude"]) 
    if err:
        raise ValueError(err)

    # Precompute arrays
    a_lat = airports["Latitude"].to_numpy()
    a_lon = airports["Longitude"].to_numpy()
    p_lat = seaports["Latitude"].to_numpy()
    p_lon = seaports["Longitude"].to_numpy()

    route_cache = st.session_state.get("route_cache", {})

    results = []
    logs: List[Dict[str, Any]] = []
    api_calls = 0

    total = len(sites)
    for i, row in sites.iterrows():
        site_name = str(row["Site Name"]).strip()
        slat = float(row["Latitude"]) ; slon = float(row["Longitude"]) 
        site_origin = (slat, slon)

        log_rec = {"site": site_name, "steps": []}
        out_rec: Dict[str, Any] = {
            "Site Name": site_name,
            "Latitude": round(slat, 6),
            "Longitude": round(slon, 6),
            "Nearest Airport": None,
            "Distance to Airport (km)": None,
            "Time to Airport (min)": None,
            "Nearest Seaport": None,
            "Distance to Seaport (km)": None,
            "Time to Seaport (min)": None,
        }
        if include_ref:
            out_rec[f"Distance to {DEFAULT_REF['name']} (km)"] = None
            out_rec[f"Time to {DEFAULT_REF['name']} (min)"] = None

        try:
            # --- Airports: Haversine preselect ---
            dists_a = haversine_km(slat, slon, a_lat, a_lon)
            idxs_a = np.argsort(dists_a)[: min(topn, len(airports))]
            cand_airports = airports.iloc[idxs_a].copy()
            log_rec["steps"].append({"msg": f"Top-{len(cand_airports)} airports by air distance: {cand_airports['Airport Name'].tolist()}"})

            # Route to each candidate airport
            best_air = None
            best_air_d = math.inf
            best_air_t = math.inf
            for _, a in cand_airports.iterrows():
                dest = (float(a["Latitude"]), float(a["Longitude"]))
                try:
                    if api_calls and pause_every and api_calls % pause_every == 0:
                        if progress_hook:
                            progress_hook(f"Pausing {pause_secs}s to respect rate limits…")
                        time.sleep(pause_secs)
                    dist_km, dur_min = get_route_ors(api_key, site_origin, dest, route_cache=route_cache)
                    api_calls += 1
                    if dist_km < best_air_d:
                        best_air_d = dist_km
                        best_air_t = dur_min
                        best_air = a
                except Exception as e:
                    log_rec["steps"].append({"error": f"Airport '{a['Airport Name']}': {e}"})

            if best_air is not None:
                out_rec["Nearest Airport"] = str(best_air.get("Airport Name"))
                out_rec["Distance to Airport (km)"] = round(best_air_d, 1)
                out_rec["Time to Airport (min)"] = round(best_air_t, 1)
            else:
                out_rec["Nearest Airport"] = "ERROR"

            # --- Seaports: Haversine preselect ---
            dists_p = haversine_km(slat, slon, p_lat, p_lon)
            idxs_p = np.argsort(dists_p)[: min(topn, len(seaports))]
            cand_ports = seaports.iloc[idxs_p].copy()
            log_rec["steps"].append({"msg": f"Top-{len(cand_ports)} seaports by air distance: {cand_ports['Seaport Name'].tolist()}"})

            best_port = None
            best_port_d = math.inf
            best_port_t = math.inf
            for _, p in cand_ports.iterrows():
                dest = (float(p["Latitude"]), float(p["Longitude"]))
                try:
                    if api_calls and pause_every and api_calls % pause_every == 0:
                        if progress_hook:
                            progress_hook(f"Pausing {pause_secs}s to respect rate limits…")
                        time.sleep(pause_secs)
                    dist_km, dur_min = get_route_ors(api_key, site_origin, dest, route_cache=route_cache)
                    api_calls += 1
                    if dist_km < best_port_d:
                        best_port_d = dist_km
                        best_port_t = dur_min
                        best_port = p
                except Exception as e:
                    log_rec["steps"].append({"error": f"Seaport '{p['Seaport Name']}': {e}"})

            if best_port is not None:
                out_rec["Nearest Seaport"] = str(best_port.get("Seaport Name"))
                out_rec["Distance to Seaport (km)"] = round(best_port_d, 1)
                out_rec["Time to Seaport (min)"] = round(best_port_t, 1)
            else:
                out_rec["Nearest Seaport"] = "ERROR"

            # --- Reference ---
            if include_ref:
                try:
                    if api_calls and pause_every and api_calls % pause_every == 0:
                        if progress_hook:
                            progress_hook(f"Pausing {pause_secs}s to respect rate limits…")
                        time.sleep(pause_secs)
                    dist_km, dur_min = get_route_ors(api_key, site_origin, (ref_lat, ref_lon), route_cache=route_cache)
                    api_calls += 1
                    out_rec[f"Distance to {DEFAULT_REF['name']} (km)"] = round(dist_km, 1)
                    out_rec[f"Time to {DEFAULT_REF['name']} (min)"] = round(dur_min, 1)
                except Exception as e:
                    log_rec["steps"].append({"error": f"Reference: {e}"})

        except Exception as e:
            log_rec["steps"].append({"fatal": str(e)})
        results.append(out_rec)

        if progress_hook:
            progress_hook(f"Processed {len(results)}/{total}")

    st.session_state["route_cache"] = route_cache
    df_res = pd.DataFrame(results)
    return df_res, logs, api_calls

# ---------------------- UI ----------------------

def sidebar():
    st.sidebar.header("Settings")

    st.sidebar.subheader("Routing Provider")
    api_key = st.sidebar.text_input("OpenRouteService API key", type="password", help="Key is kept in session only.")

    st.sidebar.subheader("Top-N Prefilter")
    topn = st.sidebar.number_input("Top-N candidates by Haversine", min_value=1, max_value=20, value=3, step=1)

    st.sidebar.subheader("Rate limiting")
    pause_every = st.sidebar.number_input("Pause after X API calls", min_value=0, max_value=500, value=35, step=1,
                                          help="0 disables pausing. ORS free tier is ~40 req/min.")
    pause_secs = st.sidebar.number_input("Pause duration (seconds)", min_value=0.0, max_value=120.0, value=60.0, step=5.0)

    st.sidebar.subheader("Reference location")
    use_ref = st.sidebar.checkbox(f"Compute distance to reference ({DEFAULT_REF['name']})", value=True)
    ref_name = st.sidebar.text_input("Reference label", value=DEFAULT_REF['name'])
    ref_lat = st.sidebar.number_input("Reference latitude", value=float(DEFAULT_REF['lat']), format="%.6f")
    ref_lon = st.sidebar.number_input("Reference longitude", value=float(DEFAULT_REF['lon']), format="%.6f")

    st.sidebar.subheader("Cache")
    if st.sidebar.button("Clear route cache"):
        st.session_state["route_cache"] = {}
        st.sidebar.success("Route cache cleared")

    return api_key, topn, pause_every, pause_secs, use_ref, ref_name, ref_lat, ref_lon


def download_buttons_area():
    st.subheader("Templates")
    st.caption("Download Excel templates with correct headers and example rows.")
    files = template_files()
    cols = st.columns(3)
    with cols[0]:
        st.download_button("Download Sites.xlsx", data=files["Sites.xlsx"], file_name="Sites.xlsx")
    with cols[1]:
        st.download_button("Download Airports.xlsx", data=files["Airports.xlsx"], file_name="Airports.xlsx")
    with cols[2]:
        st.download_button("Download Seaports.xlsx", data=files["Seaports.xlsx"], file_name="Seaports.xlsx")


def upload_area() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    st.subheader("Upload your datasets")
    c1, c2, c3 = st.columns(3)
    sites_file = c1.file_uploader("Sites.xlsx (sheet 'Sites')", type=["xlsx"])
    airports_file = c2.file_uploader("Airports.xlsx (sheet 'Airports')", type=["xlsx"])
    seaports_file = c3.file_uploader("Seaports.xlsx (sheet 'Seaports')", type=["xlsx"])

    sites_df = airports_df = seaports_df = None

    def _read_xlsx(up: UploadedFile, sheet: str) -> pd.DataFrame:
        return pd.read_excel(up, engine="openpyxl", sheet_name=sheet)

    if sites_file is not None:
        sites_df = _read_xlsx(sites_file, "Sites")
        miss = _validate_columns(sites_df, REQUIRED_SITES_COLS)
        if miss:
            st.error(f"Sites.xlsx is missing columns: {', '.join(miss)}")
            sites_df = None

    if airports_file is not None:
        airports_df = _read_xlsx(airports_file, "Airports")
        miss = _validate_columns(airports_df, REQUIRED_AIRPORTS_COLS)
        if miss:
            st.error(f"Airports.xlsx is missing columns: {', '.join(miss)}")
            airports_df = None

    if seaports_file is not None:
        seaports_df = _read_xlsx(seaports_file, "Seaports")
        miss = _validate_columns(seaports_df, REQUIRED_SEAPORTS_COLS)
        if miss:
            st.error(f"Seaports.xlsx is missing columns: {', '.join(miss)}")
            seaports_df = None

    return sites_df, airports_df, seaports_df


def results_downloads(df: pd.DataFrame, filename_prefix: str = "results"):
    st.subheader("Downloads")
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    st.download_button("Download CSV", data=csv_bytes, file_name=f"{filename_prefix}.csv")

    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as xw:
        df.to_excel(xw, index=False, sheet_name="Results")
    st.download_button("Download XLSX", data=bio.getvalue(), file_name=f"{filename_prefix}.xlsx")


def maybe_map(df: pd.DataFrame, airports: pd.DataFrame, seaports: pd.DataFrame):
    if not _HAS_MAP:
        st.info("Optional map preview requires streamlit-folium and folium. Add them to requirements.txt.")
        return
    if df.empty:
        return
    st.subheader("Map preview (nearest picks)")
    mean_lat = df["Latitude"].mean()
    mean_lon = df["Longitude"].mean()
    m = folium.Map(location=[mean_lat, mean_lon], zoom_start=5)

    # Add site markers and lines to chosen airport & port
    for _, r in df.iterrows():
        site = [r["Latitude"], r["Longitude"]]
        folium.CircleMarker(site, radius=5, tooltip=r["Site Name"], fill=True).add_to(m)

        if isinstance(r.get("Nearest Airport"), str) and r.get("Nearest Airport") not in (None, "ERROR"):
            arow = airports[airports["Airport Name"] == r["Nearest Airport"]]
            if not arow.empty:
                a = [float(arow.iloc[0]["Latitude"]), float(arow.iloc[0]["Longitude"])]
                folium.Marker(a, tooltip=f"Airport: {r['Nearest Airport']}").add_to(m)
                folium.PolyLine([site, a], weight=2).add_to(m)

        if isinstance(r.get("Nearest Seaport"), str) and r.get("Nearest Seaport") not in (None, "ERROR"):
            prow = seaports[seaports["Seaport Name"] == r["Nearest Seaport"]]
            if not prow.empty:
                p = [float(prow.iloc[0]["Latitude"]), float(prow.iloc[0]["Longitude"])]
                folium.Marker(p, tooltip=f"Seaport: {r['Nearest Seaport']}").add_to(m)
                folium.PolyLine([site, p], weight=2).add_to(m)

    st_folium(m, height=500, use_container_width=True)


def main():
    st.set_page_config(page_title=APP_TITLE, layout="wide")
    st.title(APP_TITLE)
    st.caption("Compute road distance/time from sites to nearest airport and container seaport (with Top-N prefilter) and an optional reference location.")

    api_key, topn, pause_every, pause_secs, use_ref, ref_name, ref_lat, ref_lon = sidebar()

    download_buttons_area()

    sites_df, airports_df, seaports_df = upload_area()

    run = st.button("Run batch")

    if run:
        if api_key == "":
            st.error("Provide your OpenRouteService API key in the sidebar.")
            return
        if sites_df is None or airports_df is None or seaports_df is None:
            st.error("Upload all three templates with correct columns.")
            return
        if len(airports_df) == 0 or len(seaports_df) == 0 or len(sites_df) == 0:
            st.error("Uploaded files must contain at least one row in each sheet.")
            return

        # Progress UI
        status = st.empty()
        pbar = st.progress(0)
        total = len(sites_df)

        def progress_hook(msg: str):
            # Increase progress by one step when msg indicates processed count
            if "Processed" in msg:
                parts = msg.split()
                done = int(parts[1].split("/")[0])
                pbar.progress(min(done/total, 1.0))
            status.info(msg)

        try:
            df_res, logs, api_calls = process_batch(
                sites_df, airports_df, seaports_df,
                api_key=api_key,
                topn=int(topn),
                include_ref=use_ref,
                ref_lat=ref_lat,
                ref_lon=ref_lon,
                pause_every=int(pause_every),
                pause_secs=float(pause_secs),
                progress_hook=progress_hook,
            )
            st.success(f"Completed. API calls: {api_calls}. Cached routes: {len(st.session_state.get('route_cache', {}))}.")

            # Rename reference columns to current label
            if use_ref:
                df_res = df_res.rename(columns={
                    f"Distance to {DEFAULT_REF['name']} (km)": f"Distance to {ref_name} (km)",
                    f"Time to {DEFAULT_REF['name']} (min)": f"Time to {ref_name} (min)",
                })

            # Column order per spec
            cols = [
                "Site Name", "Latitude", "Longitude",
                "Nearest Airport", "Distance to Airport (km)", "Time to Airport (min)",
                "Nearest Seaport", "Distance to Seaport (km)", "Time to Seaport (min)",
            ]
            if use_ref:
                cols += [f"Distance to {ref_name} (km)", f"Time to {ref_name} (min)"]

            df_res = df_res[cols]

            st.subheader("Results")
            st.dataframe(df_res, use_container_width=True)
            results_downloads(df_res, filename_prefix="road_distance_results")

            with st.expander("Processing log (per-site)"):
                for rec in logs:
                    st.write(f"### {rec['site']}")
                    for step in rec["steps"]:
                        if "msg" in step:
                            st.write("- " + step["msg"])
                        if "error" in step:
                            st.error("- " + step["error"])
                        if "fatal" in step:
                            st.error("FATAL: " + step["fatal"])

            if st.checkbox("Show map preview (optional)"):
                maybe_map(df_res, airports_df, seaports_df)

        except Exception as e:
            st.error(f"Processing failed: {e}")
            st.exception(e)


if __name__ == "__main__":
    main()
