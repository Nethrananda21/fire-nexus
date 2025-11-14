# etl/ingest.py
import os
import time
import logging
from typing import Optional, Iterable
from io import StringIO
from datetime import datetime
import requests
import pandas as pd

from sqlalchemy import (
    create_engine, MetaData, Table, Column, Integer, Float, String, DateTime, UniqueConstraint
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from geoalchemy2 import Geometry, WKTElement
import sqlalchemy


# geopandas/shapely imports
import geopandas as gpd
from shapely.geometry import Point

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# default constants
MAP_KEY = os.getenv("MAP_KEY")
DEFAULT_SOURCE = "VIIRS_SNPP_NRT"
BASE_URL = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"

# Exceptions
class MissingAPIKeyError(RuntimeError):
    pass

class FetchError(RuntimeError):
    pass

def _check_map_key():
    if not MAP_KEY:
        raise MissingAPIKeyError("MAP_KEY environment variable is not set. Set it to your NASA FIRMS API key.")


def fetch_firms_data(
    days: int = 1,
    source: str = DEFAULT_SOURCE,
    area: str = "world",
    timeout: int = 30,
    max_retries: int = 3,
    backoff_factor: float = 1.0,
) -> pd.DataFrame:
    """
    Fetch FIRMS CSV data for a given area and days.

    Args:
        days: number of past days to fetch (1 = last 24 hours)
        source: FIRMS data source (e.g., "VIIRS_SNPP_NRT")
        area: "world" or a region identifier supported by the FIRMS API
        timeout: HTTP request timeout in seconds
        max_retries: number of retries on failure
        backoff_factor: backoff multiplier between retries (in seconds)

    Returns:
        pandas.DataFrame with raw CSV contents parsed.

    Raises:
        MissingAPIKeyError: if MAP_KEY not set
        FetchError: if HTTP response is not successful after retries
    """
    _check_map_key()
    url = f"{BASE_URL}/{MAP_KEY}/{source}/{area}/{days}"
    headers = {
        "Accept": "text/csv",
        "User-Agent": "etl-ingest/1.0 (+https://example.org/)"  # optional
    }

    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            logger.info("Fetching FIRMS data (attempt %d) from %s", attempt, url)
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                # Parse CSV response into pandas DataFrame
                df = pd.read_csv(StringIO(resp.text))
                logger.info("Fetched %d rows", len(df))
                return df
            else:
                logger.warning("Non-200 response (status=%s): %s", resp.status_code, resp.text[:200])
                # Raise to trigger retry logic
                raise FetchError(f"HTTP {resp.status_code}: {resp.text[:200]}")
        except (requests.RequestException, FetchError) as exc:
            logger.exception("Fetch attempt %d failed: %s", attempt, exc)
            if attempt >= max_retries:
                raise FetchError(f"Failed to fetch FIRMS data after {max_retries} attempts: {exc}")
            sleep_for = backoff_factor * (2 ** (attempt - 1))
            logger.info("Retrying after %.1f seconds...", sleep_for)
            time.sleep(sleep_for)
    raise FetchError("Unreachable code reached in fetch_firms_data")


def transform_data(
    df: pd.DataFrame,
    filter_confidence: Optional[Iterable] = None,  # can be list of strings OR a numeric threshold like {"min":30}
    keep_fields: Optional[Iterable[str]] = None,
    timezone_aware: bool = True,
) -> gpd.GeoDataFrame:
    required_cols = {"acq_date", "acq_time", "latitude", "longitude"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns in input DataFrame: {missing}")

    df = df.copy()

    # Ensure acq_time is string with zero-padding
    df["acq_time"] = df["acq_time"].astype(int).astype(str).str.zfill(4)

    # Combine into datetime
    dt_str = df["acq_date"].astype(str) + " " + df["acq_time"]
    df["acq_datetime"] = pd.to_datetime(dt_str, format="%Y-%m-%d %H%M", errors="coerce")
    if timezone_aware:
        df["acq_datetime"] = df["acq_datetime"].dt.tz_localize("UTC", ambiguous="NaT", nonexistent="NaT")

    # Convert numeric columns
    num_cols = ["latitude", "longitude", "bright_ti4", "bright_ti5", "frp"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # DIAGNOSTIC: report confidence dtype and value distribution
    if "confidence" in df.columns:
        conf_series = df["confidence"]
        # detect numeric-like (all values numeric or numeric-strings)
        # attempt to coerce to numeric and see how many non-nulls we get
        conf_num = pd.to_numeric(conf_series, errors="coerce")
        num_numeric = conf_num.notna().sum()
        num_total = len(conf_series)
        print(f"Confidence column: {num_numeric}/{num_total} values look numeric after coercion.")
        # show top unique samples (trimmed)
        unique_vals = pd.unique(conf_series).tolist()[:20]
        print("Sample unique confidence values (up to 20):", unique_vals)
    else:
        print("No 'confidence' column in dataframe.")

    # Apply filter_confidence if provided
    if filter_confidence is not None and "confidence" in df.columns:
        # If user passed a dict like {"min":30} use numeric threshold
        if isinstance(filter_confidence, dict) and "min" in filter_confidence:
            min_val = float(filter_confidence["min"])
            df["confidence_numeric"] = pd.to_numeric(df["confidence"], errors="coerce")
            before = len(df)
            df = df[df["confidence_numeric"].notna() & (df["confidence_numeric"] >= min_val)].copy()
            print(f"Applied numeric threshold filter: confidence >= {min_val} -> {len(df)} rows (before {before})")
            # drop helper column later
            if "confidence_numeric" in df.columns:
                df = df.drop(columns=["confidence_numeric"])
        else:
            # If values are numeric in the column, try numeric matching first
            # If filter_confidence is a list of strings, use .isin()
            # If it's a list of numbers, convert and compare numeric values
            fc_list = list(filter_confidence)
            # try to coerce DataFrame confidence to numeric
            conf_num_full = pd.to_numeric(df["confidence"], errors="coerce")
            if conf_num_full.notna().sum() > 0 and all(isinstance(x, (int, float)) or (isinstance(x, str) and x.strip().isdigit()) for x in fc_list):
                # numeric matching
                fc_nums = []
                for x in fc_list:
                    try:
                        fc_nums.append(float(x))
                    except Exception:
                        pass
                before = len(df)
                df = df[conf_num_full.isin(fc_nums)].copy()
                print(f"Applied numeric equality filter on confidence values {fc_nums} -> {len(df)} rows (before {before})")
            else:
                # fallback to text matching (case-insensitive)
                before = len(df)
                df = df[df["confidence"].astype(str).str.lower().isin([str(x).lower() for x in fc_list])].copy()
                print(f"Applied text filter on confidence values {fc_list} -> {len(df)} rows (before {before})")
    elif filter_confidence is not None:
        print("filter_confidence provided but no 'confidence' column available; skipping filter.")

    # Build geometry
    df["geometry"] = [Point(xy) if not pd.isna(xy[0]) and not pd.isna(xy[1]) else None
                      for xy in zip(df["longitude"], df["latitude"])]

    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    default_keep = ["latitude", "longitude", "bright_ti4", "bright_ti5", "frp", "confidence", "acq_datetime", "geometry"]
    if keep_fields is None:
        keep_fields = default_keep
    else:
        keep_fields = list(keep_fields)
        if "geometry" not in keep_fields:
            keep_fields.append("geometry")
        if "acq_datetime" not in keep_fields:
            keep_fields.append("acq_datetime")

    keep_fields = [c for c in keep_fields if c in gdf.columns]
    gdf = gdf[keep_fields].copy()

    # WKT / GeoJSON columns (best effort)
    try:
        gdf["wkt"] = gdf["geometry"].apply(lambda geom: geom.wkt if geom is not None else None)
        gdf["geojson"] = gdf["geometry"].apply(lambda geom: gpd.GeoSeries([geom]).__geo_interface__['features'][0]['geometry'] if geom is not None else None)
    except Exception:
        pass

    before = len(gdf)
    gdf = gdf[~gdf["geometry"].isna()].copy()
    if "acq_datetime" in gdf.columns:
        gdf = gdf[~gdf["acq_datetime"].isna()].copy()
    print(f"Dropped {before - len(gdf)} rows without geometry/timestamp -> {len(gdf)} rows remain")

    # normalize confidence codes to full words and add color label
    conf_map = {"h": "high", "n": "nominal", "l": "low"}
    gdf["confidence_label"] = gdf["confidence"].astype(str).str.lower().map(conf_map).fillna(gdf["confidence"])
    # optional color for mapping / visualization
    color_map = {"high": "red", "nominal": "orange", "low": "yellow"}
    gdf["confidence_color"] = gdf["confidence_label"].map(color_map).fillna("gray")


    return gdf



# Convenience function: fetch + transform in one call
def fetch_and_transform(
    days: int = 1,
    source: str = DEFAULT_SOURCE,
    area: str = "world",
    filter_confidence: Optional[Iterable[str]] = None,
    **fetch_kwargs,
) -> gpd.GeoDataFrame:
    """
    Combined helper: fetch raw FIRMS CSV and transform to GeoDataFrame.

    Example:
        gdf = fetch_and_transform(days=2, area="world", filter_confidence=['nominal','high'])
    """
    raw = fetch_firms_data(days=days, source=source, area=area, **fetch_kwargs)
    return transform_data(raw, filter_confidence=filter_confidence)


# If run as a script, demo usage (will raise if MAP_KEY missing)
if __name__ == "__main__":
    try:
        print("Fetching and transforming FIRMS data...")

        gdf = fetch_and_transform(
            days=1,
            area="world",
            filter_confidence=["h", "n"]   # FIXED!
        )

        print("\nTotal fire detections:", len(gdf))
        print("\nSample results:")
        print(gdf.head().to_string(index=False))

        # Save to CSV (with WKT, without geometry column)
        gdf.drop(columns="geometry").to_csv("fires_last24h.csv", index=False)
        print("\nSaved CSV -> fires_last24h.csv")

    except MissingAPIKeyError as e:
        print("ERROR:", e)
    except Exception as e:
        print("ERROR:", e)



