"""
loaders.py public data loaders for tribal_water_monitoring.

All functions follow the same pattern:
  - Check cache first, download only if needed
  - force_refresh=True to re-download
  - Return clean GeoDataFrame or DataFrame
  - Treat missing/sparse data as a policy finding, not an error

Data sources:
  USGS NWIS  : streamflow, groundwater levels, water quality
  Census TIGER : AIANNH Tribal boundaries
  USGS NHD    : stream network
  USGS WBD    : watershed (HUC) boundaries
  NOAA        : PDSI drought index
"""

from __future__ import annotations

import io
import json
import logging
import warnings
import zipfile
import tempfile
from pathlib import Path
from typing import Optional

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from src.constants import (
    CACHE_DIR,
    CRS_GEOGRAPHIC,
    CRS_PROJECTED,
    CENSUS_TIGER_BASE,
    USGS_NWIS_SITE_URL,
    USGS_NWIS_DV_URL,
    USGS_NWIS_GWL_URL,
    USGS_NWIS_WQ_URL,
    NHD_FLOWLINE_URL,
    WBD_HUC8_URL,
    NOAA_DROUGHT_BASE,
    OCETI_SAKOWIN_CENSUS_NAMES,
    CENSUS_TO_COMMON,
    NWIS_PARAMS,
)

log = logging.getLogger(__name__)

_retry = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)


# Tribal boundaries

def load_tribal_boundaries(
    nation_names: list[str] | None = None,
    force_refresh: bool = False,
) -> gpd.GeoDataFrame:
    """
    Load AIANNH Tribal boundaries from Census TIGER.

    Parameters
    nation_names  : Census NAME field values to filter.
                    Defaults to all eight Oceti Sakowin Nations.
    force_refresh : Re-download even if cached.

    Returns
    GeoDataFrame with columns: NAME, common_name, area_km2, geometry
    """
    if nation_names is None:
        nation_names = OCETI_SAKOWIN_CENSUS_NAMES

    cache_path = CACHE_DIR / "tl_2023_us_aiannh.geojson"

    if not cache_path.exists() or force_refresh:
        log.info("Downloading Census TIGER AIANNH boundaries...")
        url = f"{CENSUS_TIGER_BASE}/TIGER2023/AIANNH/tl_2023_us_aiannh.zip"
        r   = requests.get(url, timeout=300)
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            with tempfile.TemporaryDirectory() as tmp:
                z.extractall(tmp)
                shp = next(Path(tmp).glob("*.shp"))
                all_aiannh = gpd.read_file(shp).to_crs(CRS_GEOGRAPHIC)
        all_aiannh.to_file(cache_path, driver="GeoJSON")
        log.info("AIANNH cached: %d features", len(all_aiannh))
    else:
        all_aiannh = gpd.read_file(cache_path)

    from shapely.validation import make_valid

    gdf = all_aiannh[all_aiannh["NAME"].isin(nation_names)].copy()
    gdf = gdf.dissolve(by="NAME", as_index=False)
    gdf["geometry"]    = gdf.geometry.apply(make_valid)
    gdf["common_name"] = gdf["NAME"].map(CENSUS_TO_COMMON)
    gdf["area_km2"]    = gdf.to_crs(CRS_PROJECTED).geometry.area / 1e6

    return gdf.reset_index(drop=True)


# USGS NWIS streamflow

@_retry
def load_streamflow(
    site_ids: list[str],
    start_date: str = "2000-01-01",
    end_date: str   = "2024-12-31",
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    Load daily mean streamflow from USGS NWIS (RDB format).

    Parameters
    site_ids    : List of USGS site IDs (ex. ['06447000'])
    start_date  : Start date string 'YYYY-MM-DD'
    end_date    : End date string 'YYYY-MM-DD'

    Returns
    DataFrame with columns: site_no, datetime, flow_cfs
    """
    param_code = NWIS_PARAMS["streamflow_cfs"]
    cache_key  = f"nwis_flow_{'_'.join(sorted(site_ids))}_{start_date[:4]}_{end_date[:4]}.csv"
    cache_file = CACHE_DIR/cache_key

    if cache_file.exists() and not force_refresh:
        df = pd.read_csv(cache_file, parse_dates=["datetime"])
        log.info("Streamflow loaded from cache: %d records", len(df))
        return df

    sites_str = ",".join(site_ids)
    r = requests.get(
        USGS_NWIS_DV_URL,
        params={
            "format":      "rdb",
            "sites":        sites_str,
            "startDT":      start_date,
            "endDT":        end_date,
            "parameterCd":  param_code,
            "statCd":       "00003",   # daily mean
        },
        timeout=120,
    )
    r.raise_for_status()

    records = _parse_nwis_rdb(r.text, value_col=f"{param_code}_00003_cd",
                               value_name="flow_cfs")
    df = pd.DataFrame(records)
    if df.empty:
        warnings.warn(
            f"No streamflow data returned for sites {site_ids}. "
            "USGS gauge coverage may be sparse near Tribal lands "
            "this is a monitoring gap, not a data error.",
            UserWarning,
            stacklevel=2,
        )
        return df

    df["datetime"] = pd.to_datetime(df["datetime"])
    df.to_csv(cache_file, index=False)
    log.info("Streamflow downloaded and cached: %d records", len(df))
    return df


# USGS NWIS groundwater levels

@_retry
def load_usgs_groundwater_sites(
    bbox: tuple[float, float, float, float],
    force_refresh: bool = False,
) -> gpd.GeoDataFrame:
    """
    Fetch USGS groundwater monitoring well sites within a bounding box.

    Parameters
    bbox : (min_lon, min_lat, max_lon, max_lat)

    Returns
    GeoDataFrame of USGS monitoring well sites with location and record info.
    Note: Coverage is systematically sparse on Tribal lands. Document gaps
    as a policy finding.
    """
    bbox_str   = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"
    cache_file = CACHE_DIR / f"usgs_gw_sites_{bbox_str.replace(',','_')}.csv"

    if cache_file.exists() and not force_refresh:
        df = pd.read_csv(cache_file, dtype=str)
    else:
        r = requests.get(
            USGS_NWIS_SITE_URL,
            params={
                "format":        "rdb",
                "bBox":          bbox_str,
                "siteType":      "GW",
                "hasDataTypeCd": "gw",
                "siteStatus":    "all",
            },
            timeout=60,
        )
        r.raise_for_status()
        df = _parse_nwis_site_rdb(r.text)
        df.to_csv(cache_file, index=False)

    if df.empty:
        warnings.warn(
            "No USGS groundwater monitoring sites found in this bounding box. "
            "Sparse monitoring coverage on Tribal lands is a federal infrastructure "
            "equity gap. Tribal-collected well data fills this gap.",
            UserWarning,
            stacklevel=2,
        )
        return gpd.GeoDataFrame()

    df["dec_lat_va"]  = pd.to_numeric(df.get("dec_lat_va"),  errors="coerce")
    df["dec_long_va"] = pd.to_numeric(df.get("dec_long_va"), errors="coerce")
    df = df.dropna(subset=["dec_lat_va", "dec_long_va"]).reset_index(drop=True)

    return gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df["dec_long_va"], df["dec_lat_va"]),
        crs=CRS_GEOGRAPHIC,
    )


@_retry
def load_usgs_groundwater_levels(
    site_no: str,
    start_date: str = "1980-01-01",
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    Load discrete groundwater level measurements for one USGS well.

    Returns
    DataFrame with columns: site_no, date, water_level_ft
    """
    cache_file = CACHE_DIR / f"gwl_{site_no}.csv"

    if cache_file.exists() and not force_refresh:
        df = pd.read_csv(cache_file, parse_dates=["date"])
        return df

    r = requests.get(
        USGS_NWIS_GWL_URL,
        params={"format": "rdb", "sites": site_no, "startDT": start_date},
        timeout=60,
    )
    r.raise_for_status()

    lines      = [l for l in r.text.splitlines() if not l.startswith("#")]
    if len(lines) < 3:
        return pd.DataFrame()

    from io import StringIO
    cols       = lines[0].split("\t")
    data_lines = [l for l in lines[2:] if l.strip()]
    if not data_lines:
        return pd.DataFrame()

    df = pd.read_csv(
        StringIO("\n".join(data_lines)),
        sep="\t", header=None, names=cols, dtype=str,
    )

    date_col  = next((c for c in cols if "lev_dt" in c or "date" in c.lower()), None)
    level_col = next((c for c in cols if "lev_va" in c), None)

    if not date_col or not level_col:
        return pd.DataFrame()

    df["date"]           = pd.to_datetime(df[date_col], errors="coerce")
    df["water_level_ft"] = pd.to_numeric(df[level_col], errors="coerce")
    df["site_no"]        = site_no

    result = (
        df[["site_no", "date", "water_level_ft"]]
        .dropna(subset=["date", "water_level_ft"])
        .reset_index(drop=True)
    )
    result.to_csv(cache_file, index=False)
    return result


# USGS/EPA Water Quality Portal 

@_retry
def load_water_quality(
    bbox: tuple[float, float, float, float],
    start_date: str = "2000-01-01",
    characteristics: list[str] | None = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    Load water quality data from the USGS/EPA Water Quality Portal.

    Parameters
    bbox            : (min_lon, min_lat, max_lon, max_lat)
    start_date      : Earliest sample date
    characteristics : List of parameter names to retrieve.
                      Defaults to key drinking water parameters.

    Returns
    DataFrame with sample results
    """
    if characteristics is None:
        characteristics = [
            "Nitrate", "pH", "Total dissolved solids",
            "Turbidity", "Arsenic", "Fluoride",
        ]

    cache_key  = f"wq_{bbox[0]}_{bbox[1]}_{bbox[2]}_{bbox[3]}_{start_date[:4]}.csv"
    cache_file = CACHE_DIR / cache_key.replace(" ", "_")

    if cache_file.exists() and not force_refresh:
        df = pd.read_csv(cache_file, parse_dates=["ActivityStartDate"])
        log.info("Water quality loaded from cache: %d records", len(df))
        return df

    params = {
        "bBox":              f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
        "startDateLo":       start_date,
        "characteristicName": "|".join(characteristics),
        "mimeType":          "csv",
        "dataProfile":       "narrowResult",
        "service":           "Result",
    }

    r = requests.get(USGS_NWIS_WQ_URL, params=params, timeout=120)
    r.raise_for_status()

    df = pd.read_csv(io.StringIO(r.text), dtype=str, low_memory=False)
    if df.empty:
        warnings.warn(
            "No water quality data returned for this bounding box. "
            "Public water quality monitoring coverage on Tribal lands is sparse.",
            UserWarning, stacklevel=2,
        )
        return df

    # Parse key fields
    if "ActivityStartDate" in df.columns:
        df["ActivityStartDate"] = pd.to_datetime(df["ActivityStartDate"], errors="coerce")
    if "ResultMeasureValue" in df.columns:
        df["result_value"] = pd.to_numeric(df["ResultMeasureValue"], errors="coerce")

    df.to_csv(cache_file, index=False)
    log.info("Water quality downloaded and cached: %d records", len(df))
    return df


# NHD stream flowlines

@_retry
def load_nhd_flowlines(
    bbox: tuple[float, float, float, float],
    min_stream_order: int = 1,
    force_refresh: bool = False,
) -> gpd.GeoDataFrame:
    """
    Load NHDPlus HR stream network flowlines within a bounding box.

    Parameters
    bbox             : (min_lon, min_lat, max_lon, max_lat)
    min_stream_order : Minimum Strahler stream order (1 = all streams)
    """
    cache_key  = f"nhd_{bbox[0]:.2f}_{bbox[1]:.2f}_{bbox[2]:.2f}_{bbox[3]:.2f}_o{min_stream_order}.geojson"
    cache_file = CACHE_DIR / cache_key

    if cache_file.exists() and not force_refresh:
        return gpd.read_file(cache_file)

    where = f"streamorde >= {min_stream_order}" if min_stream_order > 0 else "1=1"

    r = requests.get(
        NHD_FLOWLINE_URL,
        params={
            "where":          where,
            "outFields":      "reachcode,gnis_name,streamorde,lengthkm",
            "f":              "geojson",
            "returnGeometry": "true",
            "outSR":          "4326",
            "geometry":       f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
            "geometryType":   "esriGeometryEnvelope",
            "spatialRel":     "esriSpatialRelIntersects",
            "inSR":           "4326",
        },
        timeout=120,
    )

    # 500 = no data in area (ex. no perennial streams mapped)
    if r.status_code == 500:
        log.info("NHD returned 500 for bbox %s likely no mapped streams", bbox)
        return gpd.GeoDataFrame()

    r.raise_for_status()

    gdf = gpd.read_file(io.BytesIO(r.content))
    if not gdf.empty:
        gdf = gdf.set_crs(CRS_GEOGRAPHIC, allow_override=True)
        gdf.to_file(cache_file, driver="GeoJSON")

    return gdf


# WBD HUC boundaries

@_retry
def load_huc_boundary(
    bbox: tuple[float, float, float, float],
    huc_level: int = 8,
    force_refresh: bool = False,
) -> gpd.GeoDataFrame:
    """
    Load USGS Watershed Boundary Dataset (WBD) HUC polygons.

    Note: HUC boundaries are hydrologically defined, they do not align
    with Tribal territorial boundaries. Always overlay with Tribal boundaries
    for context.

    Parameters
    bbox      : (min_lon, min_lat, max_lon, max_lat)
    huc_level : HUC level (8, 10, or 12). Default 8.
    """
    cache_key  = f"wbd_huc{huc_level}_{bbox[0]:.2f}_{bbox[1]:.2f}_{bbox[2]:.2f}_{bbox[3]:.2f}.geojson"
    cache_file = CACHE_DIR / cache_key

    if cache_file.exists() and not force_refresh:
        return gpd.read_file(cache_file)

    # WBD layer IDs: HUC8=12, HUC10=9, HUC12=6 (NHDPlus HR MapServer)
    layer_map = {8: 12, 10: 9, 12: 6}
    layer_id  = layer_map.get(huc_level, 12)
    url = f"https://hydro.nationalmap.gov/arcgis/rest/services/NHDPlus_HR/MapServer/{layer_id}/query"

    r = requests.get(
        url,
        params={
            "where":          "1=1",
            "outFields":      f"huc{huc_level},name,areasqkm",
            "f":              "geojson",
            "returnGeometry": "true",
            "outSR":          "4326",
            "geometry":       f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
            "geometryType":   "esriGeometryEnvelope",
            "spatialRel":     "esriSpatialRelIntersects",
            "inSR":           "4326",
        },
        timeout=120,
    )

    if r.status_code == 500:
        log.info("WBD returned 500 for bbox %s", bbox)
        return gpd.GeoDataFrame()

    r.raise_for_status()
    gdf = gpd.read_file(io.BytesIO(r.content))
    if not gdf.empty:
        gdf = gdf.set_crs(CRS_GEOGRAPHIC, allow_override=True)
        gdf.to_file(cache_file, driver="GeoJSON")

    return gdf


# NOAA PDSI

def load_pdsi(
    state_code: str = "39",
    start_year: int = 1895,
    end_year:   int = 2024,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """
    Load NOAA Climate Division PDSI (Palmer Drought Severity Index).
    South Dakota state code: 39
    Climate divisions 1–9 cover the state.

    Returns
    DataFrame with columns: division, year, month, date, pdsi
    """
    cache_file = CACHE_DIR/"noaa_pdsi_climdiv.txt"

    if not cache_file.exists() or force_refresh:
        log.info("Downloading NOAA PDSI...")
        # NOAA updates filename monthly, try current then discover
        base_url = NOAA_DROUGHT_BASE
        try:
            import re
            dir_r = requests.get(base_url + "/", timeout=30)
            matches = re.findall(r"climdiv-pdsidv-v1\.0\.0-\d{8}", dir_r.text)
            fname   = matches[-1] if matches else "climdiv-pdsidv-v1.0.0-20250108"
        except Exception:
            fname = "climdiv-pdsidv-v1.0.0-20250108"

        r = requests.get(f"{base_url}/{fname}", timeout=120)
        r.raise_for_status()
        cache_file.write_text(r.text)

    raw_text = cache_file.read_text()
    records  = []

    SD_DIVISIONS = list(range(1, 10))
    DIV_NAMES = {
        1: "Northwest", 2: "North Central", 3: "Northeast",
        4: "West Central", 5: "Central", 6: "East Central",
        7: "Southwest", 8: "South Central", 9: "Southeast",
    }

    for line in raw_text.strip().splitlines():
        if not line.strip():
            continue
        parts = line.split()
        if len(parts) < 13:
            continue
        code = parts[0]
        if not code.startswith(state_code):
            continue
        div  = int(code[2:4])
        year = int(code[4:8])
        if div not in SD_DIVISIONS:
            continue
        if year < start_year or year > end_year:
            continue
        for month_idx, raw_val in enumerate(parts[1:13], start=1):
            try:
                val = float(raw_val)
                pdsi = np.nan if val <= -99 else val / 100
            except ValueError:
                pdsi = np.nan
            records.append({
                "division": div,
                "div_name": DIV_NAMES.get(div, str(div)),
                "year":     year,
                "month":    month_idx,
                "date":     pd.Timestamp(year=year, month=month_idx, day=1),
                "pdsi":     pdsi,
            })

    df = pd.DataFrame(records).dropna(subset=["pdsi"]).reset_index(drop=True)
    return df


# Tribal operational data loaders (local files)

def load_tribal_groundwater(
    path: str | Path | None = None,
) -> pd.DataFrame:
    """
    Load Tribal-collected groundwater level data from local Excel or CSV.
    This data is GITIGNORED and stays under Tribal control.
    See data/templates/groundwater_template.xlsx for the expected format.
    Returns empty DataFrame with correct columns if file not found,
    the pipeline degrades to public data only.
    """
    from src.constants import GW_TEMPLATE_FIELDS, RAW_DIR

    if path is None:
        # Try common locations
        candidates = [
            RAW_DIR/"groundwater.csv",
            RAW_DIR/"groundwater.xlsx",
            RAW_DIR/"groundwater_master.xlsx",
        ]
        path = next((p for p in candidates if p.exists()), None)

    if path is None:
        warnings.warn(
            "No Tribal groundwater data file found in data/raw/. "
            "Public USGS data will be used for analysis. "
            "See data/templates/groundwater_template.xlsx to start "
            "collecting field measurements.",
            UserWarning, stacklevel=2,
        )
        return pd.DataFrame(columns=GW_TEMPLATE_FIELDS)

    path = Path(path)
    if path.suffix in (".xlsx", ".xls"):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)

    # Standardize
    df.columns = df.columns.str.lower().str.strip()
    df["date"] = pd.to_datetime(df.get("date"), errors="coerce")
    df["water_level_ft"] = pd.to_numeric(df.get("water_level_ft"), errors="coerce")

    # Basic validation
    n_before = len(df)
    df = df.dropna(subset=["well_id", "date", "water_level_ft"])
    df = df[df["water_level_ft"] > 0]
    df = df[df["water_level_ft"] < 1000]
    n_after = len(df)

    if n_before > n_after:
        warnings.warn(
            f"Removed {n_before - n_after} invalid rows from groundwater data "
            "(missing required fields, negative values, or > 1000 ft).",
            UserWarning, stacklevel=2,
        )

    return df.reset_index(drop=True)


def load_tribal_water_quality(
    path: str | Path | None = None,
) -> pd.DataFrame:
    """
    Load Tribal-collected water quality data from local Excel or CSV.
    This data is GITIGNORED and stays under Tribal control.
    See data/templates/water_quality_template.xlsx for the expected format.
    """
    from src.constants import WQ_TEMPLATE_FIELDS, RAW_DIR

    if path is None:
        candidates = [
            RAW_DIR/"water_quality.csv",
            RAW_DIR/"water_quality.xlsx",
            RAW_DIR/"water_quality_master.xlsx",
        ]
        path = next((p for p in candidates if p.exists()), None)

    if path is None:
        warnings.warn(
            "No Tribal water quality data file found in data/raw/. "
            "Public WQP data will be used for context. "
            "See data/templates/water_quality_template.xlsx to start "
            "collecting sampling results.",
            UserWarning, stacklevel=2,
        )
        return pd.DataFrame(columns=WQ_TEMPLATE_FIELDS)

    path = Path(path)
    if path.suffix in (".xlsx", ".xls"):
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str)

    df.columns = df.columns.str.lower().str.strip()
    df["date"] = pd.to_datetime(df.get("date"), errors="coerce")

    # Parse numeric columns
    numeric_cols = ["nitrate_mgl", "ph", "tds_mgl", "turbidity_ntu",
                    "arsenic_ugl", "fluoride_mgl"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df.dropna(subset=["site_id", "date"]).reset_index(drop=True)


# Internal helpers

def _parse_nwis_rdb(text: str, value_col: str, value_name: str) -> list[dict]:
    """Parse a USGS NWIS RDB (tab-delimited) response into records."""
    from io import StringIO

    lines      = [l for l in text.splitlines() if not l.startswith("#")]
    if len(lines) < 3:
        return []
    cols       = lines[0].split("\t")
    data_lines = [l for l in lines[2:] if l.strip()]
    if not data_lines:
        return []

    df = pd.read_csv(
        StringIO("\n".join(data_lines)),
        sep="\t", header=None, names=cols, dtype=str,
    )

    records = []
    # Find the value column (NWIS codes like 00060_00003)
    val_col = next((c for c in df.columns if c.startswith("0") and "_" in c
                    and not c.endswith("_cd")), None)
    if val_col is None:
        return []

    for _, row in df.iterrows():
        try:
            records.append({
                "site_no":    row.get("site_no", ""),
                "datetime":   row.get("datetime", ""),
                value_name:   float(row[val_col]) if row[val_col] not in ("", None) else np.nan,
            })
        except (ValueError, KeyError):
            continue

    return records


def _parse_nwis_site_rdb(text: str) -> pd.DataFrame:
    """Parse a USGS NWIS site inventory RDB response."""
    from io import StringIO

    lines      = [l for l in text.splitlines() if not l.startswith("#")]
    if len(lines) < 3:
        return pd.DataFrame()

    cols       = lines[0].split("\t")
    data_lines = [l for l in lines[2:] if l.strip()]
    if not data_lines:
        return pd.DataFrame()

    return pd.read_csv(
        StringIO("\n".join(data_lines)),
        sep="\t", header=None, names=cols, dtype=str,
    )
