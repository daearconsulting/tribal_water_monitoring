"""
constants.py Project-wide constants for tribal_water_monitoring.
All values that change when adapting for a different Nation live in
config/config.yaml. Constants here are technically stable values:
CRS definitions, URL bases, field name standards, and shared references.
"""

from __future__ import annotations
from pathlib import Path

# Repository root
REPO_ROOT = Path(__file__).resolve().parents[1]

# Coordinate reference systems
CRS_GEOGRAPHIC = "EPSG:4326"   # WGS84 lat/lon for all spatial data
CRS_PROJECTED  = "EPSG:5070"   # Albers Equal Area CONUS for area calculations

# Data directories
CACHE_DIR     = REPO_ROOT / "data"/"cache"
RAW_DIR       = REPO_ROOT / "data"/"raw"
PROCESSED_DIR = REPO_ROOT / "data"/"processed"
TEMPLATE_DIR  = REPO_ROOT / "data"/"templates"
OUTPUTS_DIR   = REPO_ROOT / "outputs"
FIGURES_DIR   = OUTPUTS_DIR / "figures"

for _d in [CACHE_DIR, PROCESSED_DIR, OUTPUTS_DIR, FIGURES_DIR]:
    _d.mkdir(parents=True, exist_ok=True)

# API base URLs
USGS_NWIS_BASE      = "https://waterservices.usgs.gov/nwis"
USGS_NWIS_SITE_URL  = f"{USGS_NWIS_BASE}/site/"
USGS_NWIS_DV_URL    = f"{USGS_NWIS_BASE}/dv/"         # daily values
USGS_NWIS_GWL_URL   = f"{USGS_NWIS_BASE}/gwlevels/"   # groundwater levels
USGS_NWIS_WQ_URL    = "https://www.waterqualitydata.us/data/Result/search/"
CENSUS_TIGER_BASE   = "https://www2.census.gov/geo/tiger"
NHD_FLOWLINE_URL    = "https://hydro.nationalmap.gov/arcgis/rest/services/NHDPlus_HR/MapServer/3/query"
WBD_HUC8_URL        = "https://hydro.nationalmap.gov/arcgis/rest/services/NHDPlus_HR/MapServer/12/query"
NOAA_DROUGHT_BASE   = "https://www.ncei.noaa.gov/pub/data/cirs/climdiv"
MACA_THREDDS_BASE   = "http://thredds.northwestknowledge.net:8080/thredds/dodsC/agg_macav2metdata_"

# Oceti Sakowin Nations Census TIGER NAME field values
# These are the exact strings returned by the AIANNH shapefile NAME field
OCETI_SAKOWIN_CENSUS_NAMES = [
    "Pine Ridge",
    "Rosebud",
    "Standing Rock",
    "Cheyenne River",
    "Lower Brule",
    "Crow Creek",
    "Lake Traverse",
    "Flandreau",
]

# Common name mapping: Census NAME to community-preferred name
CENSUS_TO_COMMON = {
    "Pine Ridge":     "Oglala Lakota",
    "Rosebud":        "Sicangu Lakota (Rosebud)",
    "Standing Rock":  "Standing Rock Sioux",
    "Cheyenne River": "Cheyenne River Sioux",
    "Lower Brule":    "Lower Brule Sioux",
    "Crow Creek":     "Crow Creek Sioux",
    "Lake Traverse":  "Sisseton Wahpeton Oyate",
    "Flandreau":      "Flandreau Santee Sioux",
}
COMMON_TO_CENSUS = {v: k for k, v in CENSUS_TO_COMMON.items()}

# Primary demonstration sites
PRIMARY_NATIONS = ["Oglala Lakota", "Sicangu Lakota (Rosebud)"]

# Approximate centroids for API queries (WGS84)
NATION_CENTROIDS = {
    "Oglala Lakota":            {"lat": 43.35, "lon": -102.09},
    "Sicangu Lakota (Rosebud)": {"lat": 43.31, "lon": -100.64},
    "Standing Rock Sioux":      {"lat": 45.83, "lon": -101.15},
    "Cheyenne River Sioux":     {"lat": 45.07, "lon": -101.23},
    "Lower Brule Sioux":        {"lat": 44.09, "lon": -99.77},
    "Crow Creek Sioux":         {"lat": 44.13, "lon": -99.47},
    "Sisseton Wahpeton Oyate":  {"lat": 45.64, "lon": -97.10},
    "Flandreau Santee Sioux":   {"lat": 44.07, "lon": -96.57},
}

# Bounding boxes (WGS84: min_lon, min_lat, max_lon, max_lat)
PINE_RIDGE_BBOX  = (-103.5, 42.5, -101.5, 43.8)
ROSEBUD_BBOX     = (-101.5, 42.8,  -99.8, 43.6)
OCETI_SAKOWIN_BBOX = (-104.5, 42.3, -96.4, 46.5)

# USGS NWIS parameter codes
# Used when requesting specific variables from the NWIS API
NWIS_PARAMS = {
    "streamflow_cfs":     "00060",   # Discharge (cfs)
    "stage_ft":           "00065",   # Gage height (ft)
    "gw_depth_ft":        "72019",   # Depth to water level below land surface (ft)
    "gw_elev_ft":         "72020",   # Water level elevation above NGVD (ft)
    "temp_c":             "00010",   # Water temperature (°C)
    "do_mgl":             "00300",   # Dissolved oxygen (mg/L)
    "ph":                 "00400",   # pH
    "specific_cond":      "00095",   # Specific conductance (µS/cm at 25°C)
    "turbidity_fnu":      "63680",   # Turbidity (FNU)
    "nitrate_mgl":        "00618",   # Nitrate (mg/L as N)
    "tds_mgl":            "70300",   # Total dissolved solids (mg/L)
}

# Groundwater field names (must match Excel template)
GW_TEMPLATE_FIELDS = [
    "well_id",
    "date",
    "water_level_ft",
    "measurement_method",
    "entered_by",
    "notes",
]

# Optional GW fields (add when available)
GW_OPTIONAL_FIELDS = [
    "lat",
    "lon",
    "aquifer",
    "well_depth_ft",
    "casing_diameter_in",
]

# Water quality field names (must match Excel template)
WQ_TEMPLATE_FIELDS = [
    "site_id",
    "date",
    "sample_type",    # tap, spring, well, stream
    "nitrate_mgl",
    "ph",
    "tds_mgl",
    "turbidity_ntu",
    "arsenic_ugl",
    "fluoride_mgl",
    "entered_by",
    "notes",
]

# Data sovereignty references
GOVERNANCE_REFS = {
    "ocap":     "https://fnigc.ca/ocap-training/",
    "care":     "https://www.gida-global.org/care",
    "fair":     "https://www.go-fair.org/fair-principles/",
    "ieee_2890":"https://standards.ieee.org/ieee/2890/10318/",
    "local_contexts": "https://localcontexts.org/",
}
