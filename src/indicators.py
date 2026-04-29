"""
indicators.py shared indicator computation functions for tribal_water_monitoring.

All indicator functions follow the same pattern:
  - Accept a DataFrame or Series
  - Return a DataFrame or Series with indicator columns added
  - Never raise exceptions for missing data (return NaN and warn)
  - Document what the indicator means and how it is computed

Used in both analysis notebooks and the operations pipeline.
"""

from __future__ import annotations

import warnings
import numpy as np
import pandas as pd
from scipy import stats


# Groundwater indicators

def compute_groundwater_trend(
    df: pd.DataFrame,
    level_col: str = "water_level_ft",
    date_col:  str = "date",
    window_days: int = 365,
) -> pd.DataFrame:
    """
    Compute rolling trend and decline rate for groundwater levels.

    Parameters
    df          : DataFrame with date and water level columns
    level_col   : Column name for depth to water (ft below surface)
                  Larger values = deeper water = worse conditions
    date_col    : Date column name
    window_days : Rolling window size in days for trend smoothing

    Returns
    DataFrame with added columns:
        trend_ft      : rolling mean water level
        delta_30d     : change over 30 days (positive = deepening)
        trend_slope   : Theil-Sen slope (ft/day)
    """
    df = df.sort_values(date_col).copy()
    df.set_index(date_col, inplace=True)

    df["trend_ft"]  = (
        df[level_col]
        .rolling(f"{window_days}D", min_periods=30)
        .mean()
    )
    df["delta_30d"] = df[level_col].diff(30)

    df.reset_index(inplace=True)
    return df


def classify_groundwater_status(
    df: pd.DataFrame,
    level_col: str = "water_level_ft",
    delta_col: str = "delta_30d",
    decline_threshold: float = 0.5,
    critical_pct: float = 10.0,
) -> pd.DataFrame:
    """
    Classify each groundwater observation as Stable, Declining, or Critical.

    Classification rules (in priority order):
        Critical   : level is in the bottom N% of historical observations
        Declining  : 30-day change exceeds the decline threshold (ft)
        Stable     : all other observations

    Parameters
    decline_threshold : ft/30-days trigger for Declining flag
    critical_pct      : Bottom N percentile threshold for Critical flag

    Returns
    DataFrame with added 'status' column
    """
    df = df.copy()
    critical_level = df[level_col].quantile(critical_pct / 100)

    df["status"] = "Stable"
    if delta_col in df.columns:
        df.loc[df[delta_col] > decline_threshold, "status"] = "Declining"
    df.loc[df[level_col] >= critical_level, "status"] = "Critical"

    return df


def groundwater_percentile_rank(
    current_level: float,
    historical_levels: pd.Series,
) -> float:
    """
    Return the percentile rank of a current water level vs. historical record.

    For depth to water: higher percentile = deeper water = worse conditions.

    Returns
    Percentile rank (0–100). 90th percentile = deeper than 90% of historical obs.
    """
    from scipy.stats import percentileofscore
    return percentileofscore(historical_levels.dropna(), current_level, kind="rank")


# Streamflow indicators

def compute_baseflow(
    df: pd.DataFrame,
    flow_col: str = "flow_cfs",
    window_days: int = 7,
) -> pd.DataFrame:
    """
    Estimate baseflow using rolling minimum method.

    The rolling minimum over N days approximates baseflow (the portion
    of streamflow sustained by groundwater discharge rather than direct
    precipitation runoff). 

    Parameters
    flow_col    : Column name for streamflow (cfs)
    window_days : Rolling window (days) 7 is standard for baseflow separation

    Returns
    DataFrame with added 'baseflow_cfs' column
    """
    df = df.copy().sort_values("datetime")
    df["baseflow_cfs"] = df[flow_col].rolling(window_days, min_periods=1).min()
    df["quickflow_cfs"] = (df[flow_col] - df["baseflow_cfs"]).clip(lower=0)
    df["baseflow_index"] = (
        df["baseflow_cfs"] / df[flow_col].replace(0, np.nan)
    ).clip(0, 1)
    return df


def classify_drought_stage(
    df: pd.DataFrame,
    flow_col: str = "flow_cfs",
    normal_cfs: float = 50.0,
    watch_cfs:  float = 20.0,
    emergency_cfs: float = 5.0,
) -> pd.DataFrame:
    """
    Classify each streamflow observation into a drought stage.

    Stage definitions (review and confirm thresholds with water office):
        Normal    : flow above normal_cfs
        Watch     : flow between watch_cfs and normal_cfs
        Emergency : flow below emergency_cfs

    Parameters
    Threshold values should be set in config.yaml and confirmed with
    Tribal water management staff for the specific gauge.

    Returns
    DataFrame with added 'drought_stage' and 'action' columns
    """
    ACTION_MAP = {
        "Normal":    "No action required",
        "Watch":     "Monitor closely: consider voluntary conservation",
        "Emergency": "Activate water conservation plan: restrict non-essential use",
    }

    df = df.copy()
    df["drought_stage"] = pd.cut(
        df[flow_col],
        bins=[-np.inf, emergency_cfs, watch_cfs, normal_cfs, np.inf],
        labels=["Emergency", "Watch", "Normal", "Normal"],
        ordered=False,
    )
    # pd.cut with non-ordered labels can produce duplicates 
    df["drought_stage"] = np.where(
        df[flow_col] <= emergency_cfs, "Emergency",
        np.where(df[flow_col] <= watch_cfs, "Watch", "Normal")
    )
    df["action"] = df["drought_stage"].map(ACTION_MAP)
    return df


def compute_flow_reliability(
    df: pd.DataFrame,
    flow_col: str = "flow_cfs",
    threshold_cfs: float = 1.0,
) -> pd.DataFrame:
    """
    Compute annual flow reliability: fraction of days flow exceeds threshold.

    Reliability = 0.0 means the stream was dry all year.
    Reliability = 1.0 means the stream flowed every day of the year.

    Returns annual reliability as a Series indexed by year.
    """
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["year"]     = df["datetime"].dt.year
    df["flowing"]  = df[flow_col] > threshold_cfs

    annual = (
        df.groupby("year")["flowing"]
        .agg(["sum", "count"])
        .assign(reliability=lambda x: x["sum"] / x["count"])
        .rename(columns={"sum": "days_flowing", "count": "days_total"})
        .reset_index()
    )
    return annual


# Water quality indicators

def flag_water_quality_exceedances(
    df: pd.DataFrame,
    thresholds: dict | None = None,
) -> pd.DataFrame:
    """
    Flag water quality measurements that exceed EPA MCLs or Tribal thresholds.

    Parameters
    df         : DataFrame with water quality measurement columns
    thresholds : Dict of {column_name: threshold_value}. Defaults to EPA MCLs.

    Returns
    DataFrame with added '{param}_alert' columns and 'any_alert' summary column
    """
    if thresholds is None:
        thresholds = {
            "nitrate_mgl":   10.0,
            "arsenic_ugl":   10.0,
            "tds_mgl":      500.0,
            "ph":          None,   # handled separately (range check)
            "turbidity_ntu":  1.0,
            "fluoride_mgl":   4.0,
        }

    df = df.copy()
    alert_cols = []

    for param, limit in thresholds.items():
        if param not in df.columns:
            continue
        if param == "ph":
            df["ph_alert"] = (df["ph"] < 6.5) | (df["ph"] > 8.5)
            alert_cols.append("ph_alert")
        elif limit is not None:
            col = f"{param}_alert"
            df[col] = df[param] > limit
            alert_cols.append(col)

    if alert_cols:
        df["any_alert"] = df[alert_cols].any(axis=1)
    else:
        df["any_alert"] = False

    return df


# Compound Water Stress Index

def normalize_0_1(series: pd.Series, invert: bool = False) -> pd.Series:
    """
    Min-max normalize a Series to [0, 1].
    invert=True: low raw value to high stress (for GW level, PDSI, flow)
    """
    mn, mx = series.min(), series.max()
    if mx == mn:
        return pd.Series(0.5, index=series.index)
    normed = (series - mn) / (mx - mn)
    return (1 - normed) if invert else normed


def compute_compound_stress_index(
    groundwater_stress: pd.Series | None = None,
    streamflow_stress:  pd.Series | None = None,
    drought_stress:     pd.Series | None = None,
    gw_weight:    float = 0.40,
    flow_weight:  float = 0.30,
    drought_weight: float = 0.30,
) -> pd.Series:
    """
    Compute the Compound Water Stress Index (CWSI).

    Combines groundwater, streamflow, and drought stress into a single
    normalized indicator (0 = no stress, 1 = maximum stress).

    Missing components are handled when the index is computed
    from whatever components are available, with weights redistributed.

    Parameters
    *_stress   : Pre-normalized stress Series (0–1, higher = more stress)
    *_weight   : Component weights (should sum to 1.0)

    Returns
    Series of CWSI values (0–1)
    """
    components = []
    weights    = []

    for series, weight in [
        (groundwater_stress, gw_weight),
        (streamflow_stress,  flow_weight),
        (drought_stress,     drought_weight),
    ]:
        if series is not None and not series.isna().all():
            components.append(series)
            weights.append(weight)

    if not components:
        warnings.warn(
            "No valid components for Compound Water Stress Index "
            "returning NaN series.",
            UserWarning, stacklevel=2,
        )
        return pd.Series(np.nan)

    total_weight = sum(weights)
    weighted_sum = sum(
        s * (w / total_weight)
        for s, w in zip(components, weights)
    )

    return weighted_sum.clip(0, 1).round(3)


def classify_stress_level(cwsi: pd.Series) -> pd.Series:
    """
    Classify CWSI values into named stress categories.

    Returns
    Series of stress category strings
    """
    return pd.cut(
        cwsi,
        bins=[-np.inf, 0.25, 0.50, 0.75, np.inf],
        labels=["Low", "Moderate", "High", "Critical"],
    )


# Trend analysis

def theilsen_trend(
    values: np.ndarray,
    years:  np.ndarray,
) -> dict:
    """
    Compute Theil-Sen slope and Mann-Kendall p-value for an annual time series.

    Returns
    Dict with: slope, slope_per_decade, p_value, significant, direction
    """
    mask   = ~np.isnan(values)
    yrs    = years[mask].astype(float)
    vals   = values[mask]

    if len(vals) < 5:
        return {
            "slope":            np.nan,
            "slope_per_decade": np.nan,
            "p_value":          np.nan,
            "significant":      False,
            "direction":        "Insufficient data",
        }

    slope, _, _, _ = stats.theilslopes(vals, yrs)
    _, _, _, p, _  = stats.linregress(yrs, vals)

    return {
        "slope":            round(slope, 6),
        "slope_per_decade": round(slope * 10, 4),
        "p_value":          round(p, 3),
        "significant":      p < 0.05,
        "direction":        "Worsening" if slope > 0 else "Improving / Stable",
    }
