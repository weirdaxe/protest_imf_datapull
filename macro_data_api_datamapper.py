import requests
import pandas as pd
from typing import List, Optional, Dict, Any

IMF_DM_BASE_URL = "https://www.imf.org/external/datamapper/api/v1"


# ---------- Metadata helpers ----------

def imf_dm_get_indicators() -> pd.DataFrame:
    """
    Return a table of all available DataMapper indicators.

    Columns: ['indicator', 'label', 'description', 'source', 'unit', 'dataset']
    """
    url = f"{IMF_DM_BASE_URL}/indicators"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()  # {"indicators": {code: {...}, ...}, "api": {...}}

    indicators = []
    for code, meta in data.get("indicators", {}).items():
        row = {"indicator": code}
        row.update(meta or {})
        indicators.append(row)

    return pd.DataFrame(indicators)


def imf_dm_ref_areas() -> pd.DataFrame:
    """
    Return reference areas (countries, regions, groups) from DataMapper.

    Columns: ['code', 'label', 'type'] where type ∈ {'country','region','group'}.
    """
    def _fetch(endpoint: str, type_name: str) -> pd.DataFrame:
        url = f"{IMF_DM_BASE_URL}/{endpoint}"
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        key = endpoint  # 'countries', 'regions', 'groups'
        items = []
        for code, meta in data.get(key, {}).items():
            row = {"code": code, "type": type_name}
            row.update(meta or {})
            items.append(row)
        return pd.DataFrame(items)

    df_c = _fetch("countries", "country")
    df_r = _fetch("regions", "region")
    df_g = _fetch("groups", "group")

    return pd.concat([df_c, df_r, df_g], ignore_index=True)


# ---------- Time series helper (annual only) ----------

def imf_dm_fetch_indicator(
    indicator: str,
    ref_areas: Optional[List[str]] = None,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
) -> pd.DataFrame:
    """
    Fetch an IMF DataMapper indicator as a tidy DataFrame.

    - indicator: DataMapper indicator code, e.g.:
        * 'NGDP_RPCH'  (Real GDP growth)       (WEO) :contentReference[oaicite:0]{index=0}
        * 'PCPIPCH'    (Inflation, average CPI pct change) :contentReference[oaicite:1]{index=1}

    - ref_areas: list of DataMapper reference area codes (usually ISO3 for countries,
      e.g. ['ARG','BRA','ZAF']). If None, returns all areas for that indicator.

    - start_year / end_year: years to restrict via the `periods` query string.
      If neither is provided, all years are returned.
      DataMapper is annual only. :contentReference[oaicite:2]{index=2}

    Returns DataFrame with columns:
        ['indicator', 'refarea', 'year', 'value', 'label', 'type']
    """
    # Build path: /api/v1/{indicator}/{ref1}/{ref2}/...
    parts = [indicator]
    if ref_areas:
        parts.extend(ref_areas)
    path = "/".join(parts)
    url = f"{IMF_DM_BASE_URL}/{path}"

    # Build 'periods' param: comma-separated list of years
    params: Dict[str, Any] = {}
    if start_year is not None or end_year is not None:
        if start_year is None and end_year is not None:
            # Only end_year given → from earliest up to end_year
            years = list(range(1900, end_year + 1))
        elif start_year is not None and end_year is None:
            years = list(range(start_year, start_year + 1))
        else:
            years = list(range(start_year, end_year + 1))
        params["periods"] = ",".join(str(y) for y in years)

    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    js = resp.json()

    # js structure:
    # {
    #   "values": {
    #       "NGDP_RPCH": {
    #           "ARG": {"2019": 0.5, "2020": -9.9, ...},
    #           "BRA": {...},
    #       }
    #   },
    #   "api": { ... }
    # }
    values = js.get("values", {})
    if not values:
        return pd.DataFrame(columns=["indicator", "refarea", "year", "value", "label", "type"])

    # Most calls are for a single indicator; use that
    series_dict = values.get(indicator)
    if series_dict is None:
        # fallback: first key if indicator name mismatch
        series_dict = next(iter(values.values()))

    rows = []
    for ref_code, series in series_dict.items():
        if not isinstance(series, dict):
            continue
        for year_str, val in series.items():
            if val is None:
                value = None
            else:
                try:
                    value = float(val)
                except (TypeError, ValueError):
                    value = None
            rows.append(
                {
                    "indicator": indicator,
                    "refarea": ref_code,
                    "year": int(year_str),
                    "value": value,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df.assign(label=pd.NA, type=pd.NA)

    # Attach labels and type (country/region/group)
    ref_table = imf_dm_ref_areas()
    df = df.merge(ref_table[["code", "label", "type"]], how="left",
                  left_on="refarea", right_on="code")
    df.drop(columns=["code"], inplace=True)

    return df[["indicator", "refarea", "year", "value", "label", "type"]]
