import requests
import pandas as pd
import pycountry
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional, Any

# ---- 1. Country mapping ----

RAW_COUNTRIES = [
    'Afghanistan', 'Albania', 'Argentina', 'Armenia', 'Austria',
    'Azerbaijan', 'Burundi', 'Belgium', 'Burkina Faso', 'Bangladesh',
    'Bulgaria', 'Bahrain', 'Belarus', 'Bolivia', 'Brazil', 'Canada',
    'Switzerland', 'Chile', 'China (inc. Hong Kong SAR results)',
    'Cameroon', 'D.R. Congo', 'Colombia', 'Cyprus', 'Czech Republic',
    'Germany', 'Denmark', 'Algeria', 'Ecuador', 'Egypt', 'Spain',
    'Estonia', 'Ethiopia', 'Finland', 'France', 'United Kingdom',
    'Georgia', 'Ghana', 'Guinea', 'Greece', 'Guatemala',
    'Hong Kong SAR (inc. China results)', 'Honduras', 'Haiti',
    'Hungary', 'Indonesia', 'India', 'Iran', 'Iraq', 'Israel', 'Italy',
    'Jamaica', 'Jordan', 'Kazakhstan', 'Kenya', 'Kyrgyz Republic',
    'Cambodia', 'Korea', 'Kosovo', 'Kuwait', 'Lebanon', 'Libya',
    'Sri Lanka', 'Lithuania', 'Latvia', 'Morocco', 'Moldova',
    'Madagascar', 'Mexico', 'North Macedonia ', 'Mali', 'Myanmar',
    'Montenegro, Rep. of', 'Mauritania', 'Malawi', 'Malaysia', 'Niger',
    'Nigeria', 'Nicaragua', 'Netherlands', 'Nepal', 'New Zealand',
    'Pakistan', 'Panama', 'Peru', 'Philippines', 'Papua New Guinea',
    'Poland', 'Puerto Rico', 'Portugal', 'Paraguay', 'Qatar',
    'Romania', 'Russia', 'Rwanda', 'Saudi Arabia', 'Sudan', 'Senegal',
    'El Salvador', 'Somalia', 'Serbia', 'Slovak Republic', 'Slovenia',
    'Sweden', 'Eswatini', 'Syria', 'Chad', 'Togo', 'Thailand',
    'Tajikistan', 'Timor-Leste, Dem. Rep. of', 'Tunisia', 'Turkey',
    'Taiwan Province of China', 'Tanzania', 'Uganda', 'Ukraine',
    'United States', 'Uzbekistan', 'Venezuela', 'Vietnam',
    'West Bank & Gaza', 'Yemen', 'South Africa', 'Zambia', 'Zimbabwe'
]

MANUAL_COUNTRY_OVERRIDES: Dict[str, Tuple[str, str, str]] = {
    "China (inc. Hong Kong SAR results)": ("CN", "CHN", "China"),
    "D.R. Congo": ("CD", "COD", "Congo, The Democratic Republic of the"),
    "Hong Kong SAR (inc. China results)": ("HK", "HKG", "Hong Kong"),
    "Montenegro, Rep. of": ("ME", "MNE", "Montenegro"),
    "Timor-Leste, Dem. Rep. of": ("TL", "TLS", "Timor-Leste"),
    "Taiwan Province of China": ("TW", "TWN", "Taiwan, Province of China"),
    "West Bank & Gaza": ("PS", "PSE", "Palestine, State of"),
    "Korea": ("KR", "KOR", "Korea, Republic of"),
    "Kosovo": ("XK", "XKX", "Kosovo"),
    "North Macedonia ": ("MK", "MKD", "North Macedonia"),
}


@dataclass
class CountryCode:
    raw_name: str
    iso2: str
    iso3: str
    official_name: str


def resolve_country(raw_name: str) -> CountryCode:
    raw = raw_name.strip()
    if raw in MANUAL_COUNTRY_OVERRIDES:
        iso2, iso3, official = MANUAL_COUNTRY_OVERRIDES[raw]
        return CountryCode(raw_name=raw_name, iso2=iso2, iso3=iso3, official_name=official)
    match = pycountry.countries.search_fuzzy(raw)[0]
    return CountryCode(
        raw_name=raw_name,
        iso2=match.alpha_2,
        iso3=match.alpha_3,
        official_name=match.name,
    )


def build_country_table(raw_names: Optional[List[str]] = None) -> pd.DataFrame:
    if raw_names is None:
        raw_names = RAW_COUNTRIES
    rows = []
    for n in raw_names:
        try:
            c = resolve_country(n)
            rows.append(
                {
                    "raw_name": c.raw_name,
                    "iso2": c.iso2,
                    "iso3": c.iso3,
                    "official_name": c.official_name,
                }
            )
        except Exception as exc:
            rows.append(
                {
                    "raw_name": n,
                    "iso2": None,
                    "iso3": None,
                    "official_name": f"UNRESOLVED: {exc}",
                }
            )
    return pd.DataFrame(rows)


# ---- 2. IMF DataMapper API helpers ----

IMF_DM_BASE_URL = "https://www.imf.org/external/datamapper/api/v1"

_IMF_DM_REFAREAS_CACHE: Optional[pd.DataFrame] = None


def imf_dm_get_indicators() -> pd.DataFrame:
    url = f"{IMF_DM_BASE_URL}/indicators"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    indicators = []
    for code, meta in data.get("indicators", {}).items():
        row = {"indicator": code}
        if isinstance(meta, dict):
            row.update(meta)
        indicators.append(row)
    return pd.DataFrame(indicators)


def imf_dm_ref_areas() -> pd.DataFrame:
    global _IMF_DM_REFAREAS_CACHE
    if _IMF_DM_REFAREAS_CACHE is not None:
        return _IMF_DM_REFAREAS_CACHE

    def _fetch(endpoint: str, type_name: str) -> pd.DataFrame:
        url = f"{IMF_DM_BASE_URL}/{endpoint}"
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        key = endpoint  # 'countries', 'regions', 'groups'
        items = []
        for code, meta in data.get(key, {}).items():
            row = {"code": code, "type": type_name}
            if isinstance(meta, dict):
                row.update(meta)
            items.append(row)
        return pd.DataFrame(items)

    df_c = _fetch("countries", "country")
    df_r = _fetch("regions", "region")
    df_g = _fetch("groups", "group")

    ref_df = pd.concat([df_c, df_r, df_g], ignore_index=True)
    _IMF_DM_REFAREAS_CACHE = ref_df
    return ref_df


def imf_dm_fetch_indicator(
    indicator: str,
    ref_areas: Optional[List[str]] = None,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
) -> pd.DataFrame:
    parts = [indicator]
    if ref_areas:
        parts.extend([c.strip().upper() for c in ref_areas if c])
    path = "/".join(parts)
    url = f"{IMF_DM_BASE_URL}/{path}"

    params: Dict[str, Any] = {}
    years = None
    if start_year is not None and end_year is not None:
        years = list(range(start_year, end_year + 1))
    elif start_year is not None:
        years = [start_year]
    elif end_year is not None:
        years = [end_year]

    if years:
        params["periods"] = ",".join(str(y) for y in years)

    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    js = resp.json()

    values = js.get("values", {})
    if not values:
        return pd.DataFrame(columns=["indicator", "refarea", "year", "value", "label", "type"])

    series_dict = values.get(indicator)
    if series_dict is None and values:
        series_dict = next(iter(values.values()))

    rows = []
    for ref_code, series in series_dict.items():
        if not isinstance(series, dict):
            continue
        for year_str, val in series.items():
            try:
                year_int = int(year_str)
            except (TypeError, ValueError):
                continue
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
                    "year": year_int,
                    "value": value,
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df.assign(label=pd.NA, type=pd.NA)

    ref_table = imf_dm_ref_areas()
    df = df.merge(
        ref_table[["code", "label", "type"]],
        how="left",
        left_on="refarea",
        right_on="code",
    )
    df = df.drop(columns=["code"])
    return df[["indicator", "refarea", "year", "value", "label", "type"]]


def fetch_imf_gdp_growth_weo(
    iso3_list: List[str],
    start_year: int = 1990,
    end_year: int = 2025,
) -> pd.DataFrame:
    return imf_dm_fetch_indicator(
        indicator="NGDP_RPCH",
        ref_areas=[c.upper() for c in iso3_list],
        start_year=start_year,
        end_year=end_year,
    )


def fetch_imf_cpi_inflation_weo(
    iso3_list: List[str],
    start_year: int = 1990,
    end_year: int = 2025,
) -> pd.DataFrame:
    # CPI inflation, average consumer prices (percent change)
    return imf_dm_fetch_indicator(
        indicator="PCPIPCH",
        ref_areas=[c.upper() for c in iso3_list],
        start_year=start_year,
        end_year=end_year,
    )


# ---- 3. World Bank Indicators API helper ----

WB_BASE_URL = "https://api.worldbank.org/v2"


def wb_fetch_indicator(
    indicator: str,
    iso3_list: Optional[List[str]] = None,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
) -> pd.DataFrame:
    if iso3_list:
        country_str = ";".join(code.upper() for code in iso3_list)
    else:
        country_str = "all"

    url = f"{WB_BASE_URL}/country/{country_str}/indicator/{indicator}"
    params: Dict[str, Any] = {"format": "json", "per_page": 20000}

    if start_year is not None or end_year is not None:
        if start_year is not None and end_year is not None:
            params["date"] = f"{start_year}:{end_year}"
        elif start_year is not None:
            params["date"] = f"{start_year}:"
        else:
            params["date"] = f":{end_year}"

    rows = []
    page = 1
    while True:
        params["page"] = page
        resp = requests.get(url, params=params, timeout=60)
        resp.raise_for_status()
        payload = resp.json()
        if not payload or len(payload) < 2 or payload[1] is None:
            break
        meta, data = payload
        for obs in data:
            rows.append(
                {
                    "country": obs["country"]["value"],
                    "iso3": obs["countryiso3code"],
                    "indicator": obs["indicator"]["id"],
                    "date": obs["date"],
                    "value": obs["value"],
                }
            )
        total_pages = int(meta.get("pages", 1))
        if page >= total_pages:
            break
        page += 1

    df = pd.DataFrame(rows)
    if not df.empty:
        df["date"] = pd.to_numeric(df["date"], errors="coerce").astype("Int64")
    return df


__all__ = [
    "RAW_COUNTRIES",
    "MANUAL_COUNTRY_OVERRIDES",
    "CountryCode",
    "resolve_country",
    "build_country_table",
    "IMF_DM_BASE_URL",
    "imf_dm_get_indicators",
    "imf_dm_ref_areas",
    "imf_dm_fetch_indicator",
    "fetch_imf_gdp_growth_weo",
    "fetch_imf_cpi_inflation_weo",
    "WB_BASE_URL",
    "wb_fetch_indicator",
]
