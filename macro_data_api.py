# macro_data_api.py

import requests
import pandas as pd
import pycountry
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional

# ---- 1. Country mapping ----

RAW_COUNTRIES = [
    "Afghanistan",
    "Albania",
    "Argentina",
    "Armenia",
    "Austria",
    "Azerbaijan",
    "Burundi",
    "Belgium",
    "Burkina Faso",
    "Bangladesh",
    "Bulgaria",
    "Bahrain",
    "Belarus",
    "Bolivia",
    "Brazil",
    "Canada",
    "Switzerland",
    "Chile",
    "China (inc. Hong Kong SAR results)",
    "Cameroon",
    "D.R. Congo",
    "Colombia",
    "Cyprus",
    "Czech Republic",
    "Germany",
    "Denmark",
    "Algeria",
    "Ecuador",
    "Egypt",
    "Spain",
    "Estonia",
    "Ethiopia",
    "Finland",
    "France",
    "United Kingdom",
    "Georgia",
    "Ghana",
    "Guinea",
    "Greece",
    "Guatemala",
    "Hong Kong SAR (inc. China results)",
    "Honduras",
    "Haiti",
    "Hungary",
    "Indonesia",
    "India",
    "Iran",
    "Iraq",
    "Israel",
    "Italy",
    "Jamaica",
    "Jordan",
    "Kazakhstan",
    "Kenya",
    "Kyrgyz Republic",
    "Cambodia",
    "Korea",
    "Kosovo",
    "Kuwait",
    "Lebanon",
    "Libya",
    "Sri Lanka",
    "Lithuania",
    "Latvia",
    "Morocco",
    "Moldova",
    "Madagascar",
    "Mexico",
    "North Macedonia ",
    "Mali",
    "Myanmar",
    "Montenegro, Rep. of",
    "Mauritania",
    "Malawi",
    "Malaysia",
    "Niger",
    "Nigeria",
    "Nicaragua",
    "Netherlands",
    "Nepal",
    "New Zealand",
    "Pakistan",
    "Panama",
    "Peru",
    "Philippines",
    "Papua New Guinea",
    "Poland",
    "Puerto Rico",
    "Portugal",
    "Paraguay",
    "Qatar",
    "Romania",
    "Russia",
    "Rwanda",
    "Saudi Arabia",
    "Sudan",
    "Senegal",
    "El Salvador",
    "Somalia",
    "Serbia",
    "Slovak Republic",
    "Slovenia",
    "Sweden",
    "Eswatini",
    "Syria",
    "Chad",
    "Togo",
    "Thailand",
    "Tajikistan",
    "Timor-Leste, Dem. Rep. of",
    "Tunisia",
    "Turkey",
    "Taiwan Province of China",
    "Tanzania",
    "Uganda",
    "Ukraine",
    "United States",
    "Uzbekistan",
    "Venezuela",
    "Vietnam",
    "West Bank & Gaza",
    "Yemen",
    "South Africa",
    "Zambia",
    "Zimbabwe",
]

# Manual overrides where pycountry either fails or maps to the wrong entity
MANUAL_COUNTRY_OVERRIDES: Dict[str, Tuple[str, str, str]] = {
    "China (inc. Hong Kong SAR results)": ("CN", "CHN", "China"),
    "D.R. Congo": ("CD", "COD", "Congo, The Democratic Republic of the"),
    "Hong Kong SAR (inc. China results)": ("HK", "HKG", "Hong Kong"),
    "Montenegro, Rep. of": ("ME", "MNE", "Montenegro"),
    "Timor-Leste, Dem. Rep. of": ("TL", "TLS", "Timor-Leste"),
    "Taiwan Province of China": ("TW", "TWN", "Taiwan, Province of China"),
    "West Bank & Gaza": ("PS", "PSE", "Palestine, State of"),
    "Korea": ("KR", "KOR", "Korea, Republic of"),
    # Kosovo is not an ISO standard code; WB and others typically use XKX.
    "Kosovo": ("XK", "XKX", "Kosovo"),
    # trailing space variant
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
    """
    Build a mapping table from raw country names to ISO2 / ISO3 / official names.
    """
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


# ---- 2. IMF API helper ----

# IMF JSON RESTful API, CompactData method.
IMF_BASE_URL = "https://dataservices.imf.org/REST/SDMX_JSON.svc"


def imf_compact_request(
    dataset: str,
    freq: str,
    indicator: str,
    iso2_list: List[str],
    start_period: str = "1990",
    end_period: str = "2030",
) -> dict:
    """
    Low-level call to IMF CompactData endpoint.

    - dataset: e.g. 'IFS', 'CPI', 'BOP', 'QGDP_WCA', 'IRFCL', 'ER'
    - freq: 'A', 'Q', 'M', etc.
    - indicator: dataset-specific indicator code, e.g. 'NGDP_R_SA_XDC', 'PCPI_IX'
    - iso2_list: list of 2-letter country codes, e.g. ['BR', 'MX', 'ZA']
    """
    if not iso2_list:
        raise ValueError("iso2_list is empty")

    areas = "+".join(sorted(set(code.upper() for code in iso2_list)))
    key = f"{freq}.{areas}.{indicator}"
    url = f"{IMF_BASE_URL}/CompactData/{dataset}/{key}"
    params = {"startPeriod": start_period, "endPeriod": end_period}

    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def imf_compact_to_df(json_obj: dict) -> pd.DataFrame:
    """
    Parse IMF CompactData JSON into a tidy DataFrame with:
    ['freq', 'ref_area', 'indicator', 'time_period', 'value'].
    Handles single- and multi-country responses.
    """
    data = json_obj.get("CompactData", {}).get("DataSet", {})
    series = data.get("Series")

    if series is None:
        return pd.DataFrame(columns=["freq", "ref_area", "indicator", "time_period", "value"])

    # Normalize Series and Obs to lists
    if isinstance(series, dict):
        series_list = [series]
    else:
        series_list = series

    rows = []
    for s in series_list:
        freq = s.get("@FREQ")
        ref_area = s.get("@REF_AREA")
        indicator = s.get("@INDICATOR")
        obs = s.get("Obs", [])
        if isinstance(obs, dict):
            obs = [obs]

        for ob in obs:
            time_period = ob.get("@TIME_PERIOD")
            value = ob.get("@OBS_VALUE")
            if value in (None, ""):
                val = None
            else:
                try:
                    val = float(value)
                except ValueError:
                    val = None

            rows.append(
                {
                    "freq": freq,
                    "ref_area": ref_area,
                    "indicator": indicator,
                    "time_period": time_period,
                    "value": val,
                }
            )

    return pd.DataFrame(rows)


def fetch_imf_series(
    dataset: str,
    freq: str,
    indicator: str,
    countries_iso2: List[str],
    start_period: str = "1990",
    end_period: str = "2030",
) -> pd.DataFrame:
    """
    Convenience wrapper: IMF CompactData -> DataFrame.

    Example (IMF CPI headline index, monthly):
        df_cpi = fetch_imf_series(
            dataset="CPI",
            freq="M",
            indicator="PCPI_IX",
            countries_iso2=["BR", "MX", "ZA"],
            start_period="2000-01",
            end_period="2025-12",
        )
    """
    json_obj = imf_compact_request(
        dataset=dataset,
        freq=freq,
        indicator=indicator,
        iso2_list=countries_iso2,
        start_period=start_period,
        end_period=end_period,
    )
    return imf_compact_to_df(json_obj)


# ---- 3. World Bank Indicators API helper ----

# World Bank Indicators API base URL.
WB_BASE_URL = "https://api.worldbank.org/v2"


def wb_fetch_indicator(
    indicator: str,
    iso3_list: Optional[List[str]] = None,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
) -> pd.DataFrame:
    """
    Fetch a World Bank indicator for a list of countries.

    Example indicator codes:
      - 'NY.GDP.MKTP.KD.ZG'       GDP growth (annual %)
      - 'BN.CAB.XOKA.GD.ZS'       Current account balance (% of GDP)
      - 'BX.KLT.DINV.WD.GD.ZS'    FDI net inflows (% of GDP)
      - 'GC.DOD.TOTL.GD.ZS'       Central government debt (% of GDP)
      - 'FI.RES.TOTL.MO'          Total reserves in months of imports

    - iso3_list: list of 3-letter ISO codes, e.g. ['BRA', 'MEX', 'ZAF'].
      If None, uses 'all' countries.
    - start_year/end_year: restrict the year range, e.g. 1990, 2025.
    """
    if iso3_list:
        country_str = ";".join(code.upper() for code in iso3_list)
    else:
        country_str = "all"

    url = f"{WB_BASE_URL}/country/{country_str}/indicator/{indicator}"
    params = {"format": "json", "per_page": 20000}

    if start_year or end_year:
        if start_year and end_year:
            params["date"] = f"{start_year}:{end_year}"
        elif start_year:
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

    # convert year to integer where possible
    if not df.empty:
        df["date"] = pd.to_numeric(df["date"], errors="coerce").astype("Int64")

    return df


__all__ = [
    "RAW_COUNTRIES",
    "MANUAL_COUNTRY_OVERRIDES",
    "CountryCode",
    "resolve_country",
    "build_country_table",
    "IMF_BASE_URL",
    "imf_compact_request",
    "imf_compact_to_df",
    "fetch_imf_series",
    "WB_BASE_URL",
    "wb_fetch_indicator",
]
