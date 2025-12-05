# app.py
import streamlit as st
import pandas as pd
import requests
from io import BytesIO

from macro_data_api import (
    build_country_table,
    fetch_imf_series,
    wb_fetch_indicator,
    IMF_BASE_URL,
    imf_compact_to_df,
)

st.title("Macro Data Downloader")

mode = st.radio(
    "Mode",
    ("Full macro data pull", "IMF API connectivity test"),
)

st.write(
    "Use the test mode to verify IMF API connectivity and inspect raw output. "
    "Once that works, run the full macro data pull."
)


def run_imf_test():
    """
    Barebones IMF call for debugging:
    - IFS dataset
    - Monthly USD FX rate (ENDA_XDC_USD_RATE)
    - Netherlands (NL)
    - 2015-01 to 2018-12
    Shows raw response text + parsed DataFrame head.
    """
    st.subheader("IMF API connectivity test")

    base = IMF_BASE_URL.rstrip("/")
    url = (
        f"{base}/CompactData/IFS/M.NL.ENDA_XDC_USD_RATE"
        "?startPeriod=2015-01&endPeriod=2018-12"
    )

    st.markdown("**Request URL**")
    st.code(url, language="text")

    try:
        resp = requests.get(url, timeout=10)
    except Exception as e:
        st.error(f"Request error: {e}")
        return

    st.markdown("**HTTP status**")
    st.write(resp.status_code)

    st.markdown("**Content-Type**")
    st.write(resp.headers.get("content-type"))

    st.markdown("**Final URL (after redirects, if any)**")
    st.code(resp.url, language="text")

    st.markdown("**First 1000 characters of raw response body**")
    st.code(resp.text[:1000], language="text")

    # Try parsing JSON and converting to DataFrame
    if resp.ok and "json" in (resp.headers.get("content-type") or "").lower():
        try:
            json_obj = resp.json()
            df_test = imf_compact_to_df(json_obj)
            if not df_test.empty:
                st.markdown("**Parsed DataFrame (head)**")
                st.dataframe(df_test.head())
            else:
                st.warning("Parsed JSON but got an empty DataFrame.")
        except Exception as e:
            st.warning(f"Failed to parse JSON into DataFrame: {e}")
    else:
        st.warning("Response is not JSON, or status is not OK.")


if mode == "IMF API connectivity test":
    if st.button("Run IMF API connectivity test"):
        run_imf_test()

else:
    if st.button("Fetch and export data"):
        progress = st.progress(0)
        status_text = st.empty()

        try:
            # 1) Country lookup
            status_text.text("Building country table...")
            countries = build_country_table()
            iso2 = countries["iso2"].dropna().tolist()
            iso3 = countries["iso3"].dropna().tolist()
            progress.progress(10)

            # 2) IMF: CPI (monthly)
            status_text.text("Fetching IMF CPI (monthly)...")
            df_cpi = fetch_imf_series(
                dataset="CPI",
                freq="M",
                indicator="PCPI_IX",  # headline CPI index
                countries_iso2=iso2,
                start_period="1990-01",
                end_period="2025-12",
            )
            progress.progress(25)

            # 3) IMF: FX (monthly)
            status_text.text("Fetching IMF FX (monthly)...")
            df_fx = fetch_imf_series(
                dataset="ER",
                freq="M",
                indicator="ENDA_XDC_USD_RATE",  # USD exchange rate (example code)
                countries_iso2=iso2,
                start_period="1990-01",
                end_period="2025-12",
            )
            progress.progress(40)

            # 4) IMF: real GDP (quarterly)
            status_text.text("Fetching IMF real GDP (quarterly)...")
            df_qgdp = fetch_imf_series(
                dataset="IFS",
                freq="Q",
                indicator="NGDP_R_SA_XDC",
                countries_iso2=iso2,
                start_period="1990",
                end_period="2025",
            )
            progress.progress(55)

            # 5) World Bank: GDP growth
            status_text.text("Fetching World Bank GDP growth (annual)...")
            df_gdp_g = wb_fetch_indicator(
                indicator="NY.GDP.MKTP.KD.ZG",
                iso3_list=iso3,
                start_year=1990,
                end_year=2025,
            )
            progress.progress(65)

            # 6) World Bank: current account
            status_text.text("Fetching World Bank current account (annual)...")
            df_ca = wb_fetch_indicator(
                indicator="BN.CAB.XOKA.GD.ZS",
                iso3_list=iso3,
                start_year=1990,
                end_year=2025,
            )
            progress.progress(72)

            # 7) World Bank: FDI
            status_text.text("Fetching World Bank FDI net inflows (annual)...")
            df_fdi = wb_fetch_indicator(
                indicator="BX.KLT.DINV.WD.GD.ZS",
                iso3_list=iso3,
                start_year=1990,
                end_year=2025,
            )
            progress.progress(79)

            # 8) World Bank: reserves
            status_text.text("Fetching World Bank reserves (annual)...")
            df_reserves = wb_fetch_indicator(
                indicator="FI.RES.TOTL.MO",
                iso3_list=iso3,
                start_year=1990,
                end_year=2025,
            )
            progress.progress(86)

            # 9) World Bank: government debt
            status_text.text("Fetching World Bank government debt (annual)...")
            df_debt = wb_fetch_indicator(
                indicator="GC.DOD.TOTL.GD.ZS",
                iso3_list=iso3,
                start_year=1990,
                end_year=2025,
            )
            progress.progress(93)

            # 10) Write everything to a single Excel workbook in memory
            status_text.text("Writing data to Excel workbook...")
            output = BytesIO()
            with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
                countries.to_excel(writer, sheet_name="countries", index=False)
                df_cpi.to_excel(writer, sheet_name="cpi", index=False)
                df_fx.to_excel(writer, sheet_name="fx", index=False)
                df_qgdp.to_excel(writer, sheet_name="qgdp", index=False)
                df_gdp_g.to_excel(writer, sheet_name="gdp_growth", index=False)
                df_ca.to_excel(writer, sheet_name="current_account", index=False)
                df_fdi.to_excel(writer, sheet_name="fdi", index=False)
                df_reserves.to_excel(writer, sheet_name="reserves", index=False)
                df_debt.to_excel(writer, sheet_name="debt", index=False)

            output.seek(0)
            progress.progress(100)

            status_text.text("Done.")
            st.success("Data pull complete.")
            st.download_button(
                label="Download macro_data_panel.xlsx",
                data=output,
                file_name="macro_data_panel.xlsx",
                mime=(
                    "application/vnd.openxmlformats-officedocument."
                    "spreadsheetml.sheet"
                ),
            )

            st.subheader("CPI sample (first 10 rows)")
            st.dataframe(df_cpi.head(10))

        except Exception as e:
            status_text.text("Error during data fetch.")
            progress.progress(0)
            st.error(f"Error: {e}")
