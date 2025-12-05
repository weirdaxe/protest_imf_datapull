# app.py
import streamlit as st
import pandas as pd
from io import BytesIO

from macro_data_api import (
    build_country_table,
    fetch_imf_series,
    wb_fetch_indicator,
)

st.title("Macro Data Downloader")

st.write(
    "Click the button to pull IMF & World Bank data for the predefined country set "
    "and download everything as a single Excel file (one sheet per series)."
)

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

        st.success("Data pull complete.")
        st.download_button(
            label="Download macro_data_panel.xlsx",
            data=output,
            file_name="macro_data_panel.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        # Optional: quick preview
        st.subheader("CPI sample (first 10 rows)")
        st.dataframe(df_cpi.head(10))

    except Exception as e:
        status_text.text("Error during data fetch.")
        progress.progress(0)
        st.error(f"Error: {e}")
