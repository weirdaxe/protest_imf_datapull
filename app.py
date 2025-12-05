import streamlit as st
import pandas as pd
from io import BytesIO

from macro_data_api import (
    build_country_table,
    fetch_imf_gdp_growth_weo,
    fetch_imf_cpi_inflation_weo,
    wb_fetch_indicator,
)

st.title("Macro Data Downloader (IMF DataMapper + World Bank)")

st.write(
    "Click the button to pull IMF DataMapper and World Bank annual macro data "
    "for the predefined country set and download everything as a single Excel file "
    "(one sheet per dataset)."
)

use_sample_countries = st.checkbox(
    "Use small sample of countries (ARG, BRA, ZAF) instead of full list",
    value=True,
)

if st.button("Fetch and export data"):
    progress = st.progress(0)
    status_text = st.empty()

    try:
        # 1) Country lookup
        status_text.text("Building country table...")
        countries = build_country_table()
        countries = countries[countries["iso2"].notna() & countries["iso3"].notna()]

        if use_sample_countries:
            sample_iso3 = ["ARG", "BRA", "ZAF"]
            countries = countries[countries["iso3"].isin(sample_iso3)]

        iso3 = countries["iso3"].tolist()
        if not iso3:
            st.error("No valid ISO3 country codes found.")
            st.stop()

        progress.progress(10)

        # 2) IMF DataMapper: WEO real GDP growth (NGDP_RPCH)
        status_text.text("Fetching IMF WEO real GDP growth (NGDP_RPCH)...")
        df_imf_gdp = fetch_imf_gdp_growth_weo(
            iso3_list=iso3,
            start_year=1990,
            end_year=2025,
        )
        progress.progress(25)

        # 3) IMF DataMapper: WEO CPI inflation (PCPIPCH)
        status_text.text("Fetching IMF WEO CPI inflation (PCPIPCH)...")
        df_imf_cpi = fetch_imf_cpi_inflation_weo(
            iso3_list=iso3,
            start_year=1990,
            end_year=2025,
        )
        progress.progress(40)

        # 4) World Bank: GDP growth (NY.GDP.MKTP.KD.ZG)
        status_text.text("Fetching World Bank GDP growth (NY.GDP.MKTP.KD.ZG)...")
        df_wb_gdp_g = wb_fetch_indicator(
            indicator="NY.GDP.MKTP.KD.ZG",
            iso3_list=iso3,
            start_year=1990,
            end_year=2025,
        )
        progress.progress(55)

        # 5) World Bank: current account balance (% of GDP)
        status_text.text("Fetching World Bank current account (BN.CAB.XOKA.GD.ZS)...")
        df_wb_ca = wb_fetch_indicator(
            indicator="BN.CAB.XOKA.GD.ZS",
            iso3_list=iso3,
            start_year=1990,
            end_year=2025,
        )
        progress.progress(65)

        # 6) World Bank: FDI net inflows (% of GDP)
        status_text.text("Fetching World Bank FDI net inflows (BX.KLT.DINV.WD.GD.ZS)...")
        df_wb_fdi = wb_fetch_indicator(
            indicator="BX.KLT.DINV.WD.GD.ZS",
            iso3_list=iso3,
            start_year=1990,
            end_year=2025,
        )
        progress.progress(75)

        # 7) World Bank: reserves in months of imports
        status_text.text("Fetching World Bank reserves (FI.RES.TOTL.MO)...")
        df_wb_reserves = wb_fetch_indicator(
            indicator="FI.RES.TOTL.MO",
            iso3_list=iso3,
            start_year=1990,
            end_year=2025,
        )
        progress.progress(85)

        # 8) World Bank: government debt (% of GDP)
        status_text.text("Fetching World Bank government debt (GC.DOD.TOTL.GD.ZS)...")
        df_wb_debt = wb_fetch_indicator(
            indicator="GC.DOD.TOTL.GD.ZS",
            iso3_list=iso3,
            start_year=1990,
            end_year=2025,
        )
        progress.progress(95)

        # 9) Build Excel workbook in memory
        status_text.text("Writing data to Excel workbook...")
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            countries.to_excel(writer, sheet_name="countries", index=False)
            df_imf_gdp.to_excel(writer, sheet_name="imf_weo_gdp_growth", index=False)
            df_imf_cpi.to_excel(writer, sheet_name="imf_weo_cpi_inflation", index=False)
            df_wb_gdp_g.to_excel(writer, sheet_name="wb_gdp_growth", index=False)
            df_wb_ca.to_excel(writer, sheet_name="wb_current_account", index=False)
            df_wb_fdi.to_excel(writer, sheet_name="wb_fdi", index=False)
            df_wb_reserves.to_excel(writer, sheet_name="wb_reserves", index=False)
            df_wb_debt.to_excel(writer, sheet_name="wb_debt", index=False)

        output.seek(0)
        progress.progress(100)

        st.success("Data pull complete.")
        st.download_button(
            label="Download macro_data_panel.xlsx",
            data=output,
            file_name="macro_data_panel.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

        st.subheader("IMF WEO GDP growth sample (first 10 rows)")
        st.dataframe(df_imf_gdp.head(10))

    except Exception as e:
        status_text.text("Error during data fetch.")
        progress.progress(0)
        st.error(f"Error: {e}")
