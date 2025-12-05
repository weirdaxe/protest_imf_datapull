import io
import streamlit as st
import pandas as pd

from macro_data_api import (
    build_country_table,
    fetch_imf_series,
    wb_fetch_indicator,
    imf_compact_quick_test,
    sdmxcentral_agencyscheme_test,
)


def main():
    st.title("Macro data pull shell (IMF + World Bank)")

    # ---- Sidebar options ----
    st.sidebar.header("Run options")

    test_subset = st.sidebar.checkbox(
        "Use small country subset (test mode)", value=True
    )
    show_raw_imf = st.sidebar.checkbox(
        "Show raw IMF JSON for main pull", value=False
    )

    st.sidebar.markdown("---")
    st.sidebar.subheader("IMF connectivity / debug")

    run_imf_small_test = st.sidebar.checkbox(
        "Run tiny IMF CPI test first", value=True
    )
    run_sdmxcentral_test = st.sidebar.checkbox(
        "Run IMF SDMX Central structure test", value=True
    )

    st.sidebar.markdown("---")
    filename = st.sidebar.text_input(
        "Output Excel filename",
        value="macro_panel.xlsx",
    )

    run_button = st.button("Run tests and pull data")

    if not run_button:
        return

    # ---- Connectivity / debug tests ----
    if run_sdmxcentral_test:
        st.subheader("SDMX Central connectivity test")
        with st.spinner("Calling IMF SDMX Central..."):
            try:
                xml = sdmxcentral_agencyscheme_test()
                st.success("SDMX Central call succeeded.")
                st.write("First 2000 characters of response:")
                st.code(xml[:2000])
            except Exception as exc:
                st.error(f"SDMX Central test failed: {exc}")

    if run_imf_small_test:
        st.subheader("IMF CompactData tiny CPI test (dataservices.imf.org)")
        with st.spinner("Calling IMF CompactData for a tiny CPI slice..."):
            try:
                # reuse the quick test, but capture printed output by re-running
                # through fetch_imf_series directly so we can show raw JSON
                df_test, raw = fetch_imf_series(
                    dataset="CPI",
                    freq="M",
                    indicator="PCPI_IX",
                    countries_iso2=["MX"],
                    start_period="2020-01",
                    end_period="2021-12",
                    return_raw_json=True,
                )
                st.success(
                    f"IMF tiny CPI test succeeded: {len(df_test)} rows."
                )
                st.write("Parsed head:")
                st.dataframe(df_test.head())
                st.write("Raw IMF JSON (first 2000 chars):")
                st.code(raw[:2000])
            except Exception as exc:
                st.error(f"IMF tiny CPI test failed: {exc}")

    st.markdown("---")
    st.subheader("Main macro data pull")

    progress = st.progress(0)
    status = st.empty()

    # ---- Countries ----
    status.text("Building country table...")
    countries = build_country_table()
    countries = countries[countries["iso2"].notna() & countries["iso3"].notna()]

    if test_subset:
        # Small EM sample for quick test runs
        sample_iso2 = ["AR", "BR", "ZA"]
        mask = countries["iso2"].isin(sample_iso2)
        countries = countries[mask]

    iso2 = countries["iso2"].tolist()
    iso3 = countries["iso3"].tolist()

    if not iso2 or not iso3:
        st.error("No valid ISO country codes after filtering.")
        return

    n_steps = 8  # 3 IMF + 5 WB
    step = 0

    # ---- IMF: CPI ----
    step += 1
    status.text("Fetching IMF CPI (monthly index)...")
    try:
        if show_raw_imf:
            df_cpi, raw_cpi = fetch_imf_series(
                dataset="CPI",
                freq="M",
                indicator="PCPI_IX",
                countries_iso2=iso2,
                start_period="1990-01",
                end_period="2025-12",
                return_raw_json=True,
            )
            st.expander("IMF CPI raw JSON (first 2000 chars)").code(
                raw_cpi[:2000]
            )
        else:
            df_cpi = fetch_imf_series(
                dataset="CPI",
                freq="M",
                indicator="PCPI_IX",
                countries_iso2=iso2,
                start_period="1990-01",
                end_period="2025-12",
            )
    except Exception as exc:
        st.error(f"IMF CPI pull failed: {exc}")
        return
    progress.progress(step / n_steps)

    # ---- IMF: FX ----
    step += 1
    status.text("Fetching IMF FX (monthly USD rate)...")
    try:
        if show_raw_imf:
            df_fx, raw_fx = fetch_imf_series(
                dataset="ER",
                freq="M",
                indicator="ENDA_XDC_USD_RATE",
                countries_iso2=iso2,
                start_period="1990-01",
                end_period="2025-12",
                return_raw_json=True,
            )
            st.expander("IMF FX raw JSON (first 2000 chars)").code(
                raw_fx[:2000]
            )
        else:
            df_fx = fetch_imf_series(
                dataset="ER",
                freq="M",
                indicator="ENDA_XDC_USD_RATE",
                countries_iso2=iso2,
                start_period="1990-01",
                end_period="2025-12",
            )
    except Exception as exc:
        st.error(f"IMF FX pull failed: {exc}")
        return
    progress.progress(step / n_steps)

    # ---- IMF: quarterly real GDP ----
    step += 1
    status.text("Fetching IMF real GDP (quarterly)...")
    try:
        if show_raw_imf:
            df_qgdp, raw_qgdp = fetch_imf_series(
                dataset="IFS",
                freq="Q",
                indicator="NGDP_R_SA_XDC",
                countries_iso2=iso2,
                start_period="1990",
                end_period="2025",
                return_raw_json=True,
            )
            st.expander("IMF real GDP raw JSON (first 2000 chars)").code(
                raw_qgdp[:2000]
            )
        else:
            df_qgdp = fetch_imf_series(
                dataset="IFS",
                freq="Q",
                indicator="NGDP_R_SA_XDC",
                countries_iso2=iso2,
                start_period="1990",
                end_period="2025",
            )
    except Exception as exc:
        st.error(f"IMF real GDP pull failed: {exc}")
        return
    progress.progress(step / n_steps)

    # ---- World Bank indicators ----
    # GDP growth
    step += 1
    status.text("Fetching WB GDP growth...")
    try:
        df_gdp_g = wb_fetch_indicator(
            indicator="NY.GDP.MKTP.KD.ZG",
            iso3_list=iso3,
            start_year=1990,
            end_year=2025,
        )
    except Exception as exc:
        st.error(f"WB GDP growth pull failed: {exc}")
        return
    progress.progress(step / n_steps)

    # Current account
    step += 1
    status.text("Fetching WB current account (% of GDP)...")
    try:
        df_ca = wb_fetch_indicator(
            indicator="BN.CAB.XOKA.GD.ZS",
            iso3_list=iso3,
            start_year=1990,
            end_year=2025,
        )
    except Exception as exc:
        st.error(f"WB CA pull failed: {exc}")
        return
    progress.progress(step / n_steps)

    # FDI
    step += 1
    status.text("Fetching WB FDI (% of GDP)...")
    try:
        df_fdi = wb_fetch_indicator(
            indicator="BX.KLT.DINV.WD.GD.ZS",
            iso3_list=iso3,
            start_year=1990,
            end_year=2025,
        )
    except Exception as exc:
        st.error(f"WB FDI pull failed: {exc}")
        return
    progress.progress(step / n_steps)

    # Reserves
    step += 1
    status.text("Fetching WB reserves (months of imports)...")
    try:
        df_reserves = wb_fetch_indicator(
            indicator="FI.RES.TOTL.MO",
            iso3_list=iso3,
            start_year=1990,
            end_year=2025,
        )
    except Exception as exc:
        st.error(f"WB reserves pull failed: {exc}")
        return
    progress.progress(step / n_steps)

    # Debt
    step += 1
    status.text("Fetching WB gov. debt (% of GDP)...")
    try:
        df_debt = wb_fetch_indicator(
            indicator="GC.DOD.TOTL.GD.ZS",
            iso3_list=iso3,
            start_year=1990,
            end_year=2025,
        )
    except Exception as exc:
        st.error(f"WB debt pull failed: {exc}")
        return
    progress.progress(step / n_steps)

    status.text("Assembling Excel file...")

    # ---- Build Excel in memory ----
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_cpi.to_excel(writer, sheet_name="IMF_CPI", index=False)
        df_fx.to_excel(writer, sheet_name="IMF_FX", index=False)
        df_qgdp.to_excel(writer, sheet_name="IMF_QGDP", index=False)
        df_gdp_g.to_excel(writer, sheet_name="WB_GDP_G", index=False)
        df_ca.to_excel(writer, sheet_name="WB_CA", index=False)
        df_fdi.to_excel(writer, sheet_name="WB_FDI", index=False)
        df_reserves.to_excel(writer, sheet_name="WB_RESERVES", index=False)
        df_debt.to_excel(writer, sheet_name="WB_DEBT", index=False)

    output.seek(0)

    st.success("Done. Download your Excel file below.")
    st.download_button(
        label="Download Excel file",
        data=output,
        file_name=filename or "macro_panel.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


if __name__ == "__main__":
    main()
