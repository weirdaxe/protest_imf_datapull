import requests

BASE = "https://dataservices.imf.org/REST/SDMX_JSON.svc"

def test_imf_ifx_single_country():
    # Example from recent public usage: monthly USD FX rate (IFS) for Netherlands
    url = (
        f"{BASE}/CompactData/IFS/M.NL.ENDA_XDC_USD_RATE"
        "?startPeriod=2015-01&endPeriod=2018-12"
    )
    print("Testing IMF IFS (NL, monthly USD FX)...")
    print("URL:", url)

    resp = requests.get(url, timeout=10)

    print("Status:", resp.status_code)
    print("Content-Type:", resp.headers.get("content-type"))
    print("First 1000 chars of body:")
    print(resp.text[:1000])

if __name__ == "__main__":
    test_imf_ifx_single_country()
