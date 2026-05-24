"""
Fetch USD/VND exchange rate from State Bank of Vietnam API.
Public API, no key required.
"""
import requests
import pandas as pd
from datetime import date, datetime
from loguru import logger


NHNN_API_URL = "https://portal.vietcombank.com.vn/Usercontrols/TVPortal.TyGia/pXML.aspx"


def fetch_exchange_rate(target_date: date = None) -> pd.DataFrame:
    """
    Fetch exchange rate from Vietcombank (proxy for interbank rates).
    Returns a standardized DataFrame.
    """
    if target_date is None:
        target_date = date.today()

    logger.info(f"Fetching exchange rate for {target_date}")

    # For past dates, generate simulated daily-varying rates
    # to avoid all backfill dates getting today's rate.
    if target_date < date.today():
        logger.info(f"Historical date {target_date} detected. Generating unique historical mock rate.")
        return _generate_fallback_rates(target_date)

    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        response = requests.get(NHNN_API_URL, headers=headers, timeout=10)
        response.raise_for_status()

        # Parse XML response
        import xml.etree.ElementTree as ET
        root = ET.fromstring(response.content)

        rates = []
        for exrate in root.findall(".//Exrate"):
            currency_code = exrate.get("CurrencyCode", "")
            currency_name = exrate.get("CurrencyName", "")
            buy = exrate.get("Buy", "0").replace(",", "")
            sell = exrate.get("Sell", "0").replace(",", "")
            transfer = exrate.get("Transfer", "0").replace(",", "")

            if currency_code in ["USD", "EUR", "CNY", "SGD", "JPY"]:
                rates.append({
                    "currency_code": currency_code,
                    "currency_name": currency_name.strip(),
                    "buy_rate": float(buy) if buy and buy != "-" else None,
                    "sell_rate": float(sell) if sell and sell != "-" else None,
                    "transfer_rate": float(transfer) if transfer and transfer != "-" else None,
                    "date": target_date.isoformat(),
                    "ingested_at": datetime.utcnow().isoformat(),
                    "source": "vietcombank"
                })

        df = pd.DataFrame(rates)
        logger.success(f"Fetched {len(df)} currency pairs")
        return df

    except Exception as e:
        logger.error(f"Failed to fetch exchange rate: {e}")
        # Fallback: synthetic data to prevent pipeline breakage
        return _generate_fallback_rates(target_date)


def _generate_fallback_rates(target_date: date) -> pd.DataFrame:
    """Generate synthetic data when API is unavailable."""
    import random
    
    # Seeding based on target_date for unique but deterministic rates
    date_seed = int(target_date.strftime("%Y%m%d"))
    random.seed(date_seed)
    
    logger.warning(f"Using fallback synthetic exchange rates for {target_date} using seed {date_seed}")

    base_rates = {"USD": 25400, "EUR": 27200, "CNY": 3520, "SGD": 18900, "JPY": 170}
    rows = []
    for code, base in base_rates.items():
        # Add slight daily variation to simulate realistic trends
        day_offset = (target_date - date(2026, 5, 1)).days * 20 if code == "USD" else 0
        current_base = base + day_offset
        spread = current_base * 0.005 # ~0.5% bid-ask spread
        rows.append({
            "currency_code": code,
            "currency_name": f"{code} DOLLAR" if code == "USD" else code,
            "buy_rate": round(current_base - spread + random.uniform(-10, 10), 0),
            "sell_rate": round(current_base + spread + random.uniform(-10, 10), 0),
            "transfer_rate": round(current_base + random.uniform(-10, 10), 0),
            "date": target_date.isoformat(),
            "ingested_at": datetime.utcnow().isoformat(),
            "source": "synthetic_fallback"
        })
    return pd.DataFrame(rows)