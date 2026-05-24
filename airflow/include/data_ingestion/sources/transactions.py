"""
Generate realistic synthetic e-commerce transactions for the Vietnam market.
Uses Faker + custom logic for natural distribution.
"""
import random
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from faker import Faker
from loguru import logger

fake = Faker("vi_VN")

# ---- Business logic for VN market ----

PLATFORMS = {
    "shopee": 0.45,
    "tiki": 0.20,
    "lazada": 0.18,
    "sendo": 0.08,
    "other": 0.09,
}

CATEGORIES = {
    "Điện tử": {"weight": 0.18, "avg_price": 3_500_000, "std": 2_000_000},
    "Thời trang": {"weight": 0.22, "avg_price": 350_000, "std": 200_000},
    "Mỹ phẩm": {"weight": 0.15, "avg_price": 280_000, "std": 150_000},
    "Thực phẩm": {"weight": 0.12, "avg_price": 120_000, "std": 60_000},
    "Gia dụng": {"weight": 0.13, "avg_price": 800_000, "std": 500_000},
    "Sách": {"weight": 0.05, "avg_price": 95_000, "std": 40_000},
    "Thể thao": {"weight": 0.08, "avg_price": 450_000, "std": 250_000},
    "Đồ chơi trẻ em": {"weight": 0.07, "avg_price": 320_000, "std": 180_000},
}

PAYMENT_METHODS = {
    "COD": 0.38,         # Cash on delivery still popular in VN
    "Momo": 0.22,
    "VNPay": 0.18,
    "Bank transfer": 0.12,
    "Credit card": 0.07,
    "ZaloPay": 0.03,
}

STATUS_FLOW = {
    "completed": 0.72,
    "cancelled": 0.12,
    "returned": 0.08,
    "pending": 0.05,
    "processing": 0.03,
}

CITIES = {
    "Hồ Chí Minh": 0.35,
    "Hà Nội": 0.28,
    "Đà Nẵng": 0.08,
    "Cần Thơ": 0.05,
    "Hải Phòng": 0.05,
    "Bình Dương": 0.04,
    "Đồng Nai": 0.03,
    "Other": 0.12,
}


def _weighted_choice(options: dict) -> str:
    keys = list(options.keys())
    weights = list(options.values())
    return random.choices(keys, weights=weights, k=1)[0]


def _generate_price(category_info: dict) -> float:
    """Lognormal distribution for price — more realistic than normal."""
    avg = category_info["avg_price"]
    std = category_info["std"]
    price = np.random.lognormal(
        mean=np.log(avg),
        sigma=std / avg * 0.8
    )
    # Round to nearest 1000 (typical VN pricing)
    return max(10_000, round(price / 1000) * 1000)


def generate_transactions(
    target_date: date = None,
    n_records: int = 500
) -> pd.DataFrame:
    """
    Generate n_records synthetic transactions for a specific date.
    Default 500 records/day — realistic for mid-size platform.
    """
    if target_date is None:
        target_date = date.today()

    # Dynamic seeding based on date for idempotency and distinct daily values
    date_seed = int(target_date.strftime("%Y%m%d"))
    random.seed(date_seed)
    np.random.seed(date_seed)

    logger.info(f"Generating {n_records} transactions for {target_date} using seed {date_seed}")

    # Weekend traffic is ~30% higher
    weekday = target_date.weekday()
    if weekday >= 5:  # Saturday, Sunday
        n_records = int(n_records * 1.3)

    categories = list(CATEGORIES.keys())
    cat_weights = [CATEGORIES[c]["weight"] for c in categories]

    records = []
    for i in range(n_records):
        category = random.choices(categories, weights=cat_weights, k=1)[0]
        cat_info = CATEGORIES[category]

        quantity = random.choices([1, 2, 3, 4, 5], weights=[0.6, 0.2, 0.1, 0.06, 0.04])[0]
        unit_price = _generate_price(cat_info)
        subtotal = unit_price * quantity

        # Discount logic — flash sale, voucher
        discount_pct = random.choices(
            [0, 5, 10, 15, 20, 30, 50],
            weights=[0.40, 0.15, 0.20, 0.10, 0.08, 0.05, 0.02]
        )[0]
        discount_amount = subtotal * discount_pct / 100

        shipping_fee = random.choices(
            [0, 15_000, 20_000, 30_000],
            weights=[0.30, 0.35, 0.25, 0.10]
        )[0]

        total_amount = subtotal - discount_amount + shipping_fee

        # Timestamp: hourly distribution (peaks at 11-13h, 20-22h)
        hour_weights = [
            1, 0.5, 0.3, 0.2, 0.2, 0.3,   # 0-5h
            0.8, 1.5, 2, 2.5, 3, 4,         # 6-11h
            4.5, 3, 2.5, 2, 2, 2.5,         # 12-17h
            3, 4, 4.5, 4, 3, 2              # 18-23h
        ]
        hour = random.choices(range(24), weights=hour_weights)[0]
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        order_ts = datetime(
            target_date.year, target_date.month, target_date.day,
            hour, minute, second
        )

        records.append({
            "transaction_id": f"TXN-{target_date.strftime('%Y%m%d')}-{i+1:05d}",
            "order_timestamp": order_ts.isoformat(),
            "date": target_date.isoformat(),
            "platform": _weighted_choice(PLATFORMS),
            "category": category,
            "product_name": f"{category} product {fake.bothify('??-###')}",
            "customer_id": f"CUST-{random.randint(10000, 99999)}",
            "customer_city": _weighted_choice(CITIES),
            "quantity": quantity,
            "unit_price_vnd": unit_price,
            "subtotal_vnd": subtotal,
            "discount_pct": discount_pct,
            "discount_amount_vnd": discount_amount,
            "shipping_fee_vnd": shipping_fee,
            "total_amount_vnd": round(total_amount),
            "payment_method": _weighted_choice(PAYMENT_METHODS),
            "order_status": _weighted_choice(STATUS_FLOW),
            "ingested_at": datetime.utcnow().isoformat(),
        })

    df = pd.DataFrame(records)
    logger.success(
        f"Generated {len(df)} transactions | "
        f"GMV: {df['total_amount_vnd'].sum():,.0f} VND"
    )
    return df