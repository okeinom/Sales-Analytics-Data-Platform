from __future__ import annotations

import os
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def set_seed(seed: int = 42) -> None:
    random.seed(seed)


def gen_customers(fake: Faker, n: int = 500) -> pd.DataFrame:
    tiers = ["Bronze", "Silver", "Gold", "Platinum"]
    rows = []
    base_date = datetime.now() - timedelta(days=365)

    for cid in range(1, n + 1):
        created = base_date + timedelta(days=random.randint(0, 330))
        rows.append(
            {
                "customer_id": cid,
                "full_name": fake.name(),
                "email": fake.email(),
                "loyalty_tier": random.choices(tiers, weights=[45, 30, 20, 5], k=1)[0],
                "created_at": created.isoformat(timespec="seconds"),
            }
        )
    return pd.DataFrame(rows)


def gen_customer_updates(customers: pd.DataFrame, update_rate: float = 0.18) -> pd.DataFrame:
    """
    Create a later snapshot where some customers change attributes.
    This file is the input that will drive SCD Type 2 in your pipeline.
    """
    rows = []
    change_count = max(1, int(len(customers) * update_rate))

    chosen_ids = random.sample(customers["customer_id"].tolist(), change_count)
    tiers = ["Bronze", "Silver", "Gold", "Platinum"]

    for cid in chosen_ids:
        row = customers.loc[customers["customer_id"] == cid].iloc[0].to_dict()
        # Simulate a later update time
        updated_at = datetime.fromisoformat(row["created_at"]) + timedelta(days=random.randint(30, 240))
        # Change 1–2 attributes
        if random.random() < 0.70:
            row["email"] = f"{row['full_name'].split()[0].lower()}.{cid}@example.com"
        if random.random() < 0.65:
            # move tier up/down sometimes
            row["loyalty_tier"] = random.choice(tiers)

        row["updated_at"] = updated_at.isoformat(timespec="seconds")
        rows.append(row)

    # keep only columns relevant for the updates file (plus updated_at)
    df = pd.DataFrame(rows)[["customer_id", "full_name", "email", "loyalty_tier", "updated_at"]]
    return df


def gen_products(fake: Faker, n: int = 200) -> pd.DataFrame:
    categories = ["Electronics", "Home", "Grocery", "Clothing", "Beauty", "Sports"]
    rows = []
    for pid in range(1, n + 1):
        rows.append(
            {
                "product_id": pid,
                "product_name": fake.catch_phrase(),
                "category": random.choice(categories),
                "unit_price": round(random.uniform(3.5, 450.0), 2),
            }
        )
    return pd.DataFrame(rows)


def gen_stores(fake: Faker, n: int = 20) -> pd.DataFrame:
    regions = ["Ontario", "Quebec", "Prairies", "BC", "Atlantic"]
    rows = []
    for sid in range(1, n + 1):
        rows.append(
            {
                "store_id": sid,
                "store_name": f"{fake.city()} Store",
                "region": random.choice(regions),
            }
        )
    return pd.DataFrame(rows)


def gen_sales(customers: pd.DataFrame, products: pd.DataFrame, stores: pd.DataFrame, n: int = 50_000) -> pd.DataFrame:
    rows = []
    start = datetime.now() - timedelta(days=180)

    customer_ids = customers["customer_id"].tolist()
    product_ids = products["product_id"].tolist()
    store_ids = stores["store_id"].tolist()

    # price lookup for consistent revenue math
    price_map = dict(zip(products["product_id"], products["unit_price"]))

    for sid in range(1, n + 1):
        pid = random.choice(product_ids)
        qty = random.randint(1, 5)
        ts = start + timedelta(minutes=random.randint(0, 180 * 24 * 60))

        revenue = round(price_map[pid] * qty, 2)

        rows.append(
            {
                "sale_id": sid,
                "customer_id": random.choice(customer_ids),
                "product_id": pid,
                "store_id": random.choice(store_ids),
                "sale_timestamp": ts.isoformat(timespec="seconds"),
                "quantity": qty,
                "revenue": revenue,
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    set_seed(42)
    fake = Faker()
    Faker.seed(42)

    out_dir = os.path.join("data", "raw")
    ensure_dir(out_dir)

    customers = gen_customers(fake, n=500)
    products = gen_products(fake, n=200)
    stores = gen_stores(fake, n=20)
    sales = gen_sales(customers, products, stores, n=50_000)
    customer_updates = gen_customer_updates(customers, update_rate=0.18)

    customers.to_csv(os.path.join(out_dir, "customers.csv"), index=False)
    customer_updates.to_csv(os.path.join(out_dir, "customers_updates.csv"), index=False)
    products.to_csv(os.path.join(out_dir, "products.csv"), index=False)
    stores.to_csv(os.path.join(out_dir, "stores.csv"), index=False)
    sales.to_csv(os.path.join(out_dir, "sales.csv"), index=False)

    print("✅ Generated:")
    for f in ["customers.csv", "customers_updates.csv", "products.csv", "stores.csv", "sales.csv"]:
        print(f" - {os.path.join(out_dir, f)}")


if __name__ == "__main__":
    main()
