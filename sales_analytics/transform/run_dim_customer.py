from sales_analytics.ingest.load_raw import load_all_raw_data
from sales_analytics.transform.staging import (
    stage_customers,
    stage_customer_updates,
)
from sales_analytics.transform.dim_customer import build_dim_customer


def run():
    raw = load_all_raw_data()
    #print(raw.keys())
    customers = stage_customers(raw["customers"])
    updates = stage_customer_updates(raw["customers_updates"])

    dim_customer = build_dim_customer(customers, updates)

    print(dim_customer.head())
    print(dim_customer["is_current"].value_counts())


if __name__ == "__main__":
    run()
