import os
from dotenv import load_dotenv

from sales_analytics.ingest.load_raw import load_all_raw_data
from sales_analytics.transform.staging import (
    stage_customers,
    stage_customer_updates,
    stage_sales,
)
from sales_analytics.transform.dim_customer import build_dim_customer
from sales_analytics.transform.fact_sales import build_fact_sales
from sales_analytics.load.db_loader import get_engine, load_df


def run():
    load_dotenv()
    engine = get_engine()

    raw = load_all_raw_data()

    customers = stage_customers(raw["customers"])
    updates = stage_customer_updates(raw["customers_updates"])
    sales = stage_sales(raw["sales"])

    dim_customer = build_dim_customer(customers, updates)
    fact_sales = build_fact_sales(sales)

    load_df(dim_customer, "dim_customer", engine)
    load_df(fact_sales, "fact_sales", engine)

    print("✅ Tables loaded to database")


if __name__ == "__main__":
    run()
