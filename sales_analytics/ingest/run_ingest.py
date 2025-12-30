from sales_analytics.ingest.load_raw import load_all_raw_data
from sales_analytics.transform.staging import (
    stage_customer_updates,
    stage_customers,        
    stage_sales,
)

def run():
    """
    Run the data ingestion and staging process.
    """
    # Load all raw data
    raw_data = load_all_raw_data()

    # Stage each dataset
    staged_data = {}
    if "customers.csv" in raw_data:
        staged_data["customers"] = stage_customers(raw_data["customers"])
    if "customers_updates.csv" in raw_data:
        staged_data["customers_updates"] = stage_customer_updates(raw_data["customers_updates"])
    if "sales.csv" in raw_data:
        staged_data["sales"] = stage_sales(raw_data["sales"])

    return staged_data


if __name__ == "__main__":
    staged_datasets = run()
    for name, df in staged_datasets.items():
        print(f"Staged data for {name}:")
        print(df.shape)