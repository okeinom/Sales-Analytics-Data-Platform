import pandas as pd


def build_fact_sales(sales_df: pd.DataFrame) -> pd.DataFrame:
    df = sales_df.copy()

    # In real systems, surrogate keys would be resolved here
    fact_sales = df[
        [
            "sale_id",
            "customer_id",
            "product_id",
            "store_id",
            "sale_timestamp",
            "quantity",
            "revenue",
        ]
    ]

    return fact_sales
