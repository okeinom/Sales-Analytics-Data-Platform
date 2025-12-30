import pandas as pd


def stage_customers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["customer_id"] = df["customer_id"].astype(int)
    df["email"] = df["email"].str.lower().str.strip()
    df["created_at"] = pd.to_datetime(df["created_at"])

    return df


def stage_customer_updates(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["customer_id"] = df["customer_id"].astype(int)
    df["email"] = df["email"].str.lower().str.strip()
    df["updated_at"] = pd.to_datetime(df["updated_at"])

    return df


def stage_sales(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["sale_id"] = df["sale_id"].astype(int)
    df["customer_id"] = df["customer_id"].astype(int)
    df["product_id"] = df["product_id"].astype(int)
    df["store_id"] = df["store_id"].astype(int)
    df["sale_timestamp"] = pd.to_datetime(df["sale_timestamp"])
    df["quantity"] = df["quantity"].astype(int)
    df["revenue"] = df["revenue"].astype(float)

    return df
