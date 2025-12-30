import pandas as pd
from datetime import datetime


def build_dim_customer(
    current_df: pd.DataFrame,
    updates_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build SCD Type 2 customer dimension.
    """

    current_df = current_df.copy()
    updates_df = updates_df.copy()

    # Add SCD metadata to current snapshot
    current_df["effective_start_date"] = current_df["created_at"]
    current_df["effective_end_date"] = pd.NaT
    current_df["is_current"] = True

    dim_rows = []

    for _, upd in updates_df.iterrows():
        customer_id = upd["customer_id"]

        # Find current record
        mask = (current_df["customer_id"] == customer_id) & (current_df["is_current"])
        if not mask.any():
            continue

        current_row = current_df.loc[mask].iloc[0]

        # Detect real change
        changed = (
            current_row["email"] != upd["email"]
            or current_row["loyalty_tier"] != upd["loyalty_tier"]
        )

        if changed:
            # Expire old record
            current_df.loc[mask, "effective_end_date"] = upd["updated_at"]
            current_df.loc[mask, "is_current"] = False

            # Insert new record
            new_row = current_row.copy()
            new_row["email"] = upd["email"]
            new_row["loyalty_tier"] = upd["loyalty_tier"]
            new_row["effective_start_date"] = upd["updated_at"]
            new_row["effective_end_date"] = pd.NaT
            new_row["is_current"] = True

            dim_rows.append(new_row)

    dim_customer = pd.concat(
        [current_df, pd.DataFrame(dim_rows)], ignore_index=True
    )

    return dim_customer
