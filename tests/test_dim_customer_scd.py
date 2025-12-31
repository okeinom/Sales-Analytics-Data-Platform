import pandas as pd

from sales_analytics.transform.dim_customer import build_dim_customer


def test_scd2_creates_new_version_and_expires_old():
    # Arrange: current snapshot (staged-style types)
    customers = pd.DataFrame(
        [
            {
                "customer_id": 1,
                "full_name": "Jane Doe",
                "email": "jane@example.com",
                "loyalty_tier": "Silver",
                "created_at": pd.Timestamp("2025-01-01 10:00:00"),
            }
        ]
    )

    # Arrange: one update that changes tracked attributes
    updates = pd.DataFrame(
        [
            {
                "customer_id": 1,
                "full_name": "Jane Doe",
                "email": "jane.new@example.com",
                "loyalty_tier": "Gold",
                "updated_at": pd.Timestamp("2025-02-01 12:00:00"),
            }
        ]
    )

    # Act
    dim = build_dim_customer(customers, updates)

    # Assert: two versions exist for the same business key
    rows = dim[dim["customer_id"] == 1].sort_values("effective_start_date")
    assert len(rows) == 2

    old = rows.iloc[0]
    new = rows.iloc[1]

    # Old record expired
    assert bool(old["is_current"]) is False
    assert pd.notna(old["effective_end_date"])
    assert old["effective_end_date"] == pd.Timestamp("2025-02-01 12:00:00")

    # New record is current and starts at update time
    assert bool(new["is_current"]) is True
    assert pd.isna(new["effective_end_date"])
    assert new["effective_start_date"] == pd.Timestamp("2025-02-01 12:00:00")

    # Sanity: exactly one current row per customer_id
    assert dim[dim["customer_id"] == 1]["is_current"].sum() == 1
