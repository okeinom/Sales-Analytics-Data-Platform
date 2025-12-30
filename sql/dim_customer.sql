-- dim_customer_scd.sql
-- SCD Type 2 implementation for customer dimension

-- 1. Expire current records when a tracked attribute changes
UPDATE dim_customer dc
SET
    effective_end_date = scu.updated_at,
    is_current = FALSE
FROM stg_customer_updates scu
WHERE dc.customer_id = scu.customer_id
  AND dc.is_current = TRUE
  AND (
        dc.email <> scu.email
     OR dc.loyalty_tier <> scu.loyalty_tier
  );

-- 2. Insert new version of changed customers
INSERT INTO dim_customer (
    customer_id,
    full_name,
    email,
    loyalty_tier,
    effective_start_date,
    effective_end_date,
    is_current
)
SELECT
    scu.customer_id,
    scu.full_name,
    scu.email,
    scu.loyalty_tier,
    scu.updated_at        AS effective_start_date,
    NULL                  AS effective_end_date,
    TRUE                  AS is_current
FROM stg_customer_updates scu
LEFT JOIN dim_customer dc
  ON dc.customer_id = scu.customer_id
 AND dc.is_current = TRUE
WHERE
    dc.customer_id IS NULL
    OR dc.email <> scu.email
    OR dc.loyalty_tier <> scu.loyalty_tier;
