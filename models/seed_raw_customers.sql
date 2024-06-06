MODEL (
  name tcloud_demo.seed_raw_customers,
  kind SEED (
    path '../seeds/raw_customers.csv'
  ),
  columns (
    id INT64,
    first_name STRING(50),
    last_name STRING(50)
  ),
  grain (
    id
  )
)