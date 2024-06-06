MODEL (
  name tcloud_demo.seed_raw_payments,
  kind SEED (
    path '../seeds/raw_payments.csv'
  ),
  columns (
    id INT64,
    order_id INT64,
    payment_method STRING(50),
    amount INT64
  ),
  grain (id, user_id)
)