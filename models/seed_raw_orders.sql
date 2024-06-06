MODEL (
  name tcloud_demo.seed_raw_orders,
  kind SEED (
    path '../seeds/raw_orders.csv'
  ),
  columns (
    id INT64,
    user_id INT64,
    order_date DATE,
    status STRING(50)
  ),
  grain (id, user_id)
)