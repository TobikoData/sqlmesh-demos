MODEL (
  name demo.stg_orders,
  cron '@daily',
  grain order_id,
  audits (UNIQUE_VALUES(columns = (
    order_id
  )), NOT_NULL(columns = (
    order_id
  )))
);

SELECT
  id AS order_id,
  user_id AS customer_id,
  order_date,
  status
FROM demo.seed_raw_orders