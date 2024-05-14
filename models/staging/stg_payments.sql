MODEL (
  name demo.stg_payments,
  cron '@daily',
  grain payment_id,
  audits (UNIQUE_VALUES(columns = (
    payment_id
  )), NOT_NULL(columns = (
    payment_id
  )))
);

SELECT
  id AS payment_id,
  order_id,
  payment_method,
  amount / 100 AS amount, /* `amount` is currently stored in cents, so we convert it to dollars */
  'new_column' AS new_column, /* non-breaking change example  */
  'another_column' AS another_column
FROM demo.seed_raw_payments

-- how to generate unit test code without manually writing yaml by hand
-- this will generate a file in the tests/ folder: test_stg_payments.yaml
-- sqlmesh create_test demo.stg_payments --query demo.seed_raw_payments "select * from demo.seed_raw_payments limit 5" 