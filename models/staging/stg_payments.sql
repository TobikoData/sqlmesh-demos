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
  id AS payment_id, --primary key
  order_id, --foreign key
  payment_method, -- ex: credit_card, bank_transfer
  amount / 1000 AS amount, /* `amount` is currently stored in cents, so we convert it to dollars */
  -- 'new_column' AS new_column, /* non-breaking change example  */
FROM demo.seed_raw_payments

-- how to generate unit test code without manually writing yaml by hand
-- this will generate a file in the tests/ folder: test_stg_payments.yaml
-- sqlmesh create_test demo.stg_payments --query demo.seed_raw_payments "select * from demo.seed_raw_payments limit 5" 