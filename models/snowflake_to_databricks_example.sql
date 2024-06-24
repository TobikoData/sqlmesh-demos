MODEL (
  name "demo"."snowflake_to_databricks_example",
  cron '@daily',
  grain order_id,
  audits (UNIQUE_VALUES(columns = (
    order_id
  )), NOT_NULL(columns = (
    order_id
  )))
);

select
   order_id,
   customer_id,
   order_date,
   status,
   credit_card_amount::float as credit_card_amounts, --SQLMesh will transpile snowflake shorthand cast syntax to Spark SQL
   coupon_amount::float as coupon_amount,
   bank_transfer_amount::float as bank_transfer_amount,
   gift_card_amount::float as gift_card_amount,
   amount,
   NVL(amount, 0) AS coalesce_amount, --snowflake function transpiles to Spark equivalent "COALESCE"
   IFF(amount > 20, 'High', 'Low') AS order_level, --snowflake function transpiles to Spark equivalent "IF"
from
   demo.orders

-- sqlmesh render demo.snowflake_to_databricks_example
-- sqlmesh create_test demo.snowflake_to_databricks_example --query demo.orders "select * from demo.orders limit 5" 