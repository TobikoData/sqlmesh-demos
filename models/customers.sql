MODEL (
  name tcloud_demo.customers,
  cron '@daily',
  grain customer_id,
  audits (UNIQUE_VALUES(columns = (
    customer_id
  )), NOT_NULL(columns = (
    customer_id
  )))
);

WITH customers AS (
  SELECT
    *
  FROM tcloud_demo.stg_customers
), orders AS (
  SELECT
    *
  FROM tcloud_demo.stg_orders
), payments AS (
  SELECT
    payment_id,
    order_id,
    payment_method,
    amount
  FROM tcloud_demo.stg_payments
), customer_orders AS (
  SELECT
    customer_id,
    MIN(order_date) AS first_order,
    MAX(order_date) AS most_recent_order,
    COUNT(order_id) AS number_of_orders
  FROM orders
  GROUP BY
    customer_id
), customer_payments AS (
  SELECT
    orders.customer_id,
    SUM(amount) AS total_amount
  FROM payments
  LEFT JOIN orders
    ON payments.order_id = orders.order_id
  GROUP BY
    orders.customer_id
), final AS (
  SELECT
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    customer_orders.first_order,
    customer_orders.most_recent_order,
    customer_orders.number_of_orders,
    customer_payments.total_amount AS customer_lifetime_value
  FROM customers
  LEFT JOIN customer_orders
    ON customers.customer_id = customer_orders.customer_id
  LEFT JOIN customer_payments
    ON customers.customer_id = customer_payments.customer_id
)
SELECT
  *
FROM final

-- create a unit test from this SQL model
-- sqlmesh create_test tcloud_demo.customers --query tcloud_demo.stg_customers "select * from tcloud_demo.stg_customers limit 5" \
-- --query tcloud_demo.stg_orders "select * from tcloud_demo.stg_orders limit 5" \
-- --query tcloud_demo.stg_payments "select * from tcloud_demo.stg_payments limit 5"