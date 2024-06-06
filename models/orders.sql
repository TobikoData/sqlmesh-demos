MODEL (
  name tcloud_demo.orders,
  cron '@daily',
  grain order_id,
  audits (UNIQUE_VALUES(columns = (
    surrogate_key
  )), NOT_NULL(columns = (
    surrogate_key
  )))
);

@DEF(payment_methods, ['credit_card', 'coupon', 'bank_transfer', 'gift_card']);

WITH orders AS (
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
), order_payments AS (
  SELECT
    order_id,
    @EACH(
      @payment_methods,
      x -> SUM(CASE WHEN payment_method = x THEN amount ELSE 0 END) AS @{x}_amount
    ),
    SUM(amount) AS total_amount
  FROM payments
  GROUP BY
    order_id
), final AS (
  SELECT
    @gen_surrogate_key([orders.order_id, orders.customer_id]) AS surrogate_key, /* custom macro example */
    orders.order_id,
    orders.customer_id,
    orders.order_date,
    orders.status,
    @EACH(@payment_methods, x -> order_payments.@{x}_amount),
    order_payments.total_amount AS amount
  FROM orders
  LEFT JOIN order_payments
    ON orders.order_id = order_payments.order_id
)
SELECT
  *
FROM final