MODEL (
  name tcloud_demo.full_model,
  kind FULL,
  cron '@daily',
  grain item_id,
  audits (
    assert_positive_order_ids
  )
);

-- command to generate unit test code in a yaml file
-- sqlmesh create_test tcloud_demo.incremental_model --query tcloud_demo.seed_model "select * from tcloud_demo.seed_model limit 5" 

SELECT
  item_id,
  COUNT(DISTINCT id) AS num_orders
FROM tcloud_demo.incremental_model
GROUP BY
  item_id