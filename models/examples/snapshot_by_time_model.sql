MODEL (
  name tcloud_demo.snapshot_by_time_model,
  kind SCD_TYPE_2_BY_TIME (
    unique_key id,
    updated_at_name event_date
  )
);

SELECT
  id::INT,
  item_id::INT,
  event_date::DATE
FROM
  tcloud_demo.seed_model