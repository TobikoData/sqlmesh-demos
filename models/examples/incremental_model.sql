MODEL (
  name tcloud_demo.incremental_model,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date
  ),
  start '2020-01-01',
  cron '@daily',
  grain (id, event_date),
  stamp 'fix hydration issue'
);

SELECT
  id,
  item_id,
  event_date
FROM tcloud_demo.seed_model
WHERE
  event_date BETWEEN @start_date AND @end_date