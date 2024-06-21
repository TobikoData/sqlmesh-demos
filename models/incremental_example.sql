MODEL (
  name demo.incremental_example,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_timestamp,
    on_destructive_change error,
    lookback 2, -- handle late arriving events for the past 2 (2*1) days based on cron interval
    forward_only true -- All changes will be forward only
  ),
  start '2024-06-17',
  cron '@daily',
  grain event_id,
  audits (UNIQUE_VALUES(columns = ( -- data audit tests only run for the evaluated intervals
    event_id
  )), NOT_NULL(columns = (
    event_id
  )))
);

-- How to work with incremental forward only models
-- step 1: `sqlmesh plan dev` to create this model for the first time and backfill for all of history
-- step 2: add a new column
-- step 3: pick a start date to backfill like: '2024-06-18'
-- step 4: validate only a portion of rows were backfilled: `sqlmesh fetchdf "select * from demo__dev_sung.incremental_example"`
-- step 5: `sqlmesh plan` to promote to prod with a virtual update, note: the dev backfill preview won't be reused for promotion and is only for dev purposes
-- step 6: `sqlmesh plan --restate-model "demo.incremental_example"`, to invoke a backfill to mirror dev's data preview
-- step 7: pick the same backfill start date for prod as dev's above: '2024-06-18'
-- step 8: validate changes to prod: `sqlmesh fetchdf "select * from demo.incremental_example"`

SELECT
  event_id,
  event_name,
  event_timestamp,
  user_id,
  IF(event_name = 'video_view', 'high', 'low') AS user_intent_level, --
FROM public_demo.raw_data.demo_events --external model, automatically generate yaml using command: `sqlmesh create_external_models` 
WHERE
  event_timestamp BETWEEN @start_date AND @end_date