MODEL (
  name tcloud_demo.incremental_forward_only_model,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column event_date,
    forward_only true -- All changes will be forward only
  ),
  start '2020-01-01',
  cron '*/5 * * * *',
  grain (id, event_date),
);

-- How to work with forward only models
-- step 1: `sqlmesh plan dev` to create this model for the first time and backfill for all of history
-- step 2: add a new column
-- step 3: pick a start date to backfill like: '2020-01-07'
-- step 4: validate only a portion of rows were backfilled: `sqlmesh fetchdf "select * from demo__dev.incremental_forward_only_model"`
-- step 5: `sqlmesh plan` to promote to prod with a virtual update, note: the dev backfill preview won't be reused for promotion and is only for dev purposes
-- step 6: `sqlmesh plan --restate-model "tcloud_demo.incremental_forward_only_model"`, to invoke a backfill to mirror dev's data preview
-- step 7: pick the same backfill start date for prod as dev's above: '2020-01-07'
-- step 8: validate changes to prod: `sqlmesh fetchdf "select * from tcloud_demo.incremental_forward_only_model"`

SELECT
  id,
  item_id,
  event_date,
  'new_column' as new_column,
  'new_column_v3' as new_column_v3
FROM tcloud_demo.seed_model
WHERE
  event_date BETWEEN @start_date AND @end_date
