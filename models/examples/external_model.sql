MODEL (
  name tcloud_demo.external_model
);

/* run this to create the schema file from the table's metadata: sqlmesh create_external_models */
SELECT
  event_date,
  event_timestamp,
  event_name,
  event_params,
  event_previous_timestamp,
  event_value_in_usd,
  event_bundle_sequence_id,
  event_server_timestamp_offset,
  user_id,
  user_pseudo_id,
  privacy_info,
  user_properties,
  user_first_touch_timestamp,
  user_ltv,
  device,
  geo,
  app_info,
  traffic_source,
  stream_id,
  platform,
  event_dimensions,
  ecommerce
/*   items */
FROM bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210131