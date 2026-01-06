import dlt

expectations={
  "rule1":"user_id IS NOT NULL"
}

@dlt.table
@dlt.expect_all_or_drop(expectations)
def dimuser_stg():
  df= spark.readStream.table("spotify_cata.silver.dimuser")
  return df

dlt.create_streaming_table(
  name="dimuser",
  expect_all_or_drop=expectations
  )

dlt.create_auto_cdc_flow(
  target = "dimuser",
  source = "dimuser_stg",
  keys = ["user_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = None,
  once = False
)
