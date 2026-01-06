import dlt

@dlt.table
def factStream_stg():
  df= spark.readStream.table("spotify_cata.silver.factstream")
  return df

dlt.create_streaming_table("factstream")

dlt.create_auto_cdc_flow(
  target = "factstream",
  source = "factstream_stg",
  keys = ["stream_id"],
  sequence_by = "stream_timestamp",
  stored_as_scd_type = 1,
  track_history_column_list = None,
  track_history_except_column_list = None,
  name = None,
  once = False
)
