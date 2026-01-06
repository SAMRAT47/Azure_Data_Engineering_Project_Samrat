# Databricks notebook source
# MAGIC %md
# MAGIC ### DimUser

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

import os
import sys

project_path = os.path.join(os.getcwd(),'..','..')
sys.path.append(project_path)

from utils.transformations import reusable


# COMMAND ----------

# MAGIC %md
# MAGIC ### Autoloader

# COMMAND ----------

df_user=spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","parquet")\
              .option("cloudFiles.schemaLocation","abfss://silver@samratstorageazure.dfs.core.windows.net/DimUser")\
              .load("abfss://bronze@samratstorageazure.dfs.core.windows.net/DimUser")

# COMMAND ----------

from uuid import uuid4

run_id = str(uuid4()).replace("-", "")
view_name = f"df_user_debug_{run_id}"

checkpoint_path = f"abfss://silver@samratstorageazure.dfs.core.windows.net/DimUser/checkpoint/{view_name}"

query = (
    df_user.writeStream
        .format("memory")
        .queryName(view_name)
        .outputMode("append")
        .trigger(once=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
)

query.awaitTermination()

# COMMAND ----------

spark.sql(f"SELECT * FROM {view_name}").display()

# COMMAND ----------

df_user=df_user.withColumn("user_name",upper(col("user_name")))
df_user_obj=reusable()
df_user= df_user_obj.dropColumns(df_user,['_rescued_data'])
df_user= df_user.dropDuplicates(['user_id'])


# COMMAND ----------

df_user.writeStream.format("delta")\
              .outputMode("append")\
              .option("checkpointLocation","abfss://silver@samratstorageazure.dfs.core.windows.net/DimUser/checkpoint")\
              .trigger(once=True)\
              .toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spotify_cata.silver.DimUser

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimArtist

# COMMAND ----------

df_artist=spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","parquet")\
              .option("cloudFiles.schemaLocation","abfss://silver@samratstorageazure.dfs.core.windows.net/DimArtist")\
              .load("abfss://bronze@samratstorageazure.dfs.core.windows.net/DimArtist")


# COMMAND ----------

from uuid import uuid4

run_id = str(uuid4()).replace("-", "")
view_name = f"df_user_debug_{run_id}"

# Use a checkpoint path outside of Unity Catalog managed locations
checkpoint_path = f"abfss://silver@samratstorageazure.dfs.core.windows.net/DimArtist/checkpoints/{view_name}"

query = (
    df_artist.writeStream
        .format("memory")
        .queryName(view_name)
        .outputMode("append")
        .trigger(once=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
)

query.awaitTermination()

# COMMAND ----------

spark.sql(f"SELECT * FROM {view_name}").display()

# COMMAND ----------

df_artist_obj=reusable()
df_artist= df_artist_obj.dropColumns(df_artist,['_rescued_data'])
df_artist= df_artist.dropDuplicates(['artist_id'])

# COMMAND ----------

spark.sql(f"SELECT * FROM {view_name}").display()

# COMMAND ----------

df_artist.writeStream.format("delta")\
              .outputMode("append")\
              .option("checkpointLocation","abfss://silver@samratstorageazure.dfs.core.windows.net/DimArtist/checkpoint")\
              .trigger(once=True)\
              .toTable("spotify_cata.silver.DimArtist")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spotify_cata.silver.DimArtist

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimTrack

# COMMAND ----------

df_track=spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","parquet")\
              .option("cloudFiles.schemaLocation","abfss://silver@samratstorageazure.dfs.core.windows.net/DimTrack")\
              .option("schemaEvaluationMode","addNewColumns")\
              .load("abfss://bronze@samratstorageazure.dfs.core.windows.net/DimTrack")

# COMMAND ----------

from uuid import uuid4

run_id = str(uuid4()).replace("-", "")
view_name = f"df_user_debug_{run_id}"

# Use a checkpoint path outside of Unity Catalog managed locations
checkpoint_path = f"abfss://silver@samratstorageazure.dfs.core.windows.net/DimTrack/checkpoints/{view_name}"

query = (
    df_track.writeStream
        .format("memory")
        .queryName(view_name)
        .outputMode("append")
        .trigger(once=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
)

query.awaitTermination()

# COMMAND ----------

spark.sql(f"SELECT * FROM {view_name}").display()

# COMMAND ----------

df_track=df_track.withColumn("duration_flag",when(col("duration_sec")<150,"Short")\
                                            .when(col("duration_sec")<300,"Medium")\
                                            .otherwise("Long"))

df_track= df_track.withColumn("track_name",regexp_replace(col("track_name"),'-',' '))
df_track= reusable().dropColumns(df_track,['_rescued_data'])

# COMMAND ----------

df_track.writeStream.format("delta")\
              .outputMode("append")\
              .option("checkpointLocation","abfss://silver@samratstorageazure.dfs.core.windows.net/DimTrack/checkpoint")\
              .trigger(once=True)\
              .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spotify_cata.silver.DimTrack

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimDate

# COMMAND ----------

df_date=spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","parquet")\
              .option("cloudFiles.schemaLocation","abfss://silver@samratstorageazure.dfs.core.windows.net/DimDate")\
              .option("schemaEvaluationMode","addNewColumns")\
              .load("abfss://bronze@samratstorageazure.dfs.core.windows.net/DimDate")

# COMMAND ----------

from uuid import uuid4

run_id = str(uuid4()).replace("-", "")
view_name = f"df_user_debug_{run_id}"

# Use a checkpoint path outside of Unity Catalog managed locations
checkpoint_path = f"abfss://silver@samratstorageazure.dfs.core.windows.net/DimDate/checkpoints/{view_name}"

query = (
    df_date.writeStream
        .format("memory")
        .queryName(view_name)
        .outputMode("append")
        .trigger(once=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
)

query.awaitTermination()

# COMMAND ----------

spark.sql(f"SELECT * FROM {view_name}").display()

# COMMAND ----------

df_date= reusable().dropColumns(df_date,['_rescued_data'])

# COMMAND ----------

df_date.writeStream.format("delta")\
              .outputMode("append")\
              .option("checkpointLocation","abfss://silver@samratstorageazure.dfs.core.windows.net/DimDate/checkpoint")\
              .trigger(once=True)\
              .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spotify_cata.silver.DimDate

# COMMAND ----------

# MAGIC %md
# MAGIC ### FactStream

# COMMAND ----------

df_fact=spark.readStream.format("cloudFiles")\
              .option("cloudFiles.format","parquet")\
              .option("cloudFiles.schemaLocation","abfss://silver@samratstorageazure.dfs.core.windows.net/FactStream"
              )\
              .option("schemaEvaluationMode","addNewColumns")\
              .load("abfss://bronze@samratstorageazure.dfs.core.windows.net/FactStream")

# COMMAND ----------

from uuid import uuid4

run_id = str(uuid4()).replace("-", "")
view_name = f"df_user_debug_{run_id}"

# Use a checkpoint path outside of Unity Catalog managed locations
checkpoint_path = f"abfss://silver@samratstorageazure.dfs.core.windows.net/FactStream/checkpoints/{view_name}"

query = (
    df_fact.writeStream
        .format("memory")
        .queryName(view_name)
        .outputMode("append")
        .trigger(once=True)
        .option("checkpointLocation", checkpoint_path)
        .start()
)

query.awaitTermination()

# COMMAND ----------

spark.sql(f"SELECT * FROM {view_name}").display()

# COMMAND ----------

df_fact= reusable().dropColumns(df_fact,['_rescued_data'])

df_fact.writeStream.format("delta")\
              .outputMode("append")\
              .option("checkpointLocation","abfss://silver@samratstorageazure.dfs.core.windows.net/FactStream/checkpoint")\
              .trigger(once=True)\
              .toTable("spotify_cata.silver.FactStream")



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spotify_cata.silver.FactStream

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spotify_cata.gold.dimtrack
# MAGIC WHERE track_id  in (46,5)

# COMMAND ----------

