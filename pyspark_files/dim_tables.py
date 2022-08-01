from pyspark.sql import SparkSession
from google.cloud.storage import client
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import io
import pandas as pd
from pyspark.sql.functions import when
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col,substring,regexp_replace,size

spark = SparkSession.builder.getOrCreate()

# Connection to GCS and get files

df_logs = spark.read.options(header=True).parquet(
    "gs://stg_files_wz/logs/part-00000-c379bb4a-4137-4d5b-bdc6-319adaf64468-c000.snappy.parquet"
)
df_movies = spark.read.options(header=True).parquet(
    "gs://stg_files_wz/movies/part-00000-70cbefcf-c722-4b6d-bbbf-5e7c281345d5-c000.snappy.parquet"
)

#Get browser column
df1 = df_logs.withColumn('aux_browser', split(df_logs['os'], ' '))
dfspaces = df1.withColumn('white', size(split(col("os"), r" ")) - 1)
df2 = dfspaces.withColumn("browser", when(dfspaces.white == 1 ,dfspaces["aux_browser"].getItem(0)))
df2 = df2.withColumn("os", when(dfspaces.white == 1 ,dfspaces["aux_browser"].getItem(1)).otherwise(dfspaces["aux_browser"].getItem(0)))
df_logs = df2.select("id_review","device","ipAddress","location","logDate","os","phoneNumber","browser")

# Create views to query
df_logs.createOrReplaceTempView("logs")
df_movies.createOrReplaceTempView("movies")

# Implement logicxs
dim_date = spark.sql(
    """
    SELECT 
    CAST (id_review AS INTEGER) AS id_dim_date
    ,to_date(logDate,'MM-dd-yyyy') as log_date
    , CAST(EXTRACT(DAY FROM to_date(logDate,'MM-dd-yyyy')) AS STRING) AS day
    , CAST(EXTRACT(MONTH FROM to_date(logDate,'MM-dd-yyyy')) AS STRING) AS month
    , CAST(EXTRACT(YEAR FROM to_date(logDate,'MM-dd-yyyy')) AS STRING) AS year
    , (CASE WHEN month(to_date(logDate,'MM-dd-yyyy')) in (12, 1, 2) THEN 'winter'
      WHEN month(to_date(logDate,'MM-dd-yyyy')) in (3, 4, 5) THEN 'spring'
      WHEN month(to_date(logDate,'MM-dd-yyyy')) in (6, 7, 8) THEN 'summer'
      WHEN month(to_date(logDate,'MM-dd-yyyy')) in (9, 10, 11) THEN 'autumn'
 end) as season
    FROM logs
    """
)
spark.conf.set("spark.sql.execution.arrow.enabled", "true")
dim_date_df = dim_date.toPandas()


#Defining tables' names
project_id = 'capable-hangout-357804 '
table_id_dim_date = "dim_tables.dim_date"
table_id_dim_devices = "dim_tables.dim_devices"
table_id_dim_location = "dim_tables.dim_location"
table_id_dim_os = "dim_tables.dim_os"
table_id_dim_browser = "dim_tables.dim_browser"


# Saving the data to BigQuery
dim_date_df.write.format('bigquery') \
  .mode("truncate") \
  .option('table', table_id_dim_date) \
  .save()

