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
    "gs://stg_files_wz/logs/part-00000-33189005-3c25-48c9-99c9-731e2f933c9c-c000.snappy.parquet"
)

#Defining tables' names
project_id = 'capable-hangout-357804 '
tempbucket = 'dim_bucket'
table_id_dim_date = "dim_tables.dim_date"
table_id_dim_devices = "dim_tables.dim_devices"
table_id_dim_location = "dim_tables.dim_location"
table_id_dim_os = "dim_tables.dim_os"
table_id_dim_browser = "dim_tables.dim_browser"


#Get browser column
df1 = df_logs.withColumn('aux_browser', split(df_logs['os'], ' '))
dfspaces = df1.withColumn('white', size(split(col("os"), r" ")) - 1)
df2 = dfspaces.withColumn("browser", when(dfspaces.white == 1 ,dfspaces["aux_browser"].getItem(0)))
df2 = df2.withColumn("os", when(dfspaces.white == 1 ,dfspaces["aux_browser"].getItem(1)).otherwise(dfspaces["aux_browser"].getItem(0)))
df_logs = df2.select("id_review","device","ipAddress","location","logDate","os","phoneNumber","browser")

# Create views to query
df_logs.createOrReplaceTempView("logs")

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

# Saving the data to BigQuery
dim_date.write.format('bigquery') \
  .mode("overwrite") \
    .option("temporaryGcsBucket",tempbucket) \
  .option('table', table_id_dim_date) \
  .save()

dim_devices = spark.sql(
    """
    SELECT 
    CAST (id_review AS INTEGER) AS id_dim_devices
    , CAST(device AS STRING) AS device
    FROM logs
    """
)

# Saving the data to BigQuery
dim_devices.write.format('bigquery') \
  .mode("overwrite") \
    .option("temporaryGcsBucket",tempbucket) \
  .option('table', table_id_dim_devices) \
  .save()

dim_location = spark.sql(
    """
    SELECT 
    CAST (id_review AS INTEGER) AS id_dim_location
    , CAST(location AS STRING) AS location
    FROM logs
    """
)

# Saving the data to BigQuery
dim_location.write.format('bigquery') \
  .mode("overwrite") \
  .option("temporaryGcsBucket",tempbucket) \
  .option('table', table_id_dim_location) \
  .save()
  
dim_os = spark.sql(
    """
    SELECT 
    CAST (id_review AS INTEGER) AS id_dim_os
    , CAST(os AS STRING) AS os
    FROM logs
    """
)

# Saving the data to BigQuery
dim_os.write.format('bigquery') \
  .mode("overwrite") \
    .option("temporaryGcsBucket",tempbucket) \
  .option('table', table_id_dim_os) \
  .save()

dim_browser = spark.sql(
    """
    SELECT 
    CAST (id_review AS INTEGER) AS id_dim_browser
    , CAST(browser AS STRING) AS browser
    FROM logs
    """
)

# Saving the data to BigQuery
dim_browser.write.format('bigquery') \
  .mode("overwrite") \
    .option("temporaryGcsBucket",tempbucket) \
  .option('table', table_id_dim_browser) \
  .save()