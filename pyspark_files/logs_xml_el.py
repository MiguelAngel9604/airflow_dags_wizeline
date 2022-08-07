import sqlalchemy
import os
import re

from pyspark.sql import DataFrame, SparkSession 
from pyspark.ml.feature import Tokenizer, StopWordsRemover
from pyspark.sql.functions import col, size, when, array_contains, row_number, lit
from pyspark.sql.window import Window

from google.cloud import storage

# GCS paths
log_review_path = \
    "gs://capable-hangout-357804-input/log_reviews.csv"

# Output
log_review_staging_path = \
    "gs://stg_files_wz/logs"

def read_xml_load_to_gcs(gcs_path, row_tag, root_tag):
    df_log_review = spark.read\
        .option("rowTag", row_tag)\
        .option("rootTag", root_tag)\
        .format("xml")\
        .load(gcs_path)

    window_ = Window().orderBy(lit('A'))
    df_log_review = df_log_review.withColumn(
        "id_review", row_number().over(window_)
    )

    print(df_log_review.count())
    print(df_log_review.columns)
    print(df_log_review.first())

    df_log_review\
        .write.mode('overwrite')\
        .parquet(log_review_staging_path)

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Data cleaning") \
    .getOrCreate()

read_xml_load_to_gcs(log_review_path, "log", "reviewlog")