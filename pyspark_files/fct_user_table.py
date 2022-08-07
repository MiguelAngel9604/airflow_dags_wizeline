from pyspark.sql import SparkSession
from pyspark.sql.functions import when
from pyspark.sql.functions import split, col,size
from pyspark.sql import functions as F


spark = SparkSession.builder.getOrCreate()

#Defining tables' names
project_id = 'capable-hangout-357804 '
table_id_users = "fct.fct_users"
table_id_fct = "fct.fct_movie_analytics"
tempbucket = 'dim_bucket'
# Connection to GCS and get files
df_logs = spark.read.options(header=True).parquet(
    "gs://stg_files_wz/logs/part-00000-33189005-3c25-48c9-99c9-731e2f933c9c-c000.snappy.parquet"
)

#Get browser column
df1 = df_logs.withColumn('aux_browser', split(df_logs['os'], ' '))
dfspaces = df1.withColumn('white', size(split(col("os"), r" ")) - 1)
df2 = dfspaces.withColumn("browser", when(dfspaces.white == 1 ,dfspaces["aux_browser"].getItem(0)))
df2 = df2.withColumn("os", when(dfspaces.white == 1 ,dfspaces["aux_browser"].getItem(1)).otherwise(dfspaces["aux_browser"].getItem(0)))

#Dataframes
df_logs = df2.select("id_review","device","ipAddress","location","logDate","os","phoneNumber","browser")

df_movies = spark.read.options(header=True).parquet(
    "gs://stg_files_wz/movies/part-00000-6a82d5c0-a00b-4a30-9f8c-9656bb0a9ae6-c000.snappy.parquet"
)
df_movies2 = spark.read.options(header=True).parquet(
    "gs://stg_files_wz/movies/part-00000-d50d82af-6e34-4dab-87d1-f1d4614080ad-c000.snappy.parquet"
)
df_user_purchase = spark.read.options(header=True).csv(
    "gs://stg_files_wz/user_purchase.csv"
)

#Create views
df_movies.createOrReplaceTempView("movies_1")
df_movies2.createOrReplaceTempView("movies_2")

#Merge movies' parquets
movies_full = spark.sql(
    """
    WITH 
    movies as (
      select * from movies_1
      union 
      select * from movies_2
    )
    select 
        *
     from movies
    """
)

# Calculating amount spent on each purchase
df_user_purchase = df_user_purchase.filter("customer_id IS NOT NULL")
spent = df_user_purchase\
        .withColumn('amount_spent', df_user_purchase.quantity * df_user_purchase.unit_price)\
        .select('customer_id', 'amount_spent')

# Calculating total amount spent per client
spent_per_cid = spent.groupBy('customer_id')\
                     .agg(F.sum('amount_spent')\
                     .alias('amount_spent'))

#Score by user                     
score = movies_full.groupBy('user_id',)\
               .agg(
                   F.sum('positive_review').alias('review_score'),
                   F.count('user_id').alias('review_count')
                   )
               
# Joining data frames to produce the output data per user
user_behavior = spent_per_cid.join(score,spent_per_cid.customer_id ==  score.user_id,"outer")\
                             .select('customer_id','amount_spent','review_count','review_score')\
                             .withColumn('insert_date', F.current_timestamp()).filter("customer_id IS NOT NULL")               

# Joining movies with logs
reviews_logs= movies_full.join(df_logs,movies_full.review_id ==  df_logs.id_review,"left")\
          .select('movies.review_id','movies.user_id','movies.positive_review','logDate','device','location','os','browser')
                       
# Joining movies_logs with users to get the fct table
fct_table= reviews_logs.join(user_behavior,reviews_logs.user_id ==  user_behavior.customer_id,"left")\
          .select('review_id','user_id','logDate','device','location','os','browser','amount_spent','review_count','review_score')
                             

user_behavior.write.format('bigquery') \
  .mode("overwrite") \
  .option("temporaryGcsBucket",tempbucket) \
  .option('table', table_id_users) \
  .save()

fct_table.write.format('bigquery') \
  .mode("overwrite") \
  .option("temporaryGcsBucket",tempbucket) \
  .option('table', table_id_fct) \
  .save()
