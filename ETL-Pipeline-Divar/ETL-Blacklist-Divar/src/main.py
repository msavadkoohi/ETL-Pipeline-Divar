from datetime import timedelta
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.session import SparkSession

def get_blacklist_data(start_date, end_date, spark):
    blacklist_df = spark.read.csv("data/sample_data.csv", header=True)\
        .withColumn('blacklisted_at', F.from_utc_timestamp(F.col('blacklisted_at'), "Asia/Tehran"))\
        .withColumn('verification_blacklist_source', F.when(F.col('side_effect_type').like('%verification%'), F.lit(1000)))\
        .withColumn('blacklist_source', F.coalesce(F.col('verification_blacklist_source'), F.lit(None)))\
        .select('phone', 'blacklist_source', 'blacklisted_at', 'side_effect_type')
    return blacklist_df

def get_unblacklist_data(start_date, end_date, spark):
    unblacklist_df = spark.read.csv("data/sample_data.csv", header=True)\
        .withColumn('blacklisted_at', F.from_utc_timestamp(F.col('blacklisted_at'), "Asia/Tehran"))\
        .withColumn('verification_blacklist_source', F.when(F.col('side_effect_type').like('%verification%'), F.lit(13)))\
        .withColumn('blacklist_source', F.coalesce(F.col('verification_blacklist_source'), F.lit(None)))\
        .select('phone', 'blacklist_source', 'blacklisted_at', 'side_effect_type')
    return unblacklist_df

def blacklist_source_data(start_date, end_date):
    spark = SparkSession.builder.appName("BlacklistETL").getOrCreate()
    blacklist_df = get_blacklist_data(start_date, end_date, spark)
    unblacklist_df = get_unblacklist_data(start_date, end_date, spark)
    black_unblack_df = blacklist_df.unionByName(unblacklist_df)
    return black_unblack_df

if __name__ == "__main__":
    start_date = "2023-01-01"
    end_date = "2023-01-05"
    result_df = blacklist_source_data(start_date, end_date)
    result_df.show()