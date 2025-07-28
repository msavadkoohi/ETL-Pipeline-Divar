from datetime import timedelta
import pyspark.sql.functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
from pyspark.sql.session import SparkSession

def read_query(file_path):
    spark = SparkSession.builder.appName("BlacklistAppealFunnel").getOrCreate()
    return spark.read.text(file_path).collect()[0][0]

def get_blacklist_data(start_date, end_date):
    blacklist_query = read_query("queries/blacklist_query.sql").format(start_date=start_date, end_date=end_date)
    spark = SparkSession.builder.appName("BlacklistAppealFunnel").getOrCreate()
    # Replace with sample data or actual DB connection if available
    sample_blacklist = [("09123456789", "2023-01-01 10:00:00", "verification", "token1"),
                        ("09123456780", "2023-01-02 12:00:00", "other", "token2")]
    return spark.createDataFrame(sample_blacklist, ["phone", "blacklisted_at", "side_effect_type", "token"])\
        .withColumn("blacklisted_at", F.from_utc_timestamp(F.col("blacklisted_at").cast(T.TimestampType()), "Asia/Tehran"))\
        .filter(F.col("blacklisted_at").between(start_date, end_date))

# Similar updates for other functions (get_unblacklist_data, etc.)
def get_blacklist_appeal_funnel(start_date, end_date):
    spark = SparkSession.builder.appName("BlacklistAppealFunnel").getOrCreate()
    # ... (rest of the function remains similar, update other data functions accordingly)
    blacklist_user_events = spark.createDataFrame([("09123456789", "2023-01-02 09:30:00", "view_appellant_page"),
                                                  ("09123456780", "2023-01-03 11:30:00", "view_appellant_page")],
                                                 ["phone_number", "datetime", "action"])\
        .filter(F.col("action") == "view_appellant_page")\
        .select(
            F.from_utc_timestamp(F.col("datetime").cast(T.TimestampType()), "Asia/Tehran").alias("viewed_appelant_page_at"),
            F.col("phone_number").alias("phone"),
        )\
        .filter(F.col("viewed_appelant_page_at").between(start_date, end_date))

    blacklist_appeal_df = get_blacklist_appeal_data(start_date, end_date)
    blacklist_appeallant_df = get_blacklist_appeallant_data(start_date, end_date)
    unblacklist_df = get_unblacklist_data(start_date, end_date)
    blacklist_df = get_blacklist_data(start_date, end_date)
    judgement_df = get_judgements_data(start_date, end_date)
    judgement_reason_df = get_judgement_reason_description_df()

    appellant_ids_data = blacklist_appeal_df.select("appellant_id", "result", "appeal_id")\
        .join(blacklist_appeallant_df, on="appellant_id", how="inner")

    black_unblack_df = blacklist_df.unionByName(unblacklist_df.select("phone", "blacklist_source", "blacklisted_at", "token", "side_effect_type"))

    window = Window.partitionBy("appeal_id").orderBy(
        F.col("judged_at").cast("long") - F.col("blacklisted_at").cast("long")
    )
    blacklist_source_data = black_unblack_df.join(
        appellant_ids_data.select("phone", "judged_at", "result", "appeal_id"),
        on="phone",
        how="left",
    )\
    .filter((F.col("judged_at") >= F.col("blacklisted_at")) | (F.col("judged_at").isNull()))\
    .withColumn("rank", F.when(F.col("appeal_id").isNotNull(), F.rank().over(window)).otherwise(F.lit(1)))\
    .filter("rank==1")\
    .drop("rank")\
    .join(judgement_df.select("phone", "token", "judgement_reason_id"), on=["phone", "token"], how="left")\
    .join(judgement_reason_df.select("judgement_reason_id", "title"), on="judgement_reason_id", how="left")\
    .withColumn("appeal_accept", F.when(F.col("result") == 2, True).otherwise(False))\
    .withColumn("appeal_reject", F.when(F.col("result") == 1, True).otherwise(False))\
    .withColumn("appeal_not_reviewed", F.when(((F.col("result") == "NaN") & (F.col("judged_at").isNotNull())), True).otherwise(False))\
    .withColumn("appeal_change_ownership", F.when(F.col("result") == 4, True).otherwise(False))\
    .withColumn("appeal_edit_photo", F.when(F.col("result") == 3, True).otherwise(False))\
    .withColumn("date", F.to_date(F.col("blacklisted_at")))

    blacklist_source_data = blacklist_source_data.join(blacklist_user_events, on="phone", how="left_outer")\
        .withColumn("viewed_appelant_page_at", F.when(F.col("viewed_appelant_page_at") >= F.col("blacklisted_at"), F.col("viewed_appelant_page_at")).otherwise(None))\
        .withColumn("viewed_appelant_page_at", F.min("viewed_appelant_page_at").over(Window.partitionBy("phone", "blacklisted_at")))\
        .dropDuplicates(["phone", "blacklisted_at", "token"])

    return blacklist_source_data

if __name__ == "__main__":
    start_date = "2023-01-01"
    end_date = "2023-01-05"
    result_df = get_blacklist_appeal_funnel(start_date, end_date)
    result_df.show()