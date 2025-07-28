import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pendulum as pn

def computation_date_transformer(execution_date):
    exec_date = pn.parse(execution_date)
    return {
        'monthly_start_computation_date': exec_date.start_of('month').to_date_string(),
        'monthly_end_computation_date': exec_date.end_of('month').to_date_string(),
        'jmonth': exec_date.format('YYYY/MM/DD')
    }

def read_query(file_path, start_date, end_date):
    spark = SparkSession.builder.appName("FraudMetrics").getOrCreate()
    query = spark.sparkContext.textFile(file_path).collect()[0]
    return query.format(start_date=start_date, end_date=end_date)

def get_fraud_reports(start_date, end_date):
    query = read_query("queries/queries.sql", start_date, end_date).split("-- Query for get_fraud_reports")[1].strip()
    spark = SparkSession.builder.appName("FraudMetrics").getOrCreate()
    # Replace with actual DB connection or use sample data for now
    sample_reports = [("09123456789", "token1", "cat1", "2023-01-01"),
                      ("09123456780", "token2", "cat2", "2023-01-02")]
    return spark.createDataFrame(sample_reports, ["phone", "token", "cat1", "report_date"])

def get_demand_side_crm_fraud_reporters(start_date, end_date, occurrence=True):
    query = read_query("queries/queries.sql", start_date, end_date).split("-- Query for get_demand_side_crm_fraud_reporters")[1].strip()
    spark = SparkSession.builder.appName("FraudMetrics").getOrCreate()
    sample_crm = [("09123456789", "2023-01-01"), ("09123456780", "2023-01-02")]
    return spark.createDataFrame(sample_crm, ["phone", "report_date"])

def fraud_blacklist_tokens(start_date, end_date):
    query = read_query("queries/queries.sql", start_date, end_date).split("-- Query for fraud_blacklist_tokens")[1].strip()
    spark = SparkSession.builder.appName("FraudMetrics").getOrCreate()
    sample_blacklist = [("token1", 1), ("token2", 0)]
    return spark.createDataFrame(sample_blacklist, ["token", "is_fraud"])

def get_total_active_users(start_date, end_date):
    query_click = read_query("queries/queries.sql", start_date, end_date).split("-- Query for maaleh.read.action_log.clean_click_post")[1].strip()
    query_load = read_query("queries/queries.sql", start_date, end_date).split("-- Query for maaleh.read.action_log.clean_load_post_page")[1].strip()
    spark = SparkSession.builder.appName("FraudMetrics").getOrCreate()
    sample_click = [("09123456789",), ("09123456780",)]
    sample_load = [("09123456781",), ("09123456780",)]
    click_post = spark.createDataFrame(sample_click, ["phone"])
    load_post_page = spark.createDataFrame(sample_load, ["phone"])
    return click_post.union(load_post_page).dropDuplicates()\
        .withColumn("score", F.lit(1))\
        .withColumn("tokens", F.lit(""))\
        .withColumn("cat1s", F.lit(""))

def all_interactions(start_date, end_date):
    query_click = read_query("queries/queries.sql", start_date, end_date).split("-- Query for maaleh.read.action_log.clean_click_contact")[1].strip()
    query_select = read_query("queries/queries.sql", start_date, end_date).split("-- Query for maaleh.read.action_log.clean_select_contact_method")[1].strip()
    spark = SparkSession.builder.appName("FraudMetrics").getOrCreate()
    sample_click = [("09123456789", "token1", "cat1"),
                    ("09123456780", "token2", "cat2")]
    sample_select = [("09123456781", "token3", "cat3"),
                     ("09123456780", "token4", "cat4")]
    click_contact_df = spark.createDataFrame(sample_click, ["phone", "token", "cat1"])
    select_contact_df = spark.createDataFrame(sample_select, ["phone", "token", "cat1"])
    return click_contact_df.unionByName(select_contact_df).dropDuplicates()

def calculate_fraud_events(execution_date):
    date_transformed = computation_date_transformer(execution_date)
    start_date = date_transformed.get('monthly_start_computation_date')
    end_date = date_transformed.get('monthly_end_computation_date')
    jmonth = date_transformed.get('jmonth')
    z_alpha = 1.96

    sample_conversion = [("not_exposed", 0.01), ("interacted_with_reported_tokens", 0.05),
                        ("interacted_with_blacklisted_tokens", 0.10), ("chat_blocker", 0.15),
                        ("not_verified_fraud_reporters", 0.20), ("verified_fraud_reporters", 0.25),
                        ("crm_reporter", 0.30)]
    spark = SparkSession.builder.appName("FraudMetrics").getOrCreate()
    conversion_rate_info = spark.createDataFrame(sample_conversion, ["type", "conversion_rate"])

    total_exposed = total_exposed_numbers(start_date, end_date)
    crm_actual_fraud_event = get_demand_side_crm_fraud_reporters(start_date, end_date, occurrence=True).count()

    calculate_fraud_events = (
        total_exposed
        .join(conversion_rate_info, on="type", how="left")
        .withColumn('fraud_event', F.col('conversion_rate') * F.col('count'))
        .withColumn('fraud_event', F.when(F.col('type') == 'crm_reporter', F.lit(crm_actual_fraud_event)).otherwise(F.col('fraud_event')))
        .withColumn('conversion_rate', F.when(F.col('type') == 'crm_reporter', F.col('fraud_event') / F.col('count')).otherwise(F.col('conversion_rate')))
        .withColumn('jalali_month', F.lit(jmonth))
    )
    return calculate_fraud_events

def total_exposed_numbers(start_date, end_date):
    total_exposed_numbers = (
        get_total_active_users(start_date, end_date)
        .union(get_users_interacted_with_reported_tokens(start_date, end_date))
        .union(get_users_interacted_with_blacklisted_tokens(start_date, end_date))
        .groupBy("phone")
        .agg(F.max(F.col("score")).alias("score"))
        .withColumn(
            "type",
            F.when(F.col("score") == 1, "not_exposed")
            .when(F.col("score") == 2, "interacted_with_reported_tokens")
            .when(F.col("score") == 3, "interacted_with_blacklisted_tokens")
            .otherwise("other")
        )
        .groupBy("type")
        .agg(F.count("phone").alias("count"))
    )
    return total_exposed_numbers

def get_users_interacted_with_reported_tokens(start_date, end_date):
    intractions = all_interactions(start_date, end_date)
    tokens = get_fraud_reports(start_date, end_date).select("token").withColumn("is_reported", F.lit(1))
    intract_with_reported_tokens = (
        intractions
        .join(tokens, "token", "left")
        .filter(F.col("is_reported") == 1)
        .groupBy('phone')
        .agg(F.collect_list('token').cast('string').alias('tokens'),
             F.collect_list('cat1').cast('string').alias('cat1s'))
        .withColumn("score", F.lit(2))
    )
    return intract_with_reported_tokens

def get_users_interacted_with_blacklisted_tokens(start_date, end_date):
    intractions = all_interactions(start_date, end_date)
    blacklisted_tokens = fraud_blacklist_tokens(start_date, end_date)
    intract_with_blacklist_tokens = (
        intractions
        .join(blacklisted_tokens, "token", "left")
        .filter(F.col("is_fraud") == 1)
        .groupBy('phone')
        .agg(F.collect_list('token').cast('string').alias('tokens'),
             F.collect_list('cat1').cast('string').alias('cat1s'))
        .withColumn("score", F.lit(3))
    )
    return intract_with_blacklist_tokens

if __name__ == "__main__":
    execution_date = "2023-01-15"
    result_df = calculate_fraud_events(execution_date)
    result_df.show()