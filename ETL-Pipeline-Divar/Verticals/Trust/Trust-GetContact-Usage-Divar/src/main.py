import pyspark.sql.functions as F
from pyspark.sql import SparkSession
import pendulum as pn

def get_next_date(start_date):
    return pn.parse(start_date) + pn.duration(days=1)

def get_requests_summary(start_date, end_date=None):
    '''
    Return number of requests, phones, IPs for each day, status_code and status_msg
    '''
    if not end_date:
        end_date = get_next_date(start_date)

    # Sample data (replace get_backend_get_contact_logs)
    sample_logs = [
        ("2023-01-01", "200", "OK", "192.168.1.1", "09123456789"),
        ("2023-01-01", "400", "Error", "192.168.1.2", "09123456780"),
        ("2023-01-02", "200", "OK", "192.168.1.1", "09123456781")
    ]
    spark = SparkSession.builder.appName("GetContactUsage").getOrCreate()
    df_backend_get_contact_logs = spark.createDataFrame(sample_logs, ["date", "status", "status_msg", "ip", "phone"])

    df_res = (
        df_backend_get_contact_logs
        .groupby('date', 'status', 'status_msg')
        .agg(
            F.count('*').alias('request_count'),
            F.countDistinct('phone').alias('phone_count'),
            F.countDistinct('ip').alias('ip_count')
        )
        .sort(F.asc('date'))
    )
    return df_res

def get_backend_and_user_get_contact_log_summary(start_date):
    '''
    Return number of backend requests and user log requests for each day
    '''
    end_date = get_next_date(start_date)

    cols = ['date', 'token', 'ip', 'phone']
    # Sample backend logs
    sample_backend_logs = [
        ("2023-01-01", "token1", "192.168.1.1", "09123456789"),
        ("2023-01-01", "token2", "192.168.1.2", "09123456780")
    ]
    # Sample user logs
    sample_user_logs = [
        ("2023-01-01", "token3", "192.168.1.3", "09123456781")
    ]

    spark = SparkSession.builder.appName("GetContactUsage").getOrCreate()
    df_backend_logs = spark.createDataFrame(sample_backend_logs, cols)\
        .withColumn('request_type', F.lit('backend'))
    df_user_logs = spark.createDataFrame(sample_user_logs, cols)\
        .withColumn('request_type', F.lit('user'))\
        .withColumn('date', F.lit(start_date))

    df_all_logs = df_backend_logs.union(df_user_logs)
    df_res = (
        df_all_logs
        .groupby('date', 'request_type')
        .agg(F.count('*').alias('request_count'))
    )
    return df_res

if __name__ == "__main__":
    start_date = "2023-01-01"
    requests_summary = get_requests_summary(start_date)
    requests_summary.show()
    log_summary = get_backend_and_user_get_contact_log_summary(start_date)
    log_summary.show()