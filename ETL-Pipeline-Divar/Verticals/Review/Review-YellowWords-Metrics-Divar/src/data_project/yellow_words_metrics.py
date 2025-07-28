from datetime import datetime, timedelta
from pyspark.sql import SparkSession

def get_yellow_words_metrics(start_date, end_date):
    spark = SparkSession.builder.appName("YellowWordsMetrics").getOrCreate()
    # Sample query simulation
    sample_data = [
        ("2023-05-14", "word1", "queue1", "cat1", 1, 100, 50, 0.5, 20, 10, 0.1, 30, 15, 0.15, 20, 10, 0.1),
    ]
    columns = ["date", "yellow_word", "queue", "dest_category", "reject_reason_id", "total_count", "used_solo", "used_set",
               "accepted", "accepted_solo", "accepted_set", "related_rejected", "related_rejected_solo", "related_rejected_set",
               "not_related_rejected", "not_related_rejected_solo", "not_related_rejected_set"]
    return spark.createDataFrame(sample_data, columns)

def calculate_yellow_words_metrics(ds, **kwargs):
    start_date = datetime.strptime(ds, '%Y-%m-%d')
    end_date = start_date + timedelta(days=1)
    end_date = end_date.strftime('%Y-%m-%d')
    start_date = start_date.strftime('%Y-%m-%d')

    result = get_yellow_words_metrics(start_date, end_date).collect()
    # Simulate insert_metrics
    for row in result:
        print(f"Inserting metric: date={row['date']}, yellow_word={row['yellow_word']}, total_count={row['total_count']}")

if __name__ == "__main__":
    calculate_yellow_words_metrics("2023-05-14")