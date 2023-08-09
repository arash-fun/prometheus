from pyspark.sql import SparkSession
from datetime import datetime
import os


def restore_from_parquet():
    spark = SparkSession.builder.appName('prometheusRestore').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    parquet_path = '/home/arash/Desktop/metrics_backup/metrics.parquet'  # Path to the Parquet file in Hadoop

    df = spark.read.parquet(parquet_path)

    # Convert DataFrame rows to Prometheus file format
    prometheus_data = []
    for row in df.collect():
        metric_name = row['metric']
        labels = row['labels']
        timestamp = row['timestamp'].timestamp()
        value = row['value']
        prometheus_data.append(f'{metric_name}{{{labels}}} {value} {timestamp}')
    format_restore = 'metrics.json' # or 'metrics.prom'
    prometheus_file_path = f'/home/arash/Desktop/{format_restore}'
    with open(prometheus_file_path, 'w') as prometheus_file:
        prometheus_file.write('\n'.join(prometheus_data))

    print(f'Restored Prometheus file: {prometheus_file_path}')


restore_from_parquet()