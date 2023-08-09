# #import requests
# from datetime import datetime, timedelta
# from pyspark.sql import SparkSession
# import os
# import requests

# def running_back_up():
#     spark = SparkSession.builder.appName('prometheusBackup').config("spark.executor.memory", "8g").getOrCreate()
#     spark.sparkContext.setLogLevel("ERROR")

#     prometheus_url = 'http://192.168.1.98:9090/api/v1/query_range'
#     start_date = datetime.today() - timedelta(days=1)
#     end_date = datetime.today()
#     step = '6h'
#     query = '{__name__=~".+"}'

#     for single_date in date_range(start_date, end_date):
#         for hour in range(0, 24):
#             for minute in range(0, 60, 30):  # Run every 30 minutes
#                 start_time = single_date.replace(hour=hour, minute=minute, second=0).isoformat() + 'Z'
#                 end_time = single_date.replace(hour=hour, minute=minute+29, second=59).isoformat() + 'Z'

#                 response = requests.get(prometheus_url, params={
#                     'query': query,
#                     'start': start_time,
#                     'end': end_time,
#                     'step': step
#                 })

#                 try:
#                     response.raise_for_status()
#                     data = response.json()['data']['result']
#                     save_metrics_to_parquet(spark, data, start_time, end_time)
#                     print(f'Successfully backed up metrics for {start_time} to {end_time}')
#                 except requests.exceptions.HTTPError as e:
#                     print(f'Request failed: {e}')
#                     print(f'Response content: {response.content}')
#                 except KeyError as e:
#                     print(f'Failed to extract data from response: {e}')
#                     print(f'Response content: {response.content}')


# def save_metrics_to_parquet(spark, data, start_time, end_time):
#     rows = []
#     for result in data:
#         metric_name = result['metric']['__name__']
#         labels = ','.join([f'{k}="{v}"' for k, v in result['metric'].items() if k != '__name__'])
#         for value in result['values']:
#             timestamp_float = float(value[0])
#             timestamp = datetime.fromtimestamp(timestamp_float)
#             rows.append((metric_name, labels, timestamp, float(value[1])))

#     df = spark.createDataFrame(rows, ['metric', 'labels', 'timestamp', 'value'])
#     coalesced_df = df.coalesce(1)  # Specify the desired number of partitions
#     parquet_filename = f'metrics_{start_time}_{end_time}.parquet'
#     parquet_path = '/home/arash/Desktop/metrics_backup/' + parquet_filename
#     coalesced_df.write.parquet(parquet_path, mode='overwrite', compression='gzip')


# def date_range(start_date, end_date):
#     for n in range(int((end_date - start_date).days)):
#         yield start_date + timedelta(n)


# def time_backup():
#     start_time = datetime.now()  # Start time of the backup process
#     running_back_up()
#     end_time = datetime.now()  # End time of the backup process
#     total_time = end_time - start_time
#     print(f'Total time taken for backup: {total_time}')


# time_backup()





import requests
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import os
import cProfile




# def my_function():
def running_back_up():
    spark = SparkSession.builder.appName('prometheusBackup').config("spark.executor.memory", "8g").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    prometheus_url = 'http://192.168.1.98:9090/api/v1/query_range'
    start_date = datetime.today() - timedelta(days=1)
    end_date = datetime.today()
    step = '1m'
    query = '{__name__=~".+"}'

    data = []

    for single_date in date_range(start_date, end_date):
        for hour in range(0, 24):
            for minute in range(0, 60, 30):  # Run every 30 minutes
                start_time = single_date.replace(hour=hour, minute=minute, second=0).isoformat() + 'Z'
                end_time = single_date.replace(hour=hour, minute=minute+29, second=59).isoformat() + 'Z'

                response = requests.get(prometheus_url, params={
                    'query': query,
                    'start': start_time,
                    'end': end_time,
                    'step': step
                })

                try:
                    response.raise_for_status()
                    data.extend(response.json()['data']['result'])
                    print(f'Successfully backed up metrics for {start_time} to {end_time}')
                except requests.exceptions.HTTPError as e:
                    print(f'Request failed: {e}')
                    print(f'Response content: {response.content}')
                except KeyError as e:
                    print(f'Failed to extract data from response: {e}')
                    print(f'Response content: {response.content}')

    save_metrics_to_parquet(spark, data)


def save_metrics_to_parquet(spark, data):
    rows = []
    for result in data:
        metric_name = result['metric']['__name__']
        labels = ','.join([f'{k}="{v}"' for k, v in result['metric'].items() if k != '__name__'])
        for value in result['values']:
            timestamp_float = float(value[0])
            timestamp = datetime.fromtimestamp(timestamp_float)
            rows.append((metric_name, labels, timestamp, float(value[1])))

    df = spark.createDataFrame(rows, ['metric', 'labels', 'timestamp', 'value'])
    parquet_path = '/home/arash/Desktop/metrics_backup/metrics.parquet'
    df.write.parquet(parquet_path, mode='overwrite', compression='gzip')


def date_range(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def time_backup():
    start_time = datetime.now()  # Start time of the backup process
    running_back_up()
    end_time = datetime.now()  # End time of the backup process
    total_time = end_time - start_time
    print(f'Total time taken for backup: {total_time}')


time_backup()


# cProfile.run('my_function()')