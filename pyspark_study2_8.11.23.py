
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datetime import date, datetime
from random import randint, choice
from faker import Faker


spark = SparkSession.Builder().getOrCreate()
fake = Faker()

def create_sample_data(r):

    records = []
    for i in range(r):
        new_record = [fake.date_time_between(start_date=datetime(2023,8,1,12,1,1), end_date=datetime(2023,8,10,23,59,59)), randint(500,525), randint(10,50), randint(1,20)]
        records.append(new_record)
    return records

df = spark.createDataFrame(create_sample_data(200),schema='created_on timestamp_ntz, request_id int, call_duration int, client int')


df.createOrReplaceTempView('calls')

# high_touch_customers = spark.sql('''
#                                     SELECT SUM(calls) FROM (
#                                       SELECT
#                                         clients,
#                                         COUNT(*) as calls
#                                       FROM calls
#                                       WHERE call_duration >= 30
#                                         AND HOUR(created_on) between 15 and 18
#                                       GROUP BY 1
#                                       HAVING COUNT(*) >= 2
#                                     ) a;
#                                  ''').show()


spark.sql('''
              SELECT ROUND(AVG(call_duration),2) AS avg_follow_up_call_duration FROM (
                  SELECT
                    c.*,
                    RANK() OVER (PARTITION BY client ORDER BY created_on) as client_call_count

                  FROM calls c
              ) a
              WHERE client_call_count > 1;
          ''').show()
