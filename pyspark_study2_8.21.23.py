
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from faker import Faker
from datetime import date, datetime
from random import randint, choice
from pathlib import Path


basepath = str(Path(__file__).parent)
spark = SparkSession.Builder().getOrCreate()
fake = Faker()


# df = spark.read.csv('path/file.csv', header=True)
# df = spark.read.json('path/file.json')


def create_sample_data(r):
    records = []
    for i in range(r):
        user_id = randint(1,10)
        session_start = fake.date_time_between(start_date=datetime(2023,8,1,1,1,1), end_date=datetime(2023,8,5,23,59,59))
        session_end = fake.date_time_between(start_date=session_start, end_date=datetime(2023,8,5,23,59,59))
        session_id = randint(100,200)
        session_type = choice(['streamer', 'viewer'])
        records.append([user_id, session_start, session_end, session_id, session_type])
    return records

df = spark.createDataFrame(create_sample_data(300), schema=['user_id', 'session_start', 'session_end', 'session_id', 'session_type'])
df.show()
df.printSchema()


# df.select(df.user_id, df.session_type).filter(df.session_type == 'viewer').show()
df.groupby(df.session_type).avg().createOrReplaceTempView('df_avg')
spark.sql('SELECT session_type, ROUND(`avg(user_id)`,1) FROM df_avg').show()

df.coalesce(1).write.csv(basepath + '/data_out/avg_user.csv', header=True)
print('it all ran')
