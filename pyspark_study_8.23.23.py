
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import desc
from random import randint, sample, choice
from faker import Faker
from pathlib import Path
from datetime import datetime, date

spark = SparkSession.Builder().getOrCreate()
fake = Faker()
base_path = str(Path(__file__).parent)

def get_data_from_file(t):
    if t == 'csv':
        return spark.read.csv(base_path + '/data/raw.csv', header=True)
    if t == 'json':
        return spark.read.json(base_path + '/data/raw.json')

# df_json = get_data_from_file('json')
# df_json.show()
# df_json.printSchema()


# considerations around datetime_tz: reporting: what time of day people are logically doing something
# when we report on financials as a company. When we deploy code?

# df_json.select('founded', 'timezone', 'area').filter(df_json.area > 1000).show()

# df_json.coalesce(1).write.json(base_path + '/data_out/json_test')
# print('json_test created')


def create_sample_data(r):
    new_recs = []
    u_id = 0
    for i in range(r):
        u_id += 1
        user_id = u_id
        status = choice(['open','closed'])
        country = choice(['United States', 'Mexico', 'Canada', 'England', 'China'])
        post_date = fake.date_time_between(start_date=datetime(2023,1,1,1,1,1))
        comments = randint(1,5)
        new_recs.append([user_id, status, country, post_date, comments])
    return new_recs

df = spark.createDataFrame(create_sample_data(1000), schema=['user_id', 'status', 'country', 'post_date', 'comments'])

df.show()
df.printSchema()

df.filter(df.country != 'England').groupby('country', 'status').sum('comments').orderBy('country', desc('sum(comments)')).show(truncate=False)
