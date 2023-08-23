
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from faker import Faker
from random import randint, choice
from datetime import datetime, date


spark = SparkSession.Builder().getOrCreate()
fake = Faker()

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

df.createOrReplaceTempView('posts')

# spark.sql('''
#             SELECT
#               ROUND((SUM(CASE WHEN country = 'United States' THEN 1 ELSE 0 END)/
#               COUNT(*))*100,2)
#             FROM active_users
#             WHERE status = 'open';
#           ''').show()


# spark.sql('''
#             SELECT
#               DAYOFMONTH(post_date)
#               , COUNT(DISTINCT user_id) as posts
#             FROM posts
#             GROUP BY 1
#             ORDER BY 1;
#           ''').show()


spark.sql('''
            SELECT
              user_id
              , SUM(comments) as total_comments
            FROM posts
            WHERE DATEDIFF(CURRENT_DATE, post_date) <= 30
            GROUP BY 1
          ''').show()
