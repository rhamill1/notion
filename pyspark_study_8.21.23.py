
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datetime import date, datetime
from faker import Faker
from random import randint, choice


spark = SparkSession.Builder().getOrCreate()
fake = Faker()

def create_sample_date(r):
    records = []
    for i in range(r):
        user_id = randint(1,10)
        session_start = fake.date_time_between(start_date=datetime(2023,8,1,1,1,1), end_date=datetime(2023,8,5,23,59,59))
        session_end = fake.date_time_between(start_date=session_start, end_date=datetime(2023,8,5,23,59,59))
        session_id = randint(100,200)
        session_type = choice(['streamer', 'viewer'])
        records.append([user_id, session_start, session_end, session_id, session_type])
    return records

data = create_sample_date(300)
# print(data)

df = spark.createDataFrame(data, schema=['user_id', 'session_start', 'session_end', 'session_id', 'session_type'])

df.show()
# df.printSchema()


df.createOrReplaceTempView('sessions')

# spark.sql('''
#             SELECT
#                 user_id
#                 , SUM(CASE WHEN session_type = 'viewer' THEN 1 ELSE 0 END) AS views
#                 , SUM(CASE WHEN session_type = 'streamer' THEN 1 ELSE 0 END) AS stream

#                 , SUM(CASE WHEN session_type = 'streamer' THEN 1 ELSE 0 END) -
#                 SUM(CASE WHEN session_type = 'viewer' THEN 1 ELSE 0 END)  AS diff

#             FROM sessions
#             GROUP BY 1
#             HAVING SUM(CASE WHEN session_type = 'viewer' THEN 1 ELSE 0 END) <
#                 SUM(CASE WHEN session_type = 'streamer' THEN 1 ELSE 0 END)
#             ORDER BY diff DESC
#             LIMIT 10;

#           ''').show()


# spark.sql('''
#             SELECT
#               user_id
#               , SUM(CASE WHEN session_type = 'viewer' THEN 1 ELSE 0 END) AS views
#               , SUM(CASE WHEN session_type = 'streamer' THEN 1 ELSE 0 END) AS streams

#             FROM sessions
#             GROUP BY 1
#             HAVING views > 1 AND streams > 1;
#           ''').show()


# spark.sql('''

#             SELECT
#                 session_type
#                 , AVG(session_end - session_start) as duration
#             FROM sessions
#             GROUP BY 1;
#           ''').show()


spark.sql('''
            SELECT
              s.user_id
              , SUM(CASE WHEN s.session_type = 'streamer' THEN 1 ELSE 0 END) AS streams

            FROM sessions s
            JOIN (
                SELECT *

                FROM (
                    SELECT
                      user_id
                      , session_type
                      , RANK() OVER (PARTITION BY user_id ORDER BY session_start) AS ranker
                    FROM sessions
                ) a
                WHERE ranker = 1
                  AND session_type = 'viewer'
            ) v on v.user_id = s.user_id
            GROUP BY 1
            ORDER BY 2 DESC;
          ''').show()
