
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from faker import Faker
from datetime import date, datetime
from random import randint, choice
from pathlib import Path


spark = SparkSession.Builder().getOrCreate()
fake = Faker()

# base_path = str(Path(__file__).parent)

# df_json = spark.read.json(base_path + '/data/raw.json')
# df_json.show(10)
# df_json.printSchema()


# df_csv = spark.read.csv(base_path + '/data/raw.csv', header=True)


# df_csv.select(df_csv.name, df_csv.population).filter(df_csv.population > 9000000).show()

# df_csv.createOrReplaceTempView('dim_country')

# spark.sql('''
#             SELECT country, avg(average_temperature) FROM dim_country GROUP
#           ''')



# df = spark.createDataFrame([['pasta', date(2023, 8, 1), 45.9],
#                     ['pizza', date(2023 , 8, 5), 60.],
#                     ['pizza', date(2023 , 8, 6), 6.6],
#                     ['pizza', date(2023 , 8, 5), 76.],
#                     ['meatballs', date(2023 , 7, 9), 89.],
#                     ['meatballs', date(2023 , 7, 8), 90.],
#                     ['meatballs', date(2023 , 7, 9), 100.],
#                     ['bread', date(2023 , 6, 3), 7.5]],
#                     schema ='food string, expires date, volume float')

# df.show()
# df.groupby('food').sum(round('volume',2)).show()



# id  customer_id courier_id  seller_id order_timestamp_utc amount  city_id

def create_sample_data(r):
    new_recs = []
    for i in range (r):
        customer_id = randint(100,120)
        courier_id = randint(200,220)
        seller_id = randint(300, 330)
        order_timestamp = fake.date_time_between(start_date=datetime(2023,8,1,1,1,1), end_date=datetime(2023,8,14,23,59,59))
        amount = randint(10,50)
        city_id = randint(60,90)
        new_recs.append([customer_id, courier_id, seller_id, order_timestamp, amount, city_id])

    return new_recs

df = spark.createDataFrame(create_sample_data(2000),
          schema='''customer_id int,
                    courier_id int,
                    seller_id int,
                    order_timestamp timestamp_ntz,
                    amount int,
                    city_id int''')
df.show()


df.createOrReplaceTempView('orders')

# spark.sql('''
#             SELECT
#               COUNT(DISTINCT customer_id)
#               , AVG(amount)
#             FROM orders;
#           ''').show()

# spark.sql('''
#             SELECT
#               order_hour,
#               ROUND(AVG(order_count),0) avg_order_count

#             FROM (
#               SELECT
#                 HOUR(order_timestamp) AS order_hour,
#                 DATE(order_timestamp) AS order_date,
#                 COUNT(*) AS order_count
#               FROM orders
#               GROUP BY 1,2) a
#             GROUP BY 1
#             ORDER BY 2 DESC;
#           ''').show()

# spark.sql('''

#             SELECT * FROM (
#               SELECT
#                 city_id
#                 , DATE(order_timestamp) AS order_date
#                 , SUM(amount) AS total_amount
#                 , COALESCE(LAG(SUM(amount), 1) OVER (PARTITION BY city_id ORDER BY DATE(order_timestamp)),0) AS yesterdays_amount
#                 , SUM(amount) - COALESCE(LAG(SUM(amount), 1) OVER (PARTITION BY city_id ORDER BY DATE(order_timestamp)),0)
#                   AS change_from_yesterday
#               FROM orders
#               WHERE DATE(order_timestamp)  != '2023-08-01'
#               GROUP BY 1,2
#               ORDER BY 5
#               LIMIT 1
#             ) a

#             UNION

#             SELECT * FROM (
#               SELECT
#                 city_id
#                 , DATE(order_timestamp) AS order_date
#                 , SUM(amount) AS total_amount
#                 , COALESCE(LAG(SUM(amount), 1) OVER (PARTITION BY city_id ORDER BY DATE(order_timestamp)),0) AS yesterdays_amount
#                 , SUM(amount) - COALESCE(LAG(SUM(amount), 1) OVER (PARTITION BY city_id ORDER BY DATE(order_timestamp)),0)
#                   AS change_from_yesterday
#               FROM orders
#               WHERE DATE(order_timestamp)  != '2023-08-01'
#               GROUP BY 1,2
#               ORDER BY 5 DESC
#               LIMIT 1
#             ) b;
#           ''').show()


spark.sql('''
            SELECT
              COUNT(DISTINCT merchant_name) AS pizzaolos
              , AVG(order_amount) as avg_order_amount
            FROM (
              SELECT
                merchant_id
                , merchant_name
              FROM merchants
              WHERE merchant_name LIKE '%pizza%'
                AND city = 'Boston'
            ) p
            LEFT JOIN order o ON o.merchant_id = p.merchant_id;
          ''').show()
