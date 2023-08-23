

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from random import randint, choice


spark = SparkSession.Builder().getOrCreate()



def create_sample_date(r):
    records = []
    merchants = ['joes pizza', 'seaseme inn', 'thai cuisine', 'bambai palace']
    customers = ['adam', 'brian', 'caitlyn', 'denise']
    order_date = ['202305' + str(x) for x in range(10,31)]
    for i in range(r):
        new_record = [choice(merchants), choice(customers), choice(order_date), randint(20,51)]
        records.append(new_record)
    return records

sample_data = create_sample_date(20)


df = spark.createDataFrame(sample_data, schema=['merchant', 'customer', 'order_date', 'order_total'])


df.createOrReplaceTempView('orders')
spark.sql('''
            SELECT
                m.merchant
                , m.order_date
                , m.total_orders
                , m.total_customers
                , SUM(CASE WHEN c.customer IS NOT NULL THEN 1 ELSE 0 END) AS first_time_customers
            FROM (
                SELECT
                    merchant
                    , order_date
                    , SUM(order_total) AS total_orders
                    , COUNT(DISTINCT customer) AS total_customers
                    , RANK() OVER (PARTITION BY order_date ORDER BY SUM(order_total) DESC) AS ranker
                FROM orders
                GROUP BY 1, 2
            ) m
            LEFT JOIN (
                SELECT
                    customer
                    , merchant
                    , order_date
                    , RANK() OVER (PARTITION BY customer ORDER BY order_date) as ranker
                FROM orders
           ) c ON c.merchant = m.merchant
                   AND c.order_date = m.order_date
                   AND c.ranker = 1
           WHERE m.ranker = 1
           GROUP BY 1,2,3,4;
          ''').show()

