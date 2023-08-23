
from pathlib import Path
from datetime import datetime, date

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


spark = SparkSession.Builder().getOrCreate()

base_path = Path(__file__).parent
file_path = str(base_path) + '/data/raw.'


def load_raw_data(f):
    if f == 'csv':
        df = spark.read.csv(file_path + f, header=True)
    elif f == 'json':
        df = spark.read.json(file_path + f)
    df.show()
    return df

# df_csv = load_raw_data('csv')


# df_csv.select(df_csv.country, df_csv.population, df_csv.area).filter(df_csv.area >= 1000).show()


# df = spark.createDataFrame([
#     ['red', 'banana', 1, 10], ['blue', 'banana', 2, 20], ['red', 'carrot', 3, 30],
#     ['blue', 'grape', 4, 40], ['red', 'carrot', 5, 50], ['black', 'carrot', 6, 60],
#     ['red', 'banana', 7, 70], ['red', 'grape', 8, 80]], schema=['color', 'fruit', 'v1', 'v2'])
# df.show()
# df.printSchema()



# df = spark.createDataFrame([
#     (1, 2., 'string1', date(2000, 1, 1), datetime(2000, 1, 1, 12, 0)),
#     (2, 3., 'string2', date(2000, 2, 1), datetime(2000, 1, 2, 12, 0)),
#     (3, 4., 'string3', date(2000, 3, 1), datetime(2000, 1, 3, 12, 0))
# ], schema='a int, b float, c string, d date, e timestamp')
# df.show()
# df.printSchema()


df = spark.createDataFrame([['pasta', date(2023, 8, 1), 45.9],
                    ['pizza', date(2023 , 8, 5), 60.],
                    ['pizza', date(2023 , 8, 6), 6.6],
                    ['pizza', date(2023 , 8, 5), 76.],
                    ['meatballs', date(2023 , 7, 9), 89.],
                    ['meatballs', date(2023 , 7, 8), 90.],
                    ['meatballs', date(2023 , 7, 9), 100.],
                    ['bread', date(2023 , 6, 3), 7.5]],
                    schema ='food string, expires date, volume float')

df.show()
df.groupby('food','expires').count().show()


df.coalesce(1).write.csv(str(base_path) + '/data_out/pizza.csv', header=True)
