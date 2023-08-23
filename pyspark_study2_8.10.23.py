
from datetime import datetime, date

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

spark = SparkSession.Builder().getOrCreate()

# spark.read.csv(path_file, header=True)



# df = spark.read.json(path_file)
# df.show()


# df.select(df.field1, df.field2).filter(df.field3 == 7).show()
# df.printSchema()


df = spark.createDataFrame([['pasta', date(2023, 8, 1), 45.9],
                    ['pizza', date(2023 , 8, 5), 6.],
                    ['meatballs', date(2023 , 7, 9), 89.],
                    ['bread', date(2023 , 6, 3), 7.5]],
                    schema = 'food string, expires date, volume float')

df.show()



df.groupby('food', 'expires').avg().show()
# .sum() and .count() work too!

# lessons
  # schema is declared as a string
  # df = spark.createDataFrame()
  # float values need a '.' or the task fails
  # dataFrames can be created from a list of lists or list of tuples


# from datetime import date, datetime



# df = spark.read.csv('path/file.csv', header=True)
# df = spark.read.json('path/file.json')

# df.show()
# df.select(df.column1, df.column2).filter(df.column3 % 2 == 0).show()

df.createOrReplaceTempView('df_table')
spark.sql('''
            SELECT *
              , CASE WHEN food == 'pizza' THEN True ELSE False END AS is_pizza
            FROM df_table;
          ''').show()

