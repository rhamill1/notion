
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


spark = SparkSession.Builder().getOrCreate()

df = spark.read.csv('path/file.csv', header=True)

df.show()
df.printSchema()


df.select('col1', 'col2').filter(df.col > 1).show()
df.groupby('col1', 'col2').count().show()


df.coalesce(1).write.csv('path/file', header=True)

df.createOrReplaceTempView('table1')

temp = spark.sql('SELECT * FROM table1;').show()

df1.join(df2, df2.customer = df1.customer, 'inner')




# I missed
#   creating the SparkSession.Builder().getOrCreate()
#   creating a dataFrame from lists
#     df = spark.createDataFrame([[1,2,3],[4,5,6]], schema=['age', 'min', 'max'])
