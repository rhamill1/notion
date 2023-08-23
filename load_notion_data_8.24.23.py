
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datetime import datetime, date
from pathlib import Path

spark = SparkSession.Builder().getOrCreate()
base_path = str(Path(__file__).parent)


def load_data_from_file(f):
    f_type = f.split('.')[-1]
    if f_type == 'csv':
        return spark.read.csv(base_path + '/data/' + f, header=True)
    if f_type == 'json':
        return spark.read.json(base_path + '/data/' + f)
    else:
        print('unsupported file format')


df = load_data_from_file('raw.json')
df.show()
df.printSchema()
