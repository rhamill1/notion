
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


spark = SparkSession.Builder().getOrCreate()

base_path = Path(__file__).parent
csv_file_path = str(base_path) + '/data/raw.csv'
json_file_path = str(base_path) + '/data/raw.json'

def load_raw_json_data():
  raw_data_df = spark.read.json(json_file_path)
  raw_data_df.show()
  return raw_data_df

def load_raw_csv_data():
  raw_data_df = spark.read.csv(csv_file_path, header=True)
  raw_data_df.show()
  return raw_data_df

# j = load_raw_json_data()
df_c = load_raw_csv_data()


# df_c.select(df_c.population).show()
df_c.select(df_c.population, df_c.country).filter(df_c.population >= 10000000).show()

# df_c.groupby(df_c.transit_trips_per_capita).avg().show()

df_c.createOrReplaceTempView('countries')
spark.sql('SELECT COUNT(*) FROM countries').show()
