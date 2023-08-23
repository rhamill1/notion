
## Local Environment Setup  |  M2 Macbook

<b>Context:</b> My machine already had Homebrew, Python and developer tools setup but did not have Java or PySpark.

<b>Java:</b> Up to date and most reliable downloads can be found at [oracle_java\_downloads](https://www.oracle.com/java/technologies/downloads/#jdk20-mac) <br>
For Apple chip Macs use ARM64 DMG Installer <br><br>
Confirm Java was correctly installed

```
$ java -version
java version "20.0.2" 2023-07-18
```

<br><b>PySpark</b><br>

```
$ mkdir repos/python_practice/notion
$ cd repos/python_practice/notion
$ virtualenv venv
$ venv/bin/activate
$ pip install pyspark
```

<b>Additional Python Environment</b><br>
Create directories for reading and writing data

```
$ mkdir data
$ mkdir data_out
```

## Why PySpark?
*   Distributed processing for handling very large data sets
*   Supports streaming data use case
*   Python >> common programming language in this space. Larger pool of candidates than Scala.
*   Has been around longer than Flink and has a broader community
*   Better fault tolerance and recover
*   Can run on RAM
*   SQL Contex >> Supports complex data manipulation

## Performance
*   Configuration Enhancments
    *   Parquet files. Partition by an appropriate date field.
    *   Specify write nodes. Collaborate with Data Infrastructure to understand cluster.

*   Data volume
    *   filter out-of-scope records immediately after injestion. Remove records that don't support the use case.

        ```
        df_import = spark.read.json(source_data/raw.json)
        df_stage = df_import.filter(df.country == 'United States')
        ```

## Business Considerations
*   Type 2 SCD for Financial use cases
*   Streaming vs. Batch >> Spark streaming: how up-to-date do our data customers need data? What is the business use case for this data set and how does that weigh against the cost of compute (and maybe storage)?
*   Timestamps >> how do we currate time and dates for our data customers? Use timezone and unix time to balance the least amount of fields while making it easy for our customers to query, and flexible for reporting and business processes.


## Helpful Snippets
```
df.columns
df.printSchema()
df.select('col1', 'col2').describe().show()
df.select(df_c.population, df_c.country).filter(df_c.population >= 10000000).show()
df.filter(df.country != 'England').groupby('country', 'status').sum('comments').orderBy('country', desc('sum(comments)')).show(truncate=False)

df = spark.createDataFrame(create_sample_data(1000), schema=['user_id', 'status', 'country', 'post_date', 'comments'])
df = spark.createDataFrame(create_sample_data(200),schema='created_on timestamp_ntz, request_id int, call_duration int, client int')
df.createOrReplaceTempView('posts')
spark.sql('SELECT user_id, status FROM posts;').show()
```
