## Data Analytics using Python

In this repository, I will be adding examples of Python project related to data analytics.

The tools include:

- Web Scraping
  - Beautiful Soup Object
  - `pd.read_html` function
- PySpark
  - `Spark = SparkSession.builder.appName("#").getOrCreate())`
  - `df = Spark.read.format("csv")`
    `.options(header='true', inferschema='true')`
    `.option('escape','"')`
    `.load("csv_file")`
  - `df.createOrReplaceTempView("u")`
  - `df.printSchema()`
  - `df.select("col_name")`
  - `df_new = df.groupBy("col1")`
    `.agg(sum("col2").alias("col2_new"))`
  - `df_new.orderBy("col2_new", ascending = False)`
    `.show(n)`
  - Convert to datatype date: `df.withColumn("col", to_date("col","m/d/yyyy"))`
  - `df.withColumn("Year", year("col"))`
  - Round values: `df.withColumn("col1", round(col1,2))`