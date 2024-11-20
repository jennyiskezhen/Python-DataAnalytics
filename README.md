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
  - Join datasets: `data = df1.join(df2, how = "inner", on = ["col_name"])`
  - Struct - aggregate within aggregate: `df.groupBy("col_name").agg(max(struct(col("count"), col("genre"))).alias("col_name_new")`
  - Convert dataframe `df` to a list: `df.collect`, then can save the column in an array: `var = [row["col_name"] for row in df]`

- Keras library for API for DL
  - `input = Input(shape = (col_num,))`
  - `hidden_layer = Dense(64, activation = 'relu')(input)`
  - `output = Dense(1, activation = 'sigmoid')(hidden_layer)`
  - `Dropout(rate=0.5)(hidden_layer)`
  - `BatchNormalization()(hidden_layer)`
  - `model = Model()`
  - `model.compile()`
  - `model.fit()`
  - `model.evaluation()`