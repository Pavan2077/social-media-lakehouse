from pyspark.sql import SparkSession  

spark = SparkSession.builder.getOrCreate()

# data = [
#     ("Pavan", 27),
#     ("Rahul", 24),
#     ("Prajwal", 25)
# ]

# df = spark.createDataFrame(data, ["Name","Age"])

# df2 = df.filter(df.Age >= 25)

# df3 = df2.select("Name", "Age")

# df4 = df3.orderBy("Age")

# df5 = df.groupBy("Age").count()
# df5.show()

# df5.explain(True)

# df = spark.range(0, 10_000_000)

# df = df.withColumn(
#     "Age",
#     (df.id % 100)
# )

# df5 = df.groupBy("Age").count()

# df5.explain(True) 

# df = spark.range(0, 10_000_000)

# df = df.withColumn(
#     "Age",
#     df.id % 100
# )

# df5 = df.groupBy("Age").count()

# df5.show()

# spark.stop()
# spark.stop()

spark = SparkSession.builder.getOrCreate()

df = spark.range(0, 10_000_000)

df = spark.range(0, 10_000_000)

df_repartitioned = df.repartition(4)
df_repartitioned.explain(True)

df_coalesced = df.coalesce(4)
df_coalesced.explain(True)


# df_repartitioned.explain(True)
# df_coalesced.explain(True)
spark.stop()