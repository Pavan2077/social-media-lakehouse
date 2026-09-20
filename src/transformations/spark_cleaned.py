from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date, hour
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

spark = SparkSession.builder.getOrCreate()

bronze_table = "workspace.default.social_media_bronze"
silver_table = "workspace.default.social_media_silver"

df = spark.table(bronze_table)

silver_df = (
    df
    .withColumn(
        "event_timestamp",
        to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSSX")
    )
    .withColumn(
        "event_date",
        to_date(col("event_timestamp"))
    )
    .withColumn(
        "event_hour",
        hour(col("event_timestamp"))
    )
    .drop("date")
)

window_spec = Window.partitionBy("url").orderBy(
    col("ingested_at").desc()
)

source_df = (
    silver_df
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

if not spark.catalog.tableExists(silver_table):

    source_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(silver_table)

    print("Silver table created with initial load.")

else:

    target = DeltaTable.forName(spark, silver_table)

    (
        target.alias("target")
        .merge(
            source_df.alias("source"),
            "target.url = source.url"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    print("Silver table updated using MERGE.")

print("Running Silver data quality checks...")

quality_checks = {
    "row_count": spark.sql(
        f"SELECT COUNT(*) FROM {silver_table}"
    ).first()[0],

    "null_timestamps": spark.sql(
        f"""
        SELECT COUNT(*)
        FROM {silver_table}
        WHERE event_timestamp IS NULL
        """
    ).first()[0],

    "null_languages": spark.sql(
        f"""
        SELECT COUNT(*)
        FROM {silver_table}
        WHERE language IS NULL
        """
    ).first()[0],

    "null_sentiments": spark.sql(
        f"""
        SELECT COUNT(*)
        FROM {silver_table}
        WHERE sentiment IS NULL
        """
    ).first()[0],

    "invalid_sentiments": spark.sql(
        f"""
        SELECT COUNT(*)
        FROM {silver_table}
        WHERE sentiment < -1 OR sentiment > 1
        """
    ).first()[0]
}

print("Data quality results:")
print(quality_checks)

if quality_checks["row_count"] == 0:
    raise ValueError("Silver table is empty.")

if quality_checks["null_timestamps"] > 0:
    raise ValueError("Silver contains NULL event timestamps.")

if quality_checks["null_languages"] > 0:
    raise ValueError("Silver contains NULL languages.")

if quality_checks["null_sentiments"] > 0:
    raise ValueError("Silver contains NULL sentiments.")

if quality_checks["invalid_sentiments"] > 0:
    raise ValueError("Silver contains invalid sentiment values.")

print("All Silver data quality checks passed.")

print("Silver transformation completed.")