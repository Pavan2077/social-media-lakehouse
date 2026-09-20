from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    count,
    countDistinct,
    sum,
    when,
    round,
    col,
)

spark = SparkSession.builder.getOrCreate()

SILVER_TABLE = "workspace.default.social_media_silver"
GOLD_TABLE = "workspace.default.social_media_gold_daily"


# ---------------------------------------------------------
# 1. Read Silver
# ---------------------------------------------------------

silver_df = spark.table(SILVER_TABLE)


# ---------------------------------------------------------
# 2. Create analytics-ready Gold aggregation
# ---------------------------------------------------------

gold_df = (
    silver_df
    .groupBy(
        "event_date",
        "language",
        "primary_theme"
    )
    .agg(
        count("*").alias("post_count"),

        countDistinct("author_hash").alias("unique_authors"),

        round(avg("sentiment"), 4).alias("avg_sentiment"),

        sum(
            when(col("sentiment") > 0.05, 1).otherwise(0)
        ).alias("positive_posts"),

        sum(
            when(col("sentiment") < -0.05, 1).otherwise(0)
        ).alias("negative_posts"),

        sum(
            when(
                (col("sentiment") >= -0.05)
                & (col("sentiment") <= 0.05),
                1
            ).otherwise(0)
        ).alias("neutral_posts"),
    )
)


# ---------------------------------------------------------
# 3. Write Gold
# ---------------------------------------------------------

if not spark.catalog.tableExists(GOLD_TABLE):

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("event_date")
        .saveAsTable(GOLD_TABLE)
    )

    print("Gold table created with initial load.")

else:

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("replaceWhere", "event_date IS NOT NULL")
        .saveAsTable(GOLD_TABLE)
    )


print(f"Gold table updated.")