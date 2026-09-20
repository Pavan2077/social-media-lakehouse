from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)
from datetime import datetime
from pyspark.sql.functions import current_timestamp, col,lit
import hashlib



spark = SparkSession.builder.getOrCreate()

RAW_PATH = "/Volumes/workspace/default/social_media_raw/"
INGESTION_LOG = "workspace.default.ingestion_log"
BRONZE_TABLE = "workspace.default.social_media_bronze"

# Create ingestion log if it doesn't exist
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {INGESTION_LOG} (
    file_name STRING,
    file_size BIGINT,
    file_modified_time TIMESTAMP,
    file_hash STRING,
    processed_at TIMESTAMP,
    status STRING
)
USING DELTA
""")

# 1. List files
files = dbutils.fs.ls(RAW_PATH)

parquet_files = [
    file for file in files
    if file.name.endswith(".parquet")
]

print("Parquet files found:")

for file in parquet_files:
    print(file.name, file.size)


# 2. Create stable file hash
def create_file_hash(file):
    value = f"{file.name}|{file.size}|{file.modificationTime}"
    return hashlib.sha256(value.encode()).hexdigest()


# 3. Read ingestion history
processed_hashes = {
    row.file_hash
    for row in (
        spark.table(INGESTION_LOG)
        .filter(col("status") == "SUCCESS")
        .select("file_hash")
        .collect()
    )
    if row.file_hash is not None
}


# 4. Identify new files
new_files = []

for file in parquet_files:

    file_hash = create_file_hash(file)

    if file_hash not in processed_hashes:
        new_files.append((file, file_hash))


print("New files:")

for file, file_hash in new_files:
    print(file.name, file_hash)


# 5. Stop if nothing new
if not new_files:
    print("No new files found.")
    exit()


# 6. Read only new files
new_file_paths = [
    file.path
    for file, _ in new_files
]

new_df = spark.read.parquet(*new_file_paths)

new_df = (
    new_df
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("ingested_at", current_timestamp())
)

print("New records:")
print(new_df.count())


# 7. Append to Bronze
(
    new_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(BRONZE_TABLE)
)


# 8. Record successfully processed files
log_rows = [
    (
        file.name,
        file.size,
        datetime.fromtimestamp(file.modificationTime / 1000),
        file_hash,
        "SUCCESS"
    )
    for file, file_hash in new_files
]

log_schema = StructType([
    StructField("file_name", StringType(), False),
    StructField("file_size", LongType(), True),
    StructField("file_modified_time", TimestampType(), True),
    StructField("file_hash", StringType(), False),
    StructField("status", StringType(), False)
])

log_df = (
    spark.createDataFrame(log_rows, log_schema)
    .withColumn("processed_at", current_timestamp())
    .select(
        "file_name",
        "file_size",
        "file_modified_time",
        "file_hash",
        "processed_at",
        "status"
    )
)

(
    log_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(INGESTION_LOG)
)

print("Incremental Bronze ingestion completed.")