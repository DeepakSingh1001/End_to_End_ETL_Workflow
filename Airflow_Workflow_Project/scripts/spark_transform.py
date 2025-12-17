from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, to_timestamp, year

spark = (
    SparkSession.builder.appName("SalesCuratedETL")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

INPUT_PATH = "s3a://deepaks-source-bucket/Validated/"
OUTPUT_PATH = "s3a://deepaks-source-bucket/Curated/sales_parquet/"

# -------------------------
# Read validated CSVs
# -------------------------
df = spark.read.option("header", True).option("inferSchema", True).csv(INPUT_PATH)

# -------------------------
# Clean & cast columns
# -------------------------
df_clean = (
    df.withColumn("order_timestamp", to_timestamp(col("order_timestamp")))
    .withColumn("quantity", col("quantity").cast("int"))
    .withColumn("price", col("price").cast("double"))
)

# -------------------------
# Transformations
# -------------------------
df_transformed = (
    df_clean.withColumn("total_amount", col("quantity") * col("price"))
    .withColumn("year", year(col("order_timestamp")))
    .withColumn("month", month(col("order_timestamp")))
)

# -------------------------
# Write Parquet to S3
# -------------------------
df_transformed.write.mode("overwrite").partitionBy("year", "month").parquet(OUTPUT_PATH)

spark.stop()

print("✅ All files transformed and stored in Curated/")
