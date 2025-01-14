from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, sum as spark_sum

# Initialize Spark session
spark = SparkSession.builder \
    .appName("R&D Awards Per State") \
    .getOrCreate()

# Path to Parquet files
parquet_path = "s3a://atbdusaspendingparquet/"
df = spark.read.option("recursiveFileLookup", "true").parquet(parquet_path)

# Automatically detect numeric columns and cast them to double
for field in df.schema.fields:
    if field.dataType.typeName() in ["double", "integer"]:
        df = df.withColumn(field.name, col(field.name).cast("double"))

# Filter data for 2015-2024 and USA
filtered_df = df.filter((col("action_date_fiscal_year") >= 2015) &
                        (col("action_date_fiscal_year") <= 2024) &
                        (col("recipient_country_code") == "USA"))

# NAICS Codes for R&D based on years
naics_rnd_2015_2017 = [541711, 541712]
naics_rnd_2018_2024 = [541713, 541714, 541715]

# Filter for R&D-related NAICS codes and add an "R&D" category
rnd_awards = filtered_df.filter(
    ((col("naics_code").isin(naics_rnd_2015_2017)) & (col("action_date_fiscal_year") <= 2017)) |
    ((col("naics_code").isin(naics_rnd_2018_2024)) & (col("action_date_fiscal_year") >= 2018))
)

# Aggregate total obligations by state and year
state_rnd_awards = rnd_awards.groupBy("action_date_fiscal_year", "recipient_state_name") \
    .agg(
        spark_sum(col("federal_action_obligation")).alias("total_rnd_awards")
    )

# Save results to HDFS
hdfs_output_path = "hdfs://172.31.20.226:9000/user/ec2-user/analytics/rnd_awards_per_state"
state_rnd_awards.write.mode("overwrite").option("header", "true").csv(hdfs_output_path)

# Save results to S3
s3_output_path = "s3a://atbd-results/rnd_awards_per_state"
state_rnd_awards.write.mode("overwrite").option("header", "true").csv(s3_output_path)

print(f"Results saved to HDFS: {hdfs_output_path}")
print(f"Results saved to S3: {s3_output_path}")

