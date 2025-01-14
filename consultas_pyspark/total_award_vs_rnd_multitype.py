from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.functions import col, when, sum as spark_sum

# Initialize Spark session
spark = SparkSession.builder \
    .appName("R&D and Industry Breakdown Analytics (USA Only)") \
    .getOrCreate()

# Path to the folder containing Parquet files
parquet_path = "s3a://atbdusaspendingparquet/"

# Read all Parquet files in the directory into a DataFrame
df = spark.read.option("recursiveFileLookup", "true").parquet(parquet_path)

# Automatically detect numeric columns and cast them to double
for field in df.schema.fields:
    if isinstance(field.dataType, (DoubleType, IntegerType)):
        df = df.withColumn(field.name, col(field.name).cast("double"))

# Debugging: Print schema after casting
df.printSchema()

# Filter data for 2015-2024 and limit to USA
filtered_df = df.filter((col("action_date_fiscal_year") >= 2015) &
                        (col("action_date_fiscal_year") <= 2024) &
                        (col("recipient_country_code") == "USA"))

# NAICS Codes for R&D
naics_rnd = {
    541713: "Biotechnology",
    541714: "Physical & Engineering Research",
    541715: "IT Research & Development"
}

# Add a column to identify R&D industries
df_with_categories = filtered_df.withColumn(
    "industry_category",
    when(col("naics_code").isin(541713, 541714, 541715), col("naics_code").cast("string"))
    .otherwise("Other Industries")
)

# Aggregate total obligations by year and industry category
aggregated_df = df_with_categories.groupBy("action_date_fiscal_year", "industry_category") \
    .agg(spark_sum("federal_action_obligation").alias("total_obligation"))

# Save results to HDFS in CSV format
output_path = "hdfs://172.31.20.226:9000/user/ec2-user/analytics/rnd_with_breakdown_usa.csv"
aggregated_df.write.mode("overwrite").option("header", "true").csv(output_path)

print(f"Results saved to: {output_path}")

