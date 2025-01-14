from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, lit, sum as spark_sum

# Initialize Spark session
spark = SparkSession.builder \
    .appName("R&D and Diverse Business Analytics") \
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

# Add industry category based on year-specific NAICS codes
df_with_categories = filtered_df.withColumn(
    "industry_category",
    when(
        (col("naics_code").isin(naics_rnd_2015_2017)) & (col("action_date_fiscal_year") <= 2017), "R&D"
    ).when(
        (col("naics_code").isin(naics_rnd_2018_2024)) & (col("action_date_fiscal_year") >= 2018), "R&D"
    ).otherwise("Other Industries")
)

# List of diversity-related columns
diversity_columns = [
    "alaskan_native_corporation_owned_firm",
    "american_indian_owned_business",
    "indian_tribe_federally_recognized",
    "native_hawaiian_organization_owned_firm",
    "tribally_owned_firm",
    "veteran_owned_business",
    "service_disabled_veteran_owned_business",
    "woman_owned_business",
    "women_owned_small_business",
    "economically_disadvantaged_women_owned_small_business",
    "joint_venture_women_owned_small_business",
    "joint_venture_economic_disadvantaged_women_owned_small_bus",
    "minority_owned_business",
    "subcontinent_asian_asian_indian_american_owned_business",
    "asian_pacific_american_owned_business",
    "black_american_owned_business",
    "hispanic_american_owned_business",
    "native_american_owned_business",
    "other_minority_owned_business",
]

# Convert BOOLEAN diversity columns to INTEGER
for column in diversity_columns:
    if column in df.columns:
        df_with_categories = df_with_categories.withColumn(column, col(column).cast("int"))

# Add column to check if a business is diverse
df_with_diversity = df_with_categories.withColumn(
    "is_diverse_business",
    coalesce(*[when(col(column) > 0, lit(1)) for column in diversity_columns], lit(0))
)

# Aggregate diverse and non-diverse businesses by year and industry
aggregated_df = df_with_diversity.groupBy("action_date_fiscal_year", "industry_category") \
    .agg(
        spark_sum(when(col("is_diverse_business") == 1, 1).otherwise(0)).alias("diverse_business_count"),
        spark_sum(when(col("is_diverse_business") == 0, 1).otherwise(0)).alias("non_diverse_business_count"),
    )

# Save results to HDFS as CSV
output_path = "hdfs://172.31.20.226:9000/user/ec2-user/analytics/diverse_business_analysis/"
aggregated_df.write.mode("overwrite").option("header", "true").csv(output_path)

print(f"Results saved to: {output_path}")

