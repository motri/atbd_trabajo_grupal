from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, lit, sum as spark_sum

spark = SparkSession.builder \
    .appName("Women-Specific Diversity in R&D Analytics") \
    .getOrCreate()


parquet_path = "s3a://atbdusaspendingparquet/"
df = spark.read.option("recursiveFileLookup", "true").parquet(parquet_path)


for field in df.schema.fields:
    if field.dataType.typeName() in ["double", "integer"]:
        df = df.withColumn(field.name, col(field.name).cast("double"))


filtered_df = df.filter((col("action_date_fiscal_year") >= 2015) &
                        (col("action_date_fiscal_year") <= 2024) &
                        (col("recipient_country_code") == "USA"))


naics_rnd_2015_2017 = [541711, 541712]
naics_rnd_2018_2024 = [541713, 541714, 541715]

df_with_categories = filtered_df.withColumn(
    "industry_category",
    when(
        (col("naics_code").isin(naics_rnd_2015_2017)) & (col("action_date_fiscal_year") <= 2017), "R&D"
    ).when(
        (col("naics_code").isin(naics_rnd_2018_2024)) & (col("action_date_fiscal_year") >= 2018), "R&D"
    ).otherwise("Other Industries")
)

women_specific_columns = [
    "woman_owned_business",
    "women_owned_small_business",
    "economically_disadvantaged_women_owned_small_business",
    "joint_venture_women_owned_small_business"
]

for column in women_specific_columns:
    if column in df.columns:
        df_with_categories = df_with_categories.withColumn(column, col(column).cast("int"))

df_with_women_diversity = df_with_categories.withColumn(
    "is_women_diverse_business",
    coalesce(*[when(col(column) > 0, lit(1)) for column in women_specific_columns], lit(0))
)

aggregated_women_df = df_with_women_diversity.groupBy("action_date_fiscal_year", "industry_category") \
    .agg(
        spark_sum(when(col("is_women_diverse_business") == 1, 1).otherwise(0)).alias("women_diverse_business_count"),
        spark_sum(when(col("is_women_diverse_business") == 0, 1).otherwise(0)).alias("non_women_diverse_business_count"),
    )

hdfs_output_path = "hdfs://172.31.20.226:9000/user/ec2-user/analytics/women_diverse_business_analysis/"
s3_output_path = "s3a://atbd-results/women_diverse_business_analysis/"

aggregated_women_df.write.mode("overwrite").option("header", "true").csv(hdfs_output_path)
aggregated_women_df.write.mode("overwrite").option("header", "true").csv(s3_output_path)
