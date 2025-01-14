from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, count, trim

# Initialize Spark session
spark = SparkSession.builder.appName("Group Startups per Year by Industry").getOrCreate()

# Load the filtered companies dataset
filtered_companies = spark.read.csv("s3a://private-investment/filtered_usa_companies.csv", header=True, inferSchema=True)

# Filter data for 2015-2024 based on Founded Date
filtered_startups = filtered_companies.filter(
    col("Founded Date").between(2015, 2024)
)

# Split the Industries column by comma and explode it into multiple rows
exploded_industries = filtered_startups.withColumn(
    "sub_industry", explode(split(col("Industries"), ","))
)

# Remove leading/trailing whitespaces in the sub_industry column
cleaned_data = exploded_industries.withColumn("sub_industry", trim(col("sub_industry")))

# Group by year and sub-industry to count the number of startups
industry_year_group = cleaned_data.groupBy(
    col("Founded Date").alias("year"),
    col("sub_industry").alias("industry")
).agg(
    count("*").alias("startup_count")
)

# Save results to HDFS
hdfs_output_path = "hdfs://172.31.20.226:9000/user/ec2-user/analytics/startups_per_industry_year"
industry_year_group.write.mode("overwrite").option("header", "true").csv(hdfs_output_path)

# Save results to S3
s3_output_path = "s3a://atbd-results/startups_per_industry_year"
industry_year_group.write.mode("overwrite").option("header", "true").csv(s3_output_path)

print(f"Results saved to HDFS: {hdfs_output_path}")
print(f"Results saved to S3: {s3_output_path}")

