from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Startups by City") \
    .getOrCreate()

# Load datasets
companies_path = "s3a://private-investment/filtered_usa_companies.csv"
university_cities_path = "s3a://other-metrics/university_cities.csv"

companies_df = spark.read.csv(companies_path, header=True, inferSchema=True)
university_cities_df = spark.read.csv(university_cities_path, header=True, inferSchema=True)

# Use the Founded Date column directly as founded_year
companies_with_year = companies_df.withColumnRenamed("Founded Date", "founded_year")

# Join startups with university cities
startups_in_university_cities = companies_with_year.join(
    university_cities_df,
    (companies_with_year["City"] == university_cities_df["City"]) &
    (companies_with_year["State"] == university_cities_df["State"]),
    "inner"
).select(
    companies_with_year["City"].alias("Startup_City"),
    companies_with_year["State"].alias("Startup_State"),
    companies_with_year["founded_year"]
)

# Filter data for startups founded in the defined years
filtered_startups = startups_in_university_cities.filter(
    (col("founded_year") >= 2015) & (col("founded_year") <= 2024)
)

# Count startups by city
startups_per_city = filtered_startups.groupBy("Startup_City", "Startup_State").agg(
    count("*").alias("startups_count")
)

# Save results to HDFS and S3
hdfs_output_path = "hdfs://172.31.20.226:9000/user/ec2-user/analytics/startups_by_city/"
s3_output_path = "s3a://atbd-results/startups_by_city/"

startups_per_city.write.mode("overwrite").option("header", "true").csv(hdfs_output_path)
startups_per_city.write.mode("overwrite").option("header", "true").csv(s3_output_path)

print(f"Results saved to HDFS: {hdfs_output_path}")
print(f"Results saved to S3: {s3_output_path}")

