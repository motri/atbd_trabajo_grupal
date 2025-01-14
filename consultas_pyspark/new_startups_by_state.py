from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark Session
spark = SparkSession.builder.appName("Identificar Regiones con Nuevas Empresas de I+D").getOrCreate()

# Load filtered companies dataset
filtered_companies = spark.read.csv("s3a://private-investment/filtered_usa_companies.csv", header=True, inferSchema=True)

# Filter data for 2015-2024 based on Founded Date
filtered_rnd_companies = filtered_companies.filter(
    (col("Founded Date").between(2015, 2024))
)

# Group by state and year (Founded Date) to count the number of companies
state_year_counts = filtered_rnd_companies.groupBy(
    col("State").alias("state"), 
    col("Founded Date").alias("year")
).agg(count("*").alias("company_count"))

# Save results to HDFS
hdfs_output_path = "hdfs://172.31.20.226:9000/user/ec2-user/analytics/rnd_hubs"
state_year_counts.write.mode("overwrite").option("header", "true").csv(hdfs_output_path)

# Save results to S3
s3_output_path = "s3a://atbd-results/rnd_hubs"
state_year_counts.write.mode("overwrite").option("header", "true").csv(s3_output_path)

print(f"Results saved to HDFS: {hdfs_output_path}")
print(f"Results saved to S3: {s3_output_path}")

