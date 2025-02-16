from pyspark.sql import SparkSession

# Create a Spark session

spark = SparkSession.builder \
  .master("local") \
  .appName("IcebergLocalDevelopment") \
  .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2') \
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
  .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
  .config("spark.sql.catalog.local.type", "hadoop") \
  .config("spark.sql.catalog.local.warehouse", "spark-warehouse/iceberg") \
  .getOrCreate()

# Verify Spark version
spark.sql("""
  CREATE TABLE local.dev.users (
    id INT,
    name STRING,
    age INT
  ) USING iceberg""")

# Query the data
result = spark.sql("SELECT * FROM local.dev.users")
result.show()