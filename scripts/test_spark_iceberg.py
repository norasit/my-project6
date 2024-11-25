import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

catalog_name = os.environ.get("CATALOG_NAME", "demo")
warehouse_bucket = os.environ.get("WAREHOUSE_BUCKET", "warehouse")
minio_endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "password")

# สร้าง SparkSession พร้อมกำหนดค่า catalog
# spark = SparkSession.builder \
#     .appName("IcebergTest") \
#     .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
#     .config("spark.sql.catalog.demo.type", "hadoop") \
#     .config("spark.sql.catalog.demo.warehouse", "s3a://warehouse/wh/") \
#     .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
#     .config("spark.sql.catalog.demo.s3.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.access.key", "admin") \
#     .config("spark.hadoop.fs.s3a.secret.key", "password") \
#     .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
#     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
#     .config("spark.sql.defaultCatalog", "demo") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("IcebergTest") \
    .config(f"spark.sql.catalog.{catalog_name}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_name}.type", "hadoop") \
    .config(f"spark.sql.catalog.{catalog_name}.warehouse", f"s3a://{warehouse_bucket}/wh/") \
    .config(f"spark.sql.catalog.{catalog_name}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config(f"spark.sql.catalog.{catalog_name}.s3.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.access.key", access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.region", "us-east-1") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.defaultCatalog", catalog_name) \
    .getOrCreate()

# สร้าง schema และ DataFrame ตัวอย่าง
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True)
])

data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
df = spark.createDataFrame(data, schema)

# สร้างฐานข้อมูลและ Table ใน Iceberg Catalog (หากยังไม่มี)
spark.sql("CREATE DATABASE IF NOT EXISTS demo.db")
spark.sql("CREATE TABLE IF NOT EXISTS demo.db.test_table (id INT, name STRING) USING iceberg")

# เขียน DataFrame ลงใน Iceberg Table
df.write.format("iceberg").mode("append").save("demo.db.test_table")

# อ่านข้อมูลจาก Iceberg Table
df_read = spark.read.format("iceberg").load("demo.db.test_table")
df_read.show()

# ปิด SparkSession
spark.stop()
