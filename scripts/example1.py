from pyspark.sql import SparkSession

# กำหนดค่า AWS Access Key และ Secret Key สำหรับการเข้าถึง MinIO
aws_access_key_id = "admin"
aws_secret_access_key = "password"
minio_endpoint = "http://minio:9000"

# สร้าง SparkSession พร้อมการตั้งค่าที่จำเป็นสำหรับการเชื่อมต่อกับ MinIO
spark = SparkSession.builder \
    .appName("Read CSV from MinIO") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# อ่านข้อมูล CSV จาก MinIO เข้าเป็น DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("s3a://warehouse/wh/data_science_job.csv")

# แสดง Schema ของ DataFrame
df.printSchema()

# แสดงข้อมูลตัวอย่าง
df.show(5)

# ปิด SparkSession
spark.stop()
