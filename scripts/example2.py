from pyspark.sql import SparkSession

# สร้าง SparkSession โดยไม่กำหนดการตั้งค่าการเชื่อมต่อในโค้ด
spark = SparkSession.builder \
    .appName("Read CSV from MinIO") \
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
