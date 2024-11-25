import os
from pyspark.sql import SparkSession

# กำหนดค่า AWS Access Key และ Secret Key สำหรับการเข้าถึง MinIO
aws_access_key_id = "admin"
aws_secret_access_key = "password"
minio_endpoint = "http://minio:9000"

# สร้าง SparkSession พร้อมการตั้งค่าที่จำเป็นสำหรับการเชื่อมต่อกับ MinIO
spark = SparkSession.builder \
    .appName("App2") \
    .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
    .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint) \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# อ่านข้อมูล Parquet จาก MinIO เข้าเป็น DataFrame
df = spark.read.format("parquet") \
    .load("s3a://warehouse/wh/data_engineer_jobs.parquet")

# ลดจำนวน partition ให้เหลือ 1 เพื่อให้ได้ไฟล์ output เพียงไฟล์เดียว
temp_output_path = "s3a://warehouse/temp/de_jobs"

df.coalesce(1).write.format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(temp_output_path)

# หลังจากเขียนไฟล์เสร็จ ให้ใช้ shell command เพื่อเปลี่ยนชื่อไฟล์ output เป็น de_jobs.csv
# โปรดตรวจสอบว่า path นี้ตรงกับ path ของคุณ
os.system(f"mc mv minio/warehouse/temp/de_jobs/part-00000*.csv minio/warehouse/process_files/de_jobs.csv")

# ลบ temp directory หลังการคัดลอก
os.system("mc rm -r --force minio/warehouse/temp/")

# ปิด SparkSession
spark.stop()
