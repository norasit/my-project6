from pyspark.sql import SparkSession

# กำหนดค่า AWS Access Key และ Secret Key สำหรับการเข้าถึง MinIO
aws_access_key_id = "admin"
aws_secret_access_key = "password"
minio_endpoint = "http://minio:9000"

# สร้าง SparkSession พร้อมการตั้งค่าที่จำเป็นสำหรับการเชื่อมต่อกับ MinIO
spark = SparkSession.builder \
    .appName("App1") \
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
    .load("s3a://warehouse/raw_files/data_science_job.csv")

# แสดงข้อมูลภาพรวมก่อนการกรองข้อมูล
print("===== Data Overview (Before Filter) =====")
print("Schema:")
df.printSchema()                    # แสดง schema ของ DataFrame
print("\nColumns and Types:", df.dtypes)  # แสดงชื่อและชนิดข้อมูลของแต่ละคอลัมน์
print("\nTotal Rows:", df.count())        # แสดงจำนวนแถวทั้งหมด
print("\nStatistics:")
df.describe().show()                # แสดงสถิติพื้นฐาน
print("\nSample Data:")
df.show(5)                          # แสดงข้อมูลตัวอย่าง

# กรองข้อมูลโดยใช้ regex เพื่อหาคำว่า "Data Engineer" ในคอลัมน์ job_title โดยไม่คำนึงถึงตัวพิมพ์
filtered_df = df.filter(df["job_title"].rlike("(?i).*data engineer.*"))

# แสดงข้อมูลภาพรวมหลังการกรองข้อมูล
print("\n===== Data Overview (After Filter) =====")
print("Schema:")
filtered_df.printSchema()           # แสดง schema ของ DataFrame หลังกรอง
print("\nColumns and Types:", filtered_df.dtypes)
print("\nTotal Rows:", filtered_df.count())
print("\nStatistics:")
filtered_df.describe().show()
print("\nSample Data:")
filtered_df.show(5)

# บันทึกข้อมูลที่กรองแล้วเป็น Parquet ไปยัง bucket 'warehouse/wh'
filtered_df.write.format("parquet") \
    .mode("overwrite") \
    .save("s3a://warehouse/wh/data_engineer_jobs.parquet")

# ปิด SparkSession
spark.stop()
