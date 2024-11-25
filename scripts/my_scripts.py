from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyApp").getOrCreate()

# ตัวอย่างการอ่านและแสดงข้อมูล
df = spark.read.parquet("s3a://warehouse/wh/yellow_tripdata_2021-04.parquet")
df.show()

spark.stop()
