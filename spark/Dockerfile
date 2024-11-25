FROM python:3.10-bullseye

# ติดตั้ง dependencies พื้นฐานและ Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      openjdk-11-jdk \
      build-essential \
      software-properties-common \
      ssh && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# คัดลอก requirements.txt และติดตั้ง dependencies
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Add scala kernel via spylon-kernel
RUN python3 -m spylon_kernel install

# Download and install IJava jupyter kernel
RUN curl https://github.com/SpencerPark/IJava/releases/download/v1.3.0/ijava-1.3.0.zip -Lo ijava-1.3.0.zip \
  && unzip ijava-1.3.0.zip \
  && python3 install.py --sys-prefix \
  && rm ijava-1.3.0.zip

# ตั้งค่า Spark
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

WORKDIR ${SPARK_HOME}

# ตั้งค่ารุ่น Spark และ Iceberg
ENV SPARK_VERSION=3.5.3
ENV SPARK_MAJOR_VERSION=3.5
ENV ICEBERG_VERSION=1.6.0

# ดาวน์โหลด Spark
RUN mkdir -p ${SPARK_HOME} \
 && curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

# ดาวน์โหลด Iceberg Spark runtime
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

# ดาวน์โหลด Hadoop AWS และ dependencies
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
 && curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar

# เพิ่ม Iceberg AWS Bundle (สำคัญสำหรับ S3FileIO)
RUN curl -L https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar -o /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Add iceberg spark runtime jar to IJava classpath
ENV IJAVA_CLASSPATH=/opt/spark/jars/*

RUN mkdir -p /home/iceberg/data \
 && curl https://data.cityofnewyork.us/resource/tg4x-b46p.json > /home/iceberg/data/nyc_film_permits.json \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-04.parquet -o /home/iceberg/data/yellow_tripdata_2022-04.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-03.parquet -o /home/iceberg/data/yellow_tripdata_2022-03.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-02.parquet -o /home/iceberg/data/yellow_tripdata_2022-02.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet -o /home/iceberg/data/yellow_tripdata_2022-01.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-12.parquet -o /home/iceberg/data/yellow_tripdata_2021-12.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-11.parquet -o /home/iceberg/data/yellow_tripdata_2021-11.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-10.parquet -o /home/iceberg/data/yellow_tripdata_2021-10.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-09.parquet -o /home/iceberg/data/yellow_tripdata_2021-09.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-08.parquet -o /home/iceberg/data/yellow_tripdata_2021-08.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-07.parquet -o /home/iceberg/data/yellow_tripdata_2021-07.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-06.parquet -o /home/iceberg/data/yellow_tripdata_2021-06.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-05.parquet -o /home/iceberg/data/yellow_tripdata_2021-05.parquet \
 && curl https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-04.parquet -o /home/iceberg/data/yellow_tripdata_2021-04.parquet

# สร้างไดเรกทอรีที่จำเป็น
# RUN mkdir -p /home/iceberg/localwarehouse /home/iceberg/warehouse /home/iceberg/spark-events
RUN mkdir -p /home/iceberg/localwarehouse /home/iceberg/notebooks /home/iceberg/warehouse /home/iceberg/spark-events /home/iceberg

# Add a notebook command
RUN echo '#! /bin/sh' >> /bin/notebook \
 && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-notebook' >> /bin/notebook \
 && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --NotebookApp.token='' --NotebookApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/notebook \
 && echo "pyspark" >> /bin/notebook \
 && chmod u+x /bin/notebook

# Add a pyspark-notebook command (alias for notebook command for backwards-compatibility)
RUN echo '#! /bin/sh' >> /bin/pyspark-notebook \
 && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-notebook' >> /bin/pyspark-notebook \
 && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --NotebookApp.token='' --NotebookApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/pyspark-notebook \
 && echo "pyspark" >> /bin/pyspark-notebook \
 && chmod u+x /bin/pyspark-notebook

RUN mkdir -p /root/.ipython/profile_default/startup
COPY ipython/startup/00-prettytables.py /root/.ipython/profile_default/startup
COPY ipython/startup/README /root/.ipython/profile_default/startup

# คัดลอกไฟล์ตั้งค่า
COPY .pyiceberg.yaml /root/.pyiceberg.yaml
COPY spark-defaults.conf /opt/spark/conf
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

# คัดลอก entrypoint.sh
COPY entrypoint.sh .

# ตั้งค่า entrypoint และ CMD
ENTRYPOINT ["./entrypoint.sh"]
CMD ["notebook"]
