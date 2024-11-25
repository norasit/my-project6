#!/bin/bash

# เริ่มการทำงานของ Spark
start-master.sh -p 7077
start-worker.sh spark://spark-iceberg:7077
start-history-server.sh
start-thriftserver.sh  --driver-java-options "-Dderby.system.home=/tmp/derby"

# ตรวจสอบว่ามีคำสั่งพิเศษหรือไม่ ถ้ามีให้รันคำสั่งนั้น
if [[ $# -gt 0 ]] ; then
    exec "$@"
else
    # ถ้าไม่มีคำสั่งพิเศษ ให้ค้างโปรเซสไว้เพื่อไม่ให้คอนเทนเนอร์หยุดทำงาน
    tail -f /dev/null
fi
