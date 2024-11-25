#!/bin/sh
until (/usr/bin/mc config host add minio http://minio:9000 admin password); do 
    echo '...waiting...' 
    sleep 1 
done

# Comment out the line below to keep existing data
/usr/bin/mc rm -r --force minio/warehouse;

/usr/bin/mc mb minio/warehouse
/usr/bin/mc policy set public minio/warehouse
tail -f /dev/null
