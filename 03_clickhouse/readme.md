 ## kafka integrate

 kafka --> pull kafka table --> pull MV --> pull storage table
### create database
CREATE DATABASE iot_db;
### consume kafka data 
CREATE TABLE  iot_db.datamicdemo1_raw (\
    topic String,\
    lot String,\
    data1 UInt32,\
    data2 UInt32,\
    data3 UInt32,\
    data4 UInt32,\
    data5 UInt32,\
    data6 UInt32,\
    data7 UInt32,\
    data8 UInt32,\
    data9 UInt32,\
    data10 UInt32\
)\
ENGINE = Kafka\
SETTINGS\
    kafka_broker_list = 'your-ip:29092,your-ip:39092,your-ip:49092',\
    kafka_topic_list = 'k_datamicdemo1',\
    kafka_group_name = 'ck_group1',\
    kafka_format = 'JSONEachRow',\
    kafka_num_consumers = 1;

### consume kafka data 

CREATE TABLE  iot_db.datamicdemo1(\
    event_time DateTime('Asia/Bangkok') DEFAULT now(),\
    topic String,\
    lot String,\
    data1 UInt32,\
    data2 UInt32,\
    data3 UInt32,\
    data4 UInt32,\
    data5 UInt32,\
    data6 UInt32,\
    data7 UInt32,\
    data8 UInt32,\
    data9 UInt32,\
    data10 UInt32\
)\
ENGINE = MergeTree()\
ORDER BY event_time;

### materialized View
CREATE MATERIALIZED VIEW iot_db.mv_kafka_ingest\
TO iot_db.datamicdemo1
AS
SELECT
    now() AS event_time,\
    topic,\
    lot,\
    data1,\
    data2,\
    data3,\
    data4,\
    data5,\
    data6,\
    data7,\
    data8,\
    data9,\
    data10\
FROM iot_db.datamicdemo1_raw;

### show data
SELECT * FROM iot_db.datamicdemo1;