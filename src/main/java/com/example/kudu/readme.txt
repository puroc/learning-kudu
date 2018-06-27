【impala建表】kudu的表必须有主键，作为分区的字段需排在其他字段前面。

【range分区】（不推荐）
CREATE TABLE KUDU_WATER_HISTORY (
id STRING,
year INT,
device STRING,
reading INT,
time STRING,
PRIMARY KEY (id,year)
) PARTITION BY RANGE (year)
(
PARTITION VALUES < 2017,
PARTITION 2017 <= VALUES < 2018,
PARTITION 2018 <= VALUES
)
STORED AS KUDU
TBLPROPERTIES('kudu.master_addresses' = '10.10.30.200:7051');

【hash分区】（不推荐）
CREATE TABLE KUDU_WATER_HISTORY_PARTITION_BY_ID (
id STRING,
year INT,
device STRING,
reading INT,
time STRING,
PRIMARY KEY (id)
) PARTITION BY HASH (id) PARTITIONS 4
STORED AS KUDU
TBLPROPERTIES('kudu.master_addresses' = '10.10.30.200:7051');

【hash range混合分区】推荐是用混合分区方式
CREATE TABLE KUDU_WATER_HISTORY (
id STRING,
year INT,
device STRING,
reading INT,
time STRING,
PRIMARY KEY (id,device,year)
) PARTITION BY HASH (device) PARTITIONS 3,
RANGE (year)
(
PARTITION VALUE = 2016,
PARTITION VALUE = 2017,
PARTITION VALUE = 2018,
PARTITION VALUE = 2019
)
STORED AS KUDU
TBLPROPERTIES('kudu.master_addresses' = '10.10.30.200:7051');

CREATE TABLE DEVICE_KUDU (
id STRING,
device STRING,
name STRING,
orgId INT,
PRIMARY KEY (id)
) PARTITION BY HASH (id) PARTITIONS 4
STORED AS KUDU
TBLPROPERTIES('kudu.master_addresses' = '10.10.30.200:7051');

【增加分区】
ALTER TABLE KUDU_WATER_HISTORY ADD RANGE PARTITION VALUE = 2020;

【查询语句】

select
T_3C75F1.`device`,
year(T_3C75F1.`time`),
month(T_3C75F1.`time`),
sum(T_3C75F1.`reading`),
count(1)
from (select DEVICE_KUDU.device,reading,to_timestamp(time,'yyyy-MM-dd HH:mm:ss') as time from KUDU_WATER_HISTORY,DEVICE_KUDU where KUDU_WATER_HISTORY.device=DEVICE_KUDU.device) as `T_3C75F1`
group by
T_3C75F1.`device`,
year(T_3C75F1.`time`),
month(T_3C75F1.`time`);

耗时：DEVICE_KUDU表50条记录，KUDU_WATER_HISTORY表1亿条记录，执行上面的查询语句耗时12秒