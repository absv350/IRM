use dm_integrated_risk_metrics;

create external table

lookup
(

LOOKUP_TYPE	VARCHAR(30),
LOOKUP_CODE	VARCHAR(30),
LOOKUP_DESC	VARCHAR(128)

)

partitioned by  (enceladus_info_date string, enceladus_info_version int)
row format delimited
fields terminated by ','
stored as parquet
location '/bigdatahdfs/project/irm/publish/SANCTIONS/lookup';
