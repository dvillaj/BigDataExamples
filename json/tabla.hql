DROP TABLE IF EXISTS table_json;

CREATE EXTERNAL TABLE table_json (
    id STRING,
    maxdate STRING,
    mindate STRING,
    name STRING,
    uid STRING,
    datacoverage STRING
)
ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDePlus' 
LOCATION '/raw/json';

SELECT id, maxdate, mindate, name, uid, 
    cast(datacoverage as decimal(6,2)) as datacoverage
FROM table_json
LIMIT 10;
