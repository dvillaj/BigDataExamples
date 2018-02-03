DROP TABLE IF EXISTS retail.top10;

CREATE TABLE retail.top10 (   
    product_id INT,   
    product_name STRING,   
    revenue DOUBLE ) 
ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '|'
STORED AS TEXTFILE 
LOCATION '/user/hive/warehouse/retail.db/top10';