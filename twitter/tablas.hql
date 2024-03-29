DROP DATABASE IF EXISTS twitter CASCADE;

CREATE DATABASE twitter;

DROP TABLE IF EXISTS twitter.tweets;

CREATE EXTERNAL TABLE twitter.tweets (
   id BIGINT,
   created_at STRING,
   source STRING,
   favorited BOOLEAN,
   retweeted_status STRUCT<
     text:STRING,
     user:STRUCT<screen_name:STRING,name:STRING>,
     retweet_count:INT>,
   entities STRUCT<
     urls:ARRAY<STRUCT<expanded_url:STRING>>,
     user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
     hashtags:ARRAY<STRUCT<text:STRING>>>,
   text STRING,
   retweet_count INT,
   user STRUCT<
     screen_name:STRING,
     name:STRING,
     friends_count:INT,
     followers_count:INT,
     statuses_count:INT,
     verified:BOOLEAN,
     utc_offset:INT,
     time_zone:STRING>,
   in_reply_to_screen_name STRING
 ) 
 ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDePlus'
 LOCATION '/raw/tweets';
 