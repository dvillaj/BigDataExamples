use retail_db;

drop table if exists retail_db.top10;

create table retail_db.top10 (
    product_id int,
    product_name varchar(200),
    revenue double
);