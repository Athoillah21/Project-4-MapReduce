-- make postgre in docker
-- docker run --name postgress -e POSTGRES_USER=athok -e POSTGRES_PASSWORD=password -p 1234:5432 -d postgres


-- DDL productfilter
create table productfilter (
product_id text,
product_name text,
product_category text,
price text
)

create table agregatedate (
purchase_date text,
total_order int
)