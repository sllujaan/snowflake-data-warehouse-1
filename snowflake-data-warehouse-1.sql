-----------------------------aggregated streaming pipeline---------------------------------------------------

DROP DATABASE IF EXISTS DEMO;
CREATE DATABASE IF NOT EXISTS DEMO;
CREATE SCHEMA IF NOT EXISTS DEMO.PUBLIC;
USE SCHEMA DEMO.PUBLIC;



--------------------------------------------STAGGING PART-----------------------------------------------------


-- file format for csv files
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1;

-- create a stage for the raw data
CREATE OR REPLACE STAGE raw_data_stage file_format = my_csv_format directory = (enable=true);

-- create a table for the raw data to store
create or replace TABLE RAW_DATA (
	id VARCHAR(16777216),
	customer_id VARCHAR(16777216),
	customer_name VARCHAR(16777216),
	customer_age VARCHAR(16777216),
	customer_country VARCHAR(16777216),
	product_id VARCHAR(16777216),
	product_name VARCHAR(16777216),
	product_price VARCHAR(16777216),
	product_quantity VARCHAR(16777216),
	order_date VARCHAR(16777216),
    last_modified timestamp,    -- additional
    is_valid boolean            -- additional
);


-- now create a pipe for raw data to load automatically
create or replace pipe raw_data_load
as
copy into RAW_DATA
from
(select
    $1,  -- id
    $2,  -- customer_id
    $3,  -- customer_name
    $4,  -- customer_age
    $5,  -- customer_country
    $6,  -- product_id
    $7,  -- product_name
    $8,  -- product_price
    $9,  -- product_quantity
    $10, -- order_date
    METADATA$FILE_LAST_MODIFIED,  -- last_modified
    
    (try_cast($1 as int) is not null and
    try_cast($2 as int) is not null and
    try_cast($4 as int) is not null and
    try_cast($6 as int) is not null and
    try_cast($8 as decimal) is not null and
    try_cast($9 as int) is not null and
    try_cast($10 as date) is not null) and
    (($3 is not null and $3 != '') or
    ($5 is not null and $5 != '') or
    ($7 is not null and $7 != ''))    -- is_valid
from @raw_data_stage/sales);


-- refresh the pipe so that it will start ingesting data from the raw files
alter pipe raw_data_load refresh;

-- create a task to refresh the pipe created above
create or replace task raw_data_load_task
warehouse=FIRST_WH
schedule='1 minute'
as
alter pipe raw_data_load refresh;

--alter task raw_data_load_task resume;
--execute task raw_data_load_task;





------------------------------------dynamic tables-------------------------------------------------------------

create or replace dynamic table stg_customers
(
    customer_id INT,
    customer_name VARCHAR(16777216),
    customer_age VARCHAR(16777216),
    customer_country VARCHAR(16777216)
)
target_lag=downstream
warehouse=FIRST_WH
as
select 
    customer_id,
    customer_name,
    customer_age,
    customer_country
from RAW_DATA where is_valid = true;


create or replace dynamic table dim_customers
(
    id INT,
    name VARCHAR(16777216),
    age VARCHAR(16777216),
    country VARCHAR(16777216)
)
target_lag=downstream
warehouse=FIRST_WH
as
select 
    customer_id,
    customer_name,
    customer_age,
    customer_country
from stg_customers;



create or replace dynamic table stg_products
(
    product_id INT,
    product_name VARCHAR(16777216),
    product_price DECIMAL
)
target_lag=downstream
warehouse=FIRST_WH
as
select 
    product_id,
    product_name,
    product_price
from RAW_DATA where is_valid = true;

create or replace dynamic table dim_products
(
    id INT,
    name VARCHAR(16777216),
    price DECIMAL
)
target_lag=downstream
warehouse=FIRST_WH
as
select 
    product_id,
    product_name,
    product_price
from stg_products;



create or replace dynamic table stg_sales
(
    order_id INT,
    product_id INT,
    customer_id INT,
    product_quantity INT,
    order_date TIMESTAMP
)
target_lag=downstream
warehouse=FIRST_WH
as
select 
    id,
    product_id,
    customer_id,
    product_quantity,
    order_date
from RAW_DATA where is_valid = true;


create or replace dynamic table fact_sales
(
    order_id INT,
    product_id INT,
    customer_id INT,
    product_quantity INT,
    order_date TIMESTAMP
)
target_lag=downstream
warehouse=FIRST_WH
as
select 
    order_id,
    product_id,
    customer_id,
    product_quantity,
    order_date
from stg_sales;





-------------------------------------aggregated dynamic tables--------------------------------------


--------sales by year and month----------------
create or replace dynamic table sales_by_year_month
target_lag='1 minute'
warehouse=FIRST_WH
as
select
        YEAR(to_timestamp(sales.order_date)) as year,
        MONTH(to_timestamp(sales.order_date)) as month,
        SUM(to_decimal(sales.product_quantity) * to_decimal(product.price)) as total_sales,
        count(*) as total_orders
    from fact_sales sales
        left join dim_products product
            on sales.product_id = product.id
    group by year, month
    order by year, month;


--------sales by product----------------
create or replace dynamic table sales_by_product
target_lag='1 minute'
warehouse=FIRST_WH
as
select
        product.name as product_name,
        SUM(sales.product_quantity * product.price) as total_sales,
        count(*) as total_orders
    from fact_sales sales
        left join dim_products product
            on sales.product_id = product.id
    group by product_name
    order by product_name;


--------sales by customer----------------
create or replace dynamic table sales_by_customer
target_lag='1 minute'
warehouse=FIRST_WH
as
select
        customer.name as customer_name,
        SUM(sales.product_quantity * product.price) as total_sales,
        count(*) as total_orders
    from fact_sales sales
        left join dim_products product
            on sales.product_id = product.id
        left join dim_customers customer
            on sales.customer_id = customer.id
    group by customer_name
    order by customer_name;


--------sales by country----------------
create or replace dynamic table sales_by_country
target_lag='1 minute'
warehouse=FIRST_WH
as
select
        customer.country as country,
        SUM(sales.product_quantity * product.price) as total_sales,
        count(*) as total_orders
    from fact_sales sales
        left join dim_customers customer
            on sales.customer_id = customer.id
        left join dim_products product
            on sales.product_id = product.id
    group by country
    order by country;



--------sales by year----------------
create or replace dynamic table sales_by_year
target_lag='1 minute'
warehouse=FIRST_WH
as
select
        YEAR(sales.order_date) as year,
        SUM(sales.product_quantity * product.price) as total_sales,
        count(*) as total_orders
    from fact_sales sales
        left join dim_products product
            on sales.product_id = product.id
    group by year
    order by year;


--------sales by month----------------
create or replace dynamic table sales_by_month
target_lag='1 minute'
warehouse=FIRST_WH
as
select
        MONTH(sales.order_date) as month,
        SUM(sales.product_quantity * product.price) as total_sales,
        count(*) as total_orders
    from fact_sales sales
        left join dim_products product
            on sales.product_id = product.id
    group by month
    order by month;


--------sales by month and country----------------
create or replace dynamic table sales_by_month_country
target_lag='1 minute'
warehouse=FIRST_WH
as
select
        MONTH(sales.order_date) as month,
        customer.country as country,
        SUM(sales.product_quantity * product.price) as total_sales,
        count(*) as total_orders
    from fact_sales sales
        left join dim_products product
            on sales.product_id = product.id
        left join dim_customers customer
            on sales.customer_id = customer.id
    group by month, country
    order by month, country;



--------Top five selling products----------------
create or replace dynamic table top_five_selling_products
target_lag='1 minute'
warehouse=FIRST_WH
as
select * from sales_by_product
    order by total_sales desc limit 5;


--------Top five customers----------------
create or replace dynamic table top_five_customers
target_lag='1 minute'
warehouse=FIRST_WH
as
select * from sales_by_customer
    order by total_sales desc limit 5;


----------------useful queries------------
select * from raw_data;
select * from stg_customers;
select * from stg_products;
select * from stg_sales;
select * from dim_customers;
select * from dim_products;
select * from fact_sales;

select * from sales_by_country;
select * from sales_by_customer;
select * from sales_by_month;
select * from sales_by_month_country;
select * from sales_by_product;
select * from sales_by_year;
select * from sales_by_year_month;
select * from top_five_selling_products;
select * from top_five_customers;