-- dim_date
CREATE OR REPLACE TABLE ecommerce_dataset.dim_date AS
SELECT DISTINCT
  FORMAT_DATE('%Y%m%d', DATE(TIMESTAMP(event_time))) AS date_id,
  DATE(TIMESTAMP(event_time)) AS full_date,
  EXTRACT(MONTH FROM TIMESTAMP(event_time)) AS month,
  EXTRACT(YEAR FROM TIMESTAMP(event_time)) AS year
FROM ecommerce_dataset.fact_sales;

-- dim_product
CREATE OR REPLACE TABLE ecommerce_dataset.dim_product AS
SELECT DISTINCT
  product_id,
  category_id
FROM ecommerce_dataset.fact_sales;

-- dim_customer
CREATE OR REPLACE TABLE ecommerce_dataset.dim_customer AS
SELECT DISTINCT
  user_id,
  user_session
FROM ecommerce_dataset.fact_sales;
