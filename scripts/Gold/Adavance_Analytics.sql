SET SEARCH_PATH TO gold;

-- change over time analysis 
SELECT
    extract(YEAR FROM order_date) AS order_year,
    sum(sales_amount) AS total_order
FROM fact_sales
WHERE order_date IS NOT NULL
GROUP BY extract(YEAR FROM order_date)
ORDER BY extract(YEAR FROM order_date);

--Cumulative analysis
SELECT
    order_year,
    sum(total_order) OVER (ORDER BY order_year),
    round(avg(avg_sales) OVER (ORDER BY order_year), 2) AS avg
FROM (
    SELECT
        extract(YEAR FROM order_date) AS order_year,
        sum(sales_amount) AS total_order,
        round(avg(sales_amount), 2) AS avg_sales
    FROM fact_sales
    WHERE order_date IS NOT NULL
    GROUP BY extract(YEAR FROM order_date)
    ORDER BY extract(YEAR FROM order_date)
) AS temp;


--Performance analysis

WITH  yearly_prod_sales AS (
SELECT
    extract(YEAR FROM order_date) AS order_year,
    product_name AS product_name,
    sum(sales_amount) AS current_sales
FROM fact_sales f
LEFT JOIN dim_product p
ON f.product_key = p.product_key
WHERE order_date IS NOT NULL
GROUP BY extract(YEAR FROM order_date),  product_name
ORDER BY extract(YEAR FROM order_date))

SELECT
    order_year,
    product_name,
    current_sales,
    round(avg(current_sales) OVER (PARTITION BY product_name)) AS avg_sales,
    current_sales - round(avg(current_sales) OVER (PARTITION BY product_name)) AS diff_avg,
    CASE
        WHEN  current_sales - round(avg(current_sales) OVER (PARTITION BY product_name)) > 0 THEN 'Above avg'
        WHEN  current_sales - round(avg(current_sales) OVER (PARTITION BY product_name)) < 0 THEN 'Below avg'
        ELSE 'Avg'
    END avg_change,
    lag(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS Pre_year_sales,
    current_sales- lag(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_pre_year,
    CASE
        WHEN  current_sales- lag(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
        WHEN  current_sales- lag(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
        ELSE 'No change'
    END diff_change
FROM yearly_prod_sales
ORDER BY product_name, order_year;



-- part to whole analysis

WITH cte_sales AS (
SELECT
    categry,
    sum(sales_amount) AS total_sales
FROM fact_sales f
left join dim_product p
ON f.product_key = p.product_key
GROUP BY categry)

SELECT
    categry,
    sum(total_sales) OVER (),
    concat(round((total_sales/sum(total_sales) OVER ())*100, 2),'%') as percentage
FROM cte_sales
ORDER BY total_sales DESC;


-- Data Segmentation

-- seg 1

WITH segment AS (
SELECT
    product_key,
    product_name,
    cost,
    CASE
        WHEN cost < 100 THEN 'Below 100'
        WHEN cost BETWEEN 100 AND 500 THEN '100-500'
        WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
        ELSE 'Above 1000'
    END cost_range
FROM dim_product)
SELECT
    count(product_key) AS total_product,
    cost_range
FROM segment
GROUP BY cost_range
ORDER BY count(product_key) DESC;

-- seg 2

WITH customer_spending AS (
SELECT
    c.customer_id AS cust_id,
    SUM(f.sales_amount) AS total_spending,
    MIN(order_date) AS first_orderdate,
    MAX(order_date) AS last_orderdate,
    extract(YEAR from AGE(MAX(order_date), MIN(order_date))) * 12 + extract(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS Life_sapan
FROM fact_sales f
LEFT JOIN dim_customers c
ON f.customer_id = c.customer_id
GROUP BY c.customer_id)

SELECT
    count(cust_id) total_cust,
    CASE
        WHEN Life_sapan >= 12 and total_spending > 5000 THEN 'VIP'
        WHEN Life_sapan >= 12 and total_spending <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS Cut_seg
FROM customer_spending
GROUP BY Cut_seg
ORDER BY total_cust DESC;


-- Customer report

/*
 ========================================================
Customer Report
========================================================

Purpose:
 - This report consolidates key customer metrics and behaviors

Highlights:
 1. Gathers essential fields such as names, ages, and transaction details.
 2. Segments customers into categories (VIP, Regular, New) and age groups.
 3. Aggregates customer-level metrics:
    - total orders
    - total sales
    - total quantity purchased
    - total products
    - lifespan (in months)
 4. Calculates valuable KPIs:
    - recency (months since last order)
    - average order value
    - average monthly spend
========================================================

 */


CREATE OR REPLACE VIEW gold.customer_report AS (

WITH base_query AS (
SELECT
    concat(first_name,' ',last_name) AS customer_name,
    f.order_nbumber,
    f.product_key,
    f.order_date,
    f.sales_amount,
    f.quantity,
    c.customer_key,
    c.customer_number,
    c.birth_date,
    extract(YEAR FROM (age(now(), birth_date))) AS cust_age
FROM fact_sales f
LEFT JOIN dim_customers c
ON f.customer_id = c.customer_id
WHERE order_date IS NOT NULL),


    customer_agg AS (
SELECT
    b.customer_key,
    b.customer_number,
    b.customer_name,
    b.cust_age,
    count(DISTINCT order_nbumber) AS total_orders,
    sum(sales_amount) AS total_sales_amount,
    sum(quantity) AS total_quantity,
    count(DISTINCT product_key) AS total_products,
    max(order_date) AS last_order,
    extract(YEAR from AGE(MAX(order_date), MIN(order_date))) * 12 + extract(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS Life_sapan
FROM base_query b
GROUP BY
    customer_key,
    customer_number,
    customer_name,
    cust_age)

SELECT
    customer_key,
    customer_number,
    customer_name,
    CASE
        WHEN cust_age < 20 THEN 'Under 20'
        WHEN cust_age BETWEEN 20 AND 29 THEN '20-29'
        WHEN cust_age BETWEEN 30 AND 99 THEN '30-39'
        ELSE 'Above 40'
    END age_group,
     CASE
        WHEN Life_sapan >= 12 and total_sales_amount > 5000 THEN 'VIP'
        WHEN Life_sapan >= 12 and total_sales_amount <= 5000 THEN 'Regular'
        ELSE 'New'
    END AS Cut_seg,
    total_orders,
    total_sales_amount,
    total_quantity,
    total_products,
    last_order,
    age(now(),last_order) AS recency,
    Life_sapan,
    CASE
        WHEN total_orders = 0 THEN 0
        ELSE total_sales_amount/total_orders
    END AS avg_sales_val,

    CASE
        WHEN Life_sapan = 0  THEN total_sales_amount
        ELSE round((total_sales_amount/ Life_sapan),2)
    END AS avg_monthly_spend
FROM customer_agg

)
