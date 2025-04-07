-- over time calculations
SELECT
EXTRACT(Year FROM order_date) as order_year, 
EXTRACT(MONTH FROM order_date) as order_month, 
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date is NOT NULL
GROUP BY EXTRACT(Year FROM order_date),EXTRACT(MONTH FROM order_date)
ORDER BY EXTRACT(Year FROM order_date),EXTRACT(MONTH FROM order_date)


SELECT
DATE_TRUNC('month',  order_date)::DATE AS order_date,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) as total_customers,
SUM(quantity) as total_quantity
FROM gold.fact_sales
WHERE order_date is NOT NULL
GROUP BY DATE_TRUNC('month',  order_date)::DATE
ORDER BY DATE_TRUNC('month',  order_date)::DATE

-- cumulitive analysis (running total by year or moving Average by month)

-- Calculate total sales per month
-- and runnig total sales overtime and moving average
SELECT order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) as running_total,
AVG(avg_price) OVER (ORDER BY order_date) as moving_avg_price
FROM(
SELECT 
DATE_TRUNC('month',order_date)::DATE as order_date,
SUM(sales_amount) as total_sales,
AVG(price) as avg_price
FROM gold.fact_sales
WHERE order_date is NOT NULL
GROUP BY DATE_TRUNC('month',order_date)::DATE
-- order BY DATE_TRUNC('month',order_date)::DATE
) t
