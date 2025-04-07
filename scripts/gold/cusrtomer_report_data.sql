// CUSTOMER DETAILS use to create report //

CREATE VIEW gold.report_customers AS 
WITH base_query AS (
-- Retrieve core columns from tables age, sales etc
SELECT 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name,' ', c.last_name) AS customer_name,
c.birthdate,
DATE_PART('year',AGE(CURRENT_DATE,c.birthdate)) AS age
FROM 
gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
WHERE f.order_date IS NOT NULL
)
, customer_aggregation AS (
-- Agg customer level metrics
-- total orders, total sales, total quantity purchased, total products,lifespan(in monthd)

SELECT 
customer_key,
customer_number,
customer_name,
age,
COUNT(DISTINCT order_number) AS total_orders,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
COUNT(DISTINCT product_key) AS total_products,
MAX(order_date) AS last_order_date,
DATE_PART('year', AGE(MAX(order_date), MIN(order_date))) * 12 +
	DATE_PART('month', AGE(MAX(order_date), MIN(order_date))) AS life_span
FROM base_query
GROUP BY customer_key,
			customer_number,
			customer_name,
			age
)

SELECT 
customer_key,
customer_number,
customer_name,
age,
CASE WHEN age < 20 THEN 'Under 20'
	WHEN age between 20 and 29 THEN '20-29'
	WHEN age between 30 and 39 THEN '30-39'
	WHEN age between 40 and 49 THEN '40-49'
	ELSE '50 and above'
END AS age_group,
CASE WHEN life_span >= 12 AND total_sales > 5000 THEN 'VIP'
	 WHEN life_span >=12 AND total_sales <= 5000 THEN 'Regular'
	 ELSE 'New'
END AS customer_status,
last_order_date,
DATE_PART('month', AGE(CURRENT_DATE,last_order_date)) AS recency,
total_orders,
total_sales,
total_quantity,
total_products,
life_span,
-- compute average order values(AVO)
CASE WHEN total_sales = 0 THEN 0 
	ELSE total_sales / total_orders 
END AS average_order_value,
-- Compute average monthly spent
ROUND(CASE WHEN life_span = 0 THEN total_sales
		Else total_sales / life_span ::NUMERIC
END,2) AS avg_monthly_spent
FROM customer_aggregation
