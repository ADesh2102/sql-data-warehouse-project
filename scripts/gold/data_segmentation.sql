-- Data Segmentataion 
-- Segment products into cost ranges and count how 
-- manny products fall into each segment
WITH product_segment AS (
SELECt 
product_key,
product_name,
cost,
CASE WHEN cost<100 THEN 'Below 100'
	WHEN cost between 100 and 500 THEN '100-500'
	WHEN COST BETWEEN 500 AND 1000 THEN '500-1000'
	ELSE 'Above 1000'
END cost_range 
FROM gold.dim_products)
SELECT 
cost_range,
COUNT(product_key) AS total_products
FROM product_segment
GROUP BY cost_range
order by total_products DESC


/* 
Group customer into 3 segments based on their spending behavior:
-VIP: Customers with atleast 12 months of history and spemding more than 5000.
-Regular: Customers with atleast 12 months of history but spending 5000 or less
-New: customers with a lifespan less tahn 12 months
find total number of customers by each group, calculate lifespan find first order and last order 
*/



WITH customer_spending AS(
SELECT 
c.customer_key,
SUM(f.sales_amount) AS total_spending,
MIN(order_date) as first_order,
MAX(order_date) as last_order,
DATE_PART('year', AGE(MAX(order_date), MIN(order_date))) * 12 +
  DATE_PART('month', AGE(MAX(order_date), MIN(order_date))) AS life_span
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key)

SELECT 
customer_status,
COUNT(customer_key) AS total_customers
FROM (
	SELECT 
	customer_key,
	CASE WHEN life_span >= 12 AND total_spending > 5000 THEN 'VIP'
		WHEN life_span >=12 AND total_spending <= 5000 THEN 'Regular'
		ELSE 'New'
	END AS customer_status
	FROM customer_spending
) t
GROUP BY customer_status
ORDER BY total_customers DESC

