// Product details used for creating reports //


CREATE VIEW gold.report_products AS
with base_query AS (
SELECT 
p.product_key,
p.product_name,
p.category,
p.suncategory,
p.cost,
f.order_number,
f.order_date,
f.customer_key,
f.sales_amount,
f.quantity
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE order_date IS NOT NULL
), product_aggregation AS (
SELECT
product_key,
product_name,
category,
suncategory,
cost,
COUNT( DISTINCT order_number) AS total_orders,
MAX(order_date) AS last_order_date,
DATE_PART('year', AGE(MAX(order_date), MIN(order_date))) * 12 +
	DATE_PART('month', AGE(MAX(order_date), MIN(order_date))) AS lifespan,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
ROUND(AVG((sales_amount / NULLIF(quantity,0))::NUMERIC),1) AS avg_selling_price
FROM base_query
GROUP BY product_key,
		product_name,
		category,
		suncategory,
		cost
)

SELECT 
product_key,
product_name,
category,
suncategory,
cost,
last_order_date,
DATE_PART('month', AGE(CURRENT_DATE,last_order_date)) AS recency,
CASE WHEN total_sales > 50000 THEN 'High-Performer'
	WHEN total_sales >= 10000 THEN 'Mid-Range'
	ELSE 'Low-Performer'
END AS product_segment,
total_orders,
lifespan,
total_customers,
total_sales,
total_quantity,
avg_selling_price,
-- Average order revenue
CASE WHEN total_sales = 0 THEN 0 
	ELSE total_sales / total_orders
END AS average_order_value,
-- average monthly revenue
ROUND(CASE WHEN lifespan = 0 THEN total_sales
		Else total_sales / lifespan ::NUMERIC
END,2) AS avg_monthly_revenue

FROM product_aggregation
		
