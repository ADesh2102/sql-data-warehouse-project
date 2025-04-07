-- performance analysis
-- compare current sales with avg sales performancee and compare with last years sales
WITH current_sales_years AS(
SELECT 
EXTRACT (YEAR FROM f.order_date) AS order_year,
p.product_name,
SUM(f.sales_amount) as current_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE f.order_date is NOT NULL
GROUP BY 
EXTRACT(YEAR FROM f.order_date),
p.product_name
)

SELECT 
order_year,
product_name,
current_sales,
AVG(current_sales) OVER(PARTITION BY product_name) AS avg_sales,
current_sales - AVG(current_sales) OVER(PARTITION BY product_name) AS avg_diff,
CASE WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) > 0 THEN 'Above Average'
	WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) < 0 THEN 'Below Average'
		ELSE 'Average'
END AS avg_change,
-- year over oear
LAG(current_sales) OVER ( PARTITION BY product_name ORDER BY order_year) as py_sales,
current_sales - LAG(current_sales) OVER ( PARTITION BY product_name ORDER BY order_year) AS diff_py,
CASE WHEN current_sales - LAG(current_sales) OVER ( PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Increase'
	WHEN current_sales - LAG(current_sales) OVER ( PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Decrease'
		ELSE 'No change'
END AS py_change
FROM current_sales_years
ORDER BY product_name, order_year
