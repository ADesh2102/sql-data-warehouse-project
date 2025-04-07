-- part to whole analysis
 -- (measure * total(measure)) * 100

 -- which category contributes to overall sales
WITH category_sales AS(
SELECT 
category,
SUM(sales_amount) total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY category)
SELECT 
category, total_sales,
SUM(total_sales) OVER () AS overall_sales,
CONCAT(ROUND((total_sales / SUM(total_sales) OVER ()) * 100,2),'%') as percent_sales
FROM category_sales
ORDER BY total_sales DESC
