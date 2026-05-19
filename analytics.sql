-- Queries sobre kudu 

-- query 1: ingresos totales por pais 
SELECT 
    country_code, 
    ROUND(SUM(amount), 2) AS total_revenue
FROM orders
GROUP BY country_code
ORDER BY total_revenue DESC;


-- query 2: cantidad total de ordenes agrupadas por estado 
SELECT 
    status, 
    COUNT(*) AS total_orders
FROM orders
GROUP BY status;


-- query 3: ingresos totales por tipo de producto 
SELECT 
    product, 
    ROUND(SUM(amount), 2) AS total_revenue
FROM orders
GROUP BY product
ORDER BY total_revenue DESC;
