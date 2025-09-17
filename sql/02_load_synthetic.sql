-- Yokohama Tires - Snowflake Intelligence Demo
-- 02_load_synthetic.sql: Deterministic synthetic data generation (no external deps)

USE DATABASE IDENTIFIER($DB_NAME);
USE SCHEMA IDENTIFIER($SCH_CORE);

-- Re-seed by truncating (safe re-run)
TRUNCATE TABLE IF EXISTS calendar;
TRUNCATE TABLE IF EXISTS products;
TRUNCATE TABLE IF EXISTS customers;
TRUNCATE TABLE IF EXISTS stores;
TRUNCATE TABLE IF EXISTS sales_order_items;
TRUNCATE TABLE IF EXISTS sales_orders;
TRUNCATE TABLE IF EXISTS tire_telemetry;
TRUNCATE TABLE IF EXISTS warranty_claims;
TRUNCATE TABLE IF EXISTS service_events;

-- Utility: deterministic random function via HASH of inputs
-- Note: Use MOD on TO_NUMBER(LEFT(TO_CHAR(HASH(...)), 18)) to control ranges

-- 1) Calendar: last 3 years up to today
WITH bounds AS (
  SELECT DATEADD(year, -3, CURRENT_DATE()) AS start_date, CURRENT_DATE() AS end_date
), seq AS (
  SELECT DATEADD(day, seq4(), (SELECT start_date FROM bounds)) AS d
  FROM TABLE(GENERATOR(ROWCOUNT => 3*366))
)
INSERT INTO calendar
SELECT d AS date_key,
       YEAR(d) AS year,
       CEIL(MONTH(d)/3.0) AS quarter,
       MONTH(d) AS month,
       TO_CHAR(d, 'MON') AS month_name,
       DAY(d) AS day,
       DAYOFWEEK(d) AS day_of_week,
       TO_CHAR(d, 'DY') AS day_name,
       CASE WHEN DAYOFWEEK(d) IN (6,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM seq
WHERE d BETWEEN (SELECT start_date FROM bounds) AND (SELECT end_date FROM bounds);

-- 2) Product catalog: 40 SKUs across 4 categories
WITH nums AS (
  SELECT ROW_NUMBER() OVER (ORDER BY seq4()) AS n
  FROM TABLE(GENERATOR(ROWCOUNT => 40))
), base AS (
  SELECT n,
         'PRD-'||LPAD(n::STRING, 4, '0') AS product_id,
         'SKU-'||LPAD(n::STRING, 5, '0') AS sku,
         CASE (1 + (n % 4))
           WHEN 1 THEN 'All-Season'
           WHEN 2 THEN 'Performance'
           WHEN 3 THEN 'Winter'
           ELSE 'Off-Road'
         END AS category,
         (200 + (n % 101))||'/'||(35 + (n % 35))||'R'||(15 + (n % 7)) AS size,
         CASE (1 + (n % 5)) WHEN 1 THEN 'H' WHEN 2 THEN 'V' WHEN 3 THEN 'W' WHEN 4 THEN 'Y' ELSE 'T' END AS speed_rating,
         TO_DATE('2022-01-01') + (n % 720) AS launch_date,
         80 + (n % 121) AS msrp_base
  FROM nums
)
INSERT INTO products
SELECT product_id,
       sku,
       'Yokohama Model '||LPAD(n::STRING, 3, '0') AS model_name,
       category,
       size,
       speed_rating,
       (msrp_base + (CASE category WHEN 'Performance' THEN 60 WHEN 'Off-Road' THEN 40 WHEN 'Winter' THEN 30 ELSE 0 END))::NUMBER(10,2) AS msrp,
       launch_date
FROM base;

-- 3) Customers: 3,000 mixed retail/fleet
WITH nums AS (
  SELECT ROW_NUMBER() OVER (ORDER BY seq4()) AS n
  FROM TABLE(GENERATOR(ROWCOUNT => 3000))
), rand AS (
  SELECT n,
         'CUST-'||LPAD(n::STRING, 6, '0') AS customer_id,
         CASE WHEN (n % 2)=0 THEN 'Retail' ELSE 'Fleet' END AS segment,
         CASE WHEN (n % 5)=0 THEN 'Gold' WHEN (n % 3)=0 THEN 'Silver' ELSE 'Bronze' END AS loyalty_tier,
         LPAD((10000 + (n % 90000))::STRING, 5, '0') AS home_zip,
         CASE (1 + (n % 6))
           WHEN 1 THEN 'CA' WHEN 2 THEN 'TX' WHEN 3 THEN 'NY' WHEN 4 THEN 'IL' WHEN 5 THEN 'FL' ELSE 'OH' END AS home_state,
         DATEADD(day, -1 * (n % 1000), CURRENT_DATE()) AS signup_date
  FROM nums
)
INSERT INTO customers
SELECT customer_id,
       'First'||n AS first_name,
       'Last'||n AS last_name,
       LOWER(customer_id)||'@example.com' AS email,
       '555-'||LPAD((1000 + (n % 9000))::STRING, 4, '0') AS phone,
       loyalty_tier,
       segment,
       home_zip,
       home_state,
       'US' AS home_country,
       signup_date
FROM rand;

-- 4) Stores: 120 across US regions
WITH nums AS (
  SELECT ROW_NUMBER() OVER (ORDER BY seq4()) AS n
  FROM TABLE(GENERATOR(ROWCOUNT => 120))
)
INSERT INTO stores
SELECT 'STR-'||LPAD(n::STRING, 4, '0') AS store_id,
       'Store '||n AS store_name,
       CASE (1 + (n % 3)) WHEN 1 THEN 'Company' WHEN 2 THEN 'Dealer' ELSE 'Online' END AS store_type,
       CASE (1 + (n % 5)) WHEN 1 THEN 'Northeast' WHEN 2 THEN 'Midwest' WHEN 3 THEN 'South' WHEN 4 THEN 'West' ELSE 'Mountain' END AS region,
       CASE (1 + (n % 8)) WHEN 1 THEN 'NY' WHEN 2 THEN 'IL' WHEN 3 THEN 'TX' WHEN 4 THEN 'FL' WHEN 5 THEN 'CA' WHEN 6 THEN 'CO' WHEN 7 THEN 'WA' ELSE 'AZ' END AS state,
       'US' AS country,
       TO_DATE('2020-01-01') + (n % 1200) AS open_date
FROM nums;

-- 5) Sales orders and items: ~60,000 orders across 3 years
-- Order header volume
SET order_count = 60000;

WITH seq AS (
  SELECT ROW_NUMBER() OVER (ORDER BY seq4()) AS n
  FROM TABLE(GENERATOR(ROWCOUNT => $order_count))
), dist AS (
  SELECT n,
         'ORD-'||LPAD(n::STRING, 8, '0') AS order_id,
         'CUST-'||LPAD(((1 + (n % 3000))::STRING), 6, '0') AS customer_id,
         'STR-'||LPAD(((1 + (n % 120))::STRING), 4, '0') AS store_id,
         (SELECT MIN(date_key) FROM calendar) + (n % DATEDIFF(day, (SELECT MIN(date_key) FROM calendar), (SELECT MAX(date_key) FROM calendar))) AS order_date,
         CASE (1 + (n % 4)) WHEN 1 THEN 'Visa' WHEN 2 THEN 'ACH' WHEN 3 THEN 'Net30' ELSE 'Mastercard' END AS payment_method,
         CASE WHEN (n % 97)=0 THEN 'Refunded' WHEN (n % 53)=0 THEN 'Cancelled' ELSE 'Completed' END AS status,
         1 + (n % 6) AS item_count
  FROM seq
)
INSERT INTO sales_orders
SELECT order_id,
       customer_id,
       store_id,
       order_date,
       0 AS subtotal,
       0 AS discount,
       0 AS tax,
       0 AS shipping,
       0 AS total,
       payment_method,
       status
FROM dist;

-- Items
WITH orders AS (
  SELECT order_id, item_count, n
  FROM (
    SELECT ROW_NUMBER() OVER (ORDER BY order_id) AS n, order_id, item_count
    FROM (
      SELECT order_id,
             CASE WHEN status IN ('Cancelled') THEN 0 ELSE item_count END AS item_count
      FROM sales_orders
    )
  )
), exp AS (
  SELECT o.order_id,
         o.n,
         seq4() AS r
  FROM orders o,
       LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, o.item_count))
), items AS (
  SELECT e.order_id,
         ('ITM-'||LPAD((ROW_NUMBER() OVER (ORDER BY e.order_id, e.r))::STRING, 10, '0')) AS order_item_id,
         'PRD-'||LPAD(((1 + (ABS(HASH(e.order_id||'-'||e.r)) % 40))::STRING), 4, '0') AS product_id,
         1 + (ABS(HASH(e.order_id||'-q-'||e.r)) % 4) AS quantity,
         80 + (ABS(HASH(e.order_id||'-p-'||e.r)) % 180) AS unit_price
  FROM exp e
)
INSERT INTO sales_order_items
SELECT order_item_id,
       order_id,
       product_id,
       quantity,
       unit_price,
       (quantity * unit_price)::NUMBER(12,2) AS line_total
FROM items;

-- Aggregate back to headers
UPDATE sales_orders s
SET subtotal = x.subtotal,
    discount = x.discount,
    tax = x.tax,
    shipping = x.shipping,
    total = x.total
FROM (
  SELECT soi.order_id,
         SUM(soi.line_total) AS subtotal,
         ROUND(SUM(soi.line_total) * 0.05, 2) AS discount,
         ROUND((SUM(soi.line_total) - ROUND(SUM(soi.line_total) * 0.05, 2)) * 0.07, 2) AS tax,
         9.99 AS shipping,
         ROUND((SUM(soi.line_total) - ROUND(SUM(soi.line_total) * 0.05, 2)) * 1.07 + 9.99, 2) AS total
  FROM sales_order_items soi
  GROUP BY soi.order_id
) x
WHERE s.order_id = x.order_id;

-- 6) Telemetry: sample 200k readings with wear and pressure trends
SET telemetry_count = 200000;

WITH seq AS (
  SELECT ROW_NUMBER() OVER (ORDER BY seq4()) AS n
  FROM TABLE(GENERATOR(ROWCOUNT => $telemetry_count))
), base AS (
  SELECT n,
         'TEL-'||LPAD(n::STRING, 9, '0') AS telemetry_id,
         'CUST-'||LPAD(((1 + (n % 3000))::STRING), 6, '0') AS customer_id,
         'PRD-'||LPAD(((1 + (n % 40))::STRING), 4, '0') AS product_id,
         (SELECT MIN(date_key) FROM calendar)::TIMESTAMP_NTZ + (n %  (DATEDIFF(day,(SELECT MIN(date_key) FROM calendar),(SELECT MAX(date_key) FROM calendar)) * 24)) * 3600 AS reading_ts,
         10 + (n % 90) AS miles,
         15 + (n % 70) AS avg_speed_mph,
         -10 + (n % 120) AS ambient_temp_f,
         CASE (1 + (n % 5)) WHEN 1 THEN 'Dry' WHEN 2 THEN 'Wet' WHEN 3 THEN 'Snow' WHEN 4 THEN 'Ice' ELSE 'Off-road' END AS road_condition,
         (12 - (n % 10) * 0.2) AS tread_depth_32nds,
         (33 + ((n % 10) * 0.3)) AS tire_pressure_psi,
         CASE (1 + (n % 4)) WHEN 1 THEN 'front_left' WHEN 2 THEN 'front_right' WHEN 3 THEN 'rear_left' ELSE 'rear_right' END AS location
  FROM seq
)
INSERT INTO tire_telemetry
SELECT telemetry_id, customer_id, product_id, reading_ts, miles, avg_speed_mph, ambient_temp_f,
       road_condition, tread_depth_32nds, tire_pressure_psi, location
FROM base;

-- 7) Service events: ~20k
SET service_count = 20000;
WITH seq AS (
  SELECT ROW_NUMBER() OVER (ORDER BY seq4()) AS n
  FROM TABLE(GENERATOR(ROWCOUNT => $service_count))
)
INSERT INTO service_events
SELECT 'SVC-'||LPAD(n::STRING, 8, '0') AS service_id,
       'CUST-'||LPAD(((1 + (n % 3000))::STRING), 6, '0') AS customer_id,
       'PRD-'||LPAD(((1 + (n % 40))::STRING), 4, '0') AS product_id,
       (SELECT MIN(date_key) FROM calendar) + (n % DATEDIFF(day, (SELECT MIN(date_key) FROM calendar), (SELECT MAX(date_key) FROM calendar))) AS service_date,
       CASE (1 + (n % 4)) WHEN 1 THEN 'Rotation' WHEN 2 THEN 'Balance' WHEN 3 THEN 'Alignment' ELSE 'Repair' END AS service_type,
       'STR-'||LPAD(((1 + (n % 120))::STRING), 4, '0') AS store_id,
       NULL AS notes;

-- 8) Warranty claims: ~4k with outcomes
SET claim_count = 4000;
WITH seq AS (
  SELECT ROW_NUMBER() OVER (ORDER BY seq4()) AS n
  FROM TABLE(GENERATOR(ROWCOUNT => $claim_count))
)
INSERT INTO warranty_claims
SELECT 'CLM-'||LPAD(n::STRING, 8, '0') AS claim_id,
       'CUST-'||LPAD(((1 + (n % 3000))::STRING), 6, '0') AS customer_id,
       'PRD-'||LPAD(((1 + (n % 40))::STRING), 4, '0') AS product_id,
       (SELECT MIN(date_key) FROM calendar) + (n % DATEDIFF(day, (SELECT MIN(date_key) FROM calendar), (SELECT MAX(date_key) FROM calendar))) AS claim_date,
       CASE (1 + (n % 5)) WHEN 1 THEN 'Premature wear' WHEN 2 THEN 'Puncture' WHEN 3 THEN 'Defect' WHEN 4 THEN 'Sidewall' ELSE 'Other' END AS claim_reason,
       CASE (1 + (n % 5)) WHEN 1 THEN 'Approved' WHEN 2 THEN 'Denied' WHEN 3 THEN 'Approved' WHEN 4 THEN 'Partial credit' ELSE 'Denied' END AS resolution,
       ROUND((50 + (n % 300)) * CASE WHEN (n % 7)=0 THEN 0.0 WHEN (n % 11)=0 THEN 0.5 ELSE 1.0 END, 2) AS reimbursed_amount;

-- Row counts
SELECT 'calendar' AS table_name, COUNT(*) AS rows FROM calendar
UNION ALL SELECT 'products', COUNT(*) FROM products
UNION ALL SELECT 'customers', COUNT(*) FROM customers
UNION ALL SELECT 'stores', COUNT(*) FROM stores
UNION ALL SELECT 'sales_orders', COUNT(*) FROM sales_orders
UNION ALL SELECT 'sales_order_items', COUNT(*) FROM sales_order_items
UNION ALL SELECT 'tire_telemetry', COUNT(*) FROM tire_telemetry
UNION ALL SELECT 'service_events', COUNT(*) FROM service_events
UNION ALL SELECT 'warranty_claims', COUNT(*) FROM warranty_claims
ORDER BY table_name;


