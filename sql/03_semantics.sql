-- Yokohama Tires - Snowflake Intelligence Demo
-- 03_semantics.sql: Star-style semantic views to enable natural language analytics

USE DATABASE IDENTIFIER($DB_NAME);
USE SCHEMA IDENTIFIER($SCH_SEM);

-- Semantic View (new method for Cortex Analyst)
-- Create a Semantic View named YOKOHAMA_SEMANTIC in this schema via Snowsight:
--   Data → Semantic views → Create semantic view
--   Location: YOKO_DEMO_DB.SEMANTICS (or your names)
--   Name: YOKOHAMA_SEMANTIC
--   Base logical tables and joins:
--     - fact_sales (base): measures SUM(line_total) as sales_revenue, SUM(quantity) as quantity_sold,
--                          COUNT(DISTINCT order_id) as orders_count, AVG order value
--     - dim_product on product_id; expose model_name, category, size, speed_rating
--     - dim_store on store_id; expose region, state, store_type
--     - dim_date on order_date; expose year, quarter, month
--   Save and grant usage/select to YOKO_DEMO_ROLE.

-- View: dim_date
CREATE OR REPLACE VIEW dim_date AS
SELECT date_key,
       year,
       quarter,
       month,
       month_name,
       day,
       day_of_week,
       day_name,
       is_weekend
FROM IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_CORE)||'.calendar';

-- View: dim_product
CREATE OR REPLACE VIEW dim_product AS
SELECT product_id,
       sku,
       model_name,
       category,
       size,
       speed_rating,
       msrp,
       launch_date
FROM IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_CORE)||'.products';

-- View: dim_customer
CREATE OR REPLACE VIEW dim_customer AS
SELECT customer_id,
       first_name,
       last_name,
       email,
       phone,
       loyalty_tier,
       segment,
       home_zip,
       home_state,
       home_country,
       signup_date
FROM IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_CORE)||'.customers';

-- View: dim_store
CREATE OR REPLACE VIEW dim_store AS
SELECT store_id,
       store_name,
       store_type,
       region,
       state,
       country,
       open_date
FROM IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_CORE)||'.stores';

-- View: fact_sales (grain: order item)
CREATE OR REPLACE VIEW fact_sales AS
SELECT soi.order_item_id,
       so.order_id,
       so.customer_id,
       so.store_id,
       so.order_date,
       soi.product_id,
       soi.quantity,
       soi.unit_price,
       soi.line_total,
       so.total AS order_total,
       so.status,
       so.payment_method
FROM IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_CORE)||'.sales_order_items' soi
JOIN IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_CORE)||'.sales_orders' so
  ON so.order_id = soi.order_id;

-- View: fact_telemetry
CREATE OR REPLACE VIEW fact_telemetry AS
SELECT telemetry_id,
       customer_id,
       product_id,
       CAST(reading_ts AS TIMESTAMP_NTZ) AS reading_ts,
       CAST(reading_ts AS DATE) AS reading_date,
       miles,
       avg_speed_mph,
       ambient_temp_f,
       road_condition,
       tread_depth_32nds,
       tire_pressure_psi,
       location
FROM IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_CORE)||'.tire_telemetry';

-- View: fact_warranty_claim
CREATE OR REPLACE VIEW fact_warranty_claim AS
SELECT claim_id,
       customer_id,
       product_id,
       claim_date,
       claim_reason,
       resolution,
       reimbursed_amount
FROM IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_CORE)||'.warranty_claims';

-- View: fact_service
CREATE OR REPLACE VIEW fact_service AS
SELECT service_id,
       customer_id,
       product_id,
       service_date,
       service_type,
       store_id,
       notes
FROM IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_CORE)||'.service_events';

-- Convenience aggregates for NL questions
CREATE OR REPLACE VIEW v_monthly_sales_by_category AS
SELECT d.year,
       d.month,
       dp.category,
       ROUND(SUM(fs.line_total),2) AS sales_revenue
FROM fact_sales fs
JOIN dim_product dp ON dp.product_id = fs.product_id
JOIN dim_date d ON d.date_key = fs.order_date
GROUP BY d.year, d.month, dp.category;

CREATE OR REPLACE VIEW v_claim_rate_by_model AS
WITH sales AS (
  SELECT product_id, COUNT(DISTINCT order_id) AS orders
  FROM fact_sales
  GROUP BY product_id
), claims AS (
  SELECT product_id, COUNT(*) AS claim_count
  FROM fact_warranty_claim
  GROUP BY product_id
)
SELECT dp.model_name,
       dp.category,
       COALESCE(c.claim_count,0) AS claim_count,
       COALESCE(s.orders,0) AS orders,
       CASE WHEN COALESCE(s.orders,0)=0 THEN 0 ELSE ROUND(100.0 * c.claim_count / s.orders,2) END AS claim_rate_pct
FROM dim_product dp
LEFT JOIN sales s ON s.product_id = dp.product_id
LEFT JOIN claims c ON c.product_id = dp.product_id
ORDER BY claim_rate_pct DESC;

CREATE OR REPLACE VIEW v_tread_wear_trend AS
SELECT dp.model_name,
       dp.category,
       DATE_TRUNC(month, ft.reading_ts) AS month,
       AVG(ft.tread_depth_32nds) AS avg_tread_depth_32nds
FROM fact_telemetry ft
JOIN dim_product dp ON dp.product_id = ft.product_id
GROUP BY dp.model_name, dp.category, DATE_TRUNC(month, ft.reading_ts);

-- Semantic View (SQL) - new method for Cortex Analyst
-- If your account supports SEMANTIC VIEW DDL, run this to create the semantic model.
-- Otherwise, use the Snowsight UI as described above.
CREATE OR REPLACE SEMANTIC VIEW YOKOHAMA_SEMANTIC
AS
(
  TABLES
  (
    {
      NAME = 'FACT_SALES',
      BASE_TABLE = 'FACT_SALES',
      PRIMARY_KEY = 'ORDER_ITEM_ID',
      DIMENSIONS = ['ORDER_DATE','PAYMENT_METHOD','STATUS','PRODUCT_ID','STORE_ID','CUSTOMER_ID'],
      MEASURES =
      [
        { NAME='SALES_REVENUE', EXPRESSION='SUM(LINE_TOTAL)', DESCRIPTION='Sum of line totals' },
        { NAME='QUANTITY_SOLD', EXPRESSION='SUM(QUANTITY)', DESCRIPTION='Units sold' },
        { NAME='ORDERS_COUNT', EXPRESSION='COUNT(DISTINCT ORDER_ID)', DESCRIPTION='Distinct orders' },
        { NAME='AVG_ORDER_VALUE', EXPRESSION='SUM(LINE_TOTAL)/NULLIF(COUNT(DISTINCT ORDER_ID),0)', DESCRIPTION='Revenue per order' }
      ]
    },
    {
      NAME = 'DIM_PRODUCT',
      BASE_TABLE = 'DIM_PRODUCT',
      PRIMARY_KEY = 'PRODUCT_ID',
      DIMENSIONS = ['PRODUCT_ID','MODEL_NAME','CATEGORY','SIZE','SPEED_RATING']
    },
    {
      NAME = 'DIM_STORE',
      BASE_TABLE = 'DIM_STORE',
      PRIMARY_KEY = 'STORE_ID',
      DIMENSIONS = ['STORE_ID','REGION','STATE','STORE_TYPE']
    },
    {
      NAME = 'DIM_DATE',
      BASE_TABLE = 'DIM_DATE',
      PRIMARY_KEY = 'DATE_KEY',
      DIMENSIONS = ['DATE_KEY','YEAR','QUARTER','MONTH','MONTH_NAME','DAY_OF_WEEK','DAY_NAME']
    }
  ),

  RELATIONSHIPS
  (
    {
      NAME = 'FACT_TO_PRODUCT',
      TABLES = ['FACT_SALES','DIM_PRODUCT'],
      JOIN_CONDITION = 'FACT_SALES.PRODUCT_ID = DIM_PRODUCT.PRODUCT_ID',
      TYPE = 'MANY_TO_ONE'
    },
    {
      NAME = 'FACT_TO_STORE',
      TABLES = ['FACT_SALES','DIM_STORE'],
      JOIN_CONDITION = 'FACT_SALES.STORE_ID = DIM_STORE.STORE_ID',
      TYPE = 'MANY_TO_ONE'
    },
    {
      NAME = 'FACT_TO_DATE',
      TABLES = ['FACT_SALES','DIM_DATE'],
      JOIN_CONDITION = 'FACT_SALES.ORDER_DATE = DIM_DATE.DATE_KEY',
      TYPE = 'MANY_TO_ONE'
    }
  ),

  METRICS
  (
    { NAME='TOTAL_REVENUE', EXPRESSION='SUM(FACT_SALES.LINE_TOTAL)', DESCRIPTION='Total revenue' },
    { NAME='TOTAL_UNITS', EXPRESSION='SUM(FACT_SALES.QUANTITY)', DESCRIPTION='Total units sold' },
    { NAME='DISTINCT_ORDERS', EXPRESSION='COUNT(DISTINCT FACT_SALES.ORDER_ID)', DESCRIPTION='Orders' },
    { NAME='AVERAGE_ORDER_VALUE', EXPRESSION='SUM(FACT_SALES.LINE_TOTAL)/NULLIF(COUNT(DISTINCT FACT_SALES.ORDER_ID),0)', DESCRIPTION='AOV' }
  ),

  DIMENSIONS
  (
    { NAME='PRODUCT_CATEGORY', EXPRESSION='DIM_PRODUCT.CATEGORY' },
    { NAME='PRODUCT_MODEL', EXPRESSION='DIM_PRODUCT.MODEL_NAME' },
    { NAME='STORE_REGION', EXPRESSION='DIM_STORE.REGION' },
    { NAME='STORE_STATE', EXPRESSION='DIM_STORE.STATE' },
    { NAME='ORDER_YEAR', EXPRESSION='DIM_DATE.YEAR' },
    { NAME='ORDER_MONTH', EXPRESSION='DIM_DATE.MONTH' }
  )
);

-- Optional: If supported
-- GRANT USAGE ON SEMANTIC VIEW YOKOHAMA_SEMANTIC TO ROLE IDENTIFIER($ROLE_NAME);

-- Grant select on SEMANTICS to demo role
GRANT USAGE ON SCHEMA IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_SEM) TO ROLE IDENTIFIER($ROLE_NAME);
GRANT SELECT ON ALL VIEWS IN SCHEMA IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_SEM) TO ROLE IDENTIFIER($ROLE_NAME);
GRANT SELECT ON FUTURE VIEWS IN SCHEMA IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_SEM) TO ROLE IDENTIFIER($ROLE_NAME);


