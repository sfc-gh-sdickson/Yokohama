-- Yokohama Tires - Snowflake Intelligence Demo
-- 01_tables.sql: Core tables (normalized) in CORE schema

USE DATABASE IDENTIFIER($DB_NAME);
USE SCHEMA IDENTIFIER($SCH_CORE);

-- Dimension tables
CREATE OR REPLACE TABLE products (
  product_id STRING NOT NULL,
  sku STRING,
  model_name STRING,
  category STRING,           -- e.g., All-Season, Performance, Winter, Off-Road
  size STRING,               -- e.g., 225/45R17
  speed_rating STRING,       -- e.g., H, V, W
  msrp NUMBER(10,2),
  launch_date DATE,
  PRIMARY KEY (product_id)
);

CREATE OR REPLACE TABLE customers (
  customer_id STRING NOT NULL,
  first_name STRING,
  last_name STRING,
  email STRING,
  phone STRING,
  loyalty_tier STRING,
  segment STRING,            -- e.g., Retail, Fleet, Dealer
  home_zip STRING,
  home_state STRING,
  home_country STRING DEFAULT 'US',
  signup_date DATE,
  PRIMARY KEY (customer_id)
);

CREATE OR REPLACE TABLE stores (
  store_id STRING NOT NULL,
  store_name STRING,
  store_type STRING,         -- e.g., Company, Dealer, Online
  region STRING,             -- e.g., Northeast, Midwest, etc.
  state STRING,
  country STRING DEFAULT 'US',
  open_date DATE,
  PRIMARY KEY (store_id)
);

CREATE OR REPLACE TABLE calendar (
  date_key DATE NOT NULL,
  year INT,
  quarter INT,
  month INT,
  month_name STRING,
  day INT,
  day_of_week INT,
  day_name STRING,
  is_weekend BOOLEAN,
  PRIMARY KEY (date_key)
);

-- Fact tables
CREATE OR REPLACE TABLE sales_orders (
  order_id STRING NOT NULL,
  customer_id STRING,
  store_id STRING,
  order_date DATE,
  subtotal NUMBER(12,2),
  discount NUMBER(12,2),
  tax NUMBER(12,2),
  shipping NUMBER(12,2),
  total NUMBER(12,2),
  payment_method STRING,     -- e.g., Visa, ACH, Net30
  status STRING,             -- e.g., Completed, Refunded, Cancelled
  PRIMARY KEY (order_id)
);

CREATE OR REPLACE TABLE sales_order_items (
  order_item_id STRING NOT NULL,
  order_id STRING,
  product_id STRING,
  quantity INT,
  unit_price NUMBER(12,2),
  line_total NUMBER(12,2),
  PRIMARY KEY (order_item_id)
);

CREATE OR REPLACE TABLE tire_telemetry (
  telemetry_id STRING NOT NULL,
  customer_id STRING,
  product_id STRING,
  reading_ts TIMESTAMP_NTZ,
  miles INT,
  avg_speed_mph NUMBER(6,2),
  ambient_temp_f NUMBER(6,2),
  road_condition STRING,     -- Dry, Wet, Snow, Ice, Off-road
  tread_depth_32nds NUMBER(6,2),
  tire_pressure_psi NUMBER(6,2),
  location STRING,           -- e.g., front_left, front_right
  PRIMARY KEY (telemetry_id)
);

CREATE OR REPLACE TABLE warranty_claims (
  claim_id STRING NOT NULL,
  customer_id STRING,
  product_id STRING,
  claim_date DATE,
  claim_reason STRING,       -- e.g., Premature wear, Puncture, Defect
  resolution STRING,         -- e.g., Approved, Denied, Partial credit
  reimbursed_amount NUMBER(12,2),
  PRIMARY KEY (claim_id)
);

CREATE OR REPLACE TABLE service_events (
  service_id STRING NOT NULL,
  customer_id STRING,
  product_id STRING,
  service_date DATE,
  service_type STRING,       -- e.g., Rotation, Balance, Alignment, Repair
  store_id STRING,
  notes STRING,
  PRIMARY KEY (service_id)
);

-- Basic FKs (not enforced):
-- sales_orders.customer_id -> customers.customer_id
-- sales_orders.store_id -> stores.store_id
-- sales_order_items.order_id -> sales_orders.order_id
-- sales_order_items.product_id -> products.product_id
-- telemetry, claims, service reference customers/products


