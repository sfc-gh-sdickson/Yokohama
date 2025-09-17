Snowflake Intelligence Agent Setup (Yokohama Tires)

Use this guide to connect Snowflake Intelligence using a Semantic View plus supporting analysis views.

Prereqs
- Run `sql/00_setup.sql` → `01_tables.sql` → `02_load_synthetic.sql` → `03_semantics.sql` in Snowsight as a role that can create and grant objects.
- Know the values you used (role, warehouse, database, schemas).

Create the Semantic View (UI)
1) In Snowsight: Data → Semantic views → Create semantic view
2) Location: `YOKO_DEMO_DB.SEMANTICS` (or your names if changed)
3) Name: `YOKOHAMA_SEMANTIC`
4) Logical tables and joins:
   - Base: `fact_sales` with measures: SUM(line_total) as sales_revenue, SUM(quantity) as quantity_sold, COUNT(DISTINCT order_id) as orders_count, AVG order value as avg_order_value
   - Join `dim_product` on product_id; expose `model_name`, `category`, `size`, `speed_rating`
   - Join `dim_store` on store_id; expose `region`, `state`, `store_type`
   - Join `dim_date` on order_date; expose `year`, `quarter`, `month`
5) Save the semantic view.
6) Grants: Share the semantic view with `YOKO_DEMO_ROLE` (or your demo role) so the agent can use it.

Create an Agent in Snowsight UI
1) Open Snowsight → AI & ML → Agents → Create agent.
2) Location: choose schema `YOKO_DEMO_DB.SEMANTICS` (or your names if changed).
3) Name: `YOKOHAMA_TIRES_AGENT`. Display name: "Yokohama Tires Agent".
4) Cortex Analyst tool:
   - Click + Add under Cortex Analyst.
   - Semantic view: select `YOKOHAMA_SEMANTIC`.
   - Warehouse: `YOKO_DEMO_WH` (or your choice)
   - Save the tool.
5) Instructions (prompt):
   - You are a helpful data analyst for Yokohama Tires.
   - Prefer using the Semantic View; when needed, you may reference helper views `v_monthly_sales_by_category`, `v_claim_rate_by_model`, and `v_tread_wear_trend`.
   - When aggregating by time, use the date dimensions from the Semantic View.
6) Tools / Data access (optional): Add helper analysis views as read sources:
   - `v_monthly_sales_by_category`, `v_claim_rate_by_model`, `v_tread_wear_trend`
7) Access: Grant access to `YOKO_DEMO_ROLE` (and users who will demo). Save.
8) Test in the chat panel with questions below.

Optionally via SQL (if supported in your account)
-- Not all accounts support SQL creation of agents with Semantic Views. If available, consult docs for the latest syntax. Example only:
-- CREATE OR REPLACE AGENT YOKO_DEMO_DB.SEMANTICS.YOKOHAMA_TIRES_AGENT
--   NAME='Yokohama Tires Agent'
--   DESCRIPTION='Chat with Yokohama synthetic sales and usage data'
--   INSTRUCTIONS='You are a helpful data analyst...'
--   CORTEX_ANALYST = (SEMANTIC_VIEW='YOKO_DEMO_DB.SEMANTICS.YOKOHAMA_SEMANTIC', WAREHOUSE='YOKO_DEMO_WH');

Starter questions
- What are monthly sales revenues by category for the last 12 months? Show a trend.
- Which tire models have the highest warranty claim rate? Include counts and rate.
- Are there seasonal patterns in tread wear by model?
- Which regions and stores contribute most to sales revenue in 2024?
- Do lower tire pressures correlate with increased warranty claims?

Tips
- If your agent struggles, include object names (the Semantic View or helper views) in your question.
- Ensure your role has SELECT on the SEMANTICS schema and the Semantic View.
- If you changed object names, update the instructions accordingly.


