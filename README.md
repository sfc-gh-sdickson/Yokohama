Snowflake Intelligence Demo: Yokohama Tires

This repo contains a repeatable, end-to-end demo showcasing Snowflake Intelligence with natural language over synthetic sales and tire usage data for Yokohama Tires.

Contents
- sql/00_setup.sql — create warehouse, roles, database, schemas, grants
- sql/01_tables.sql — create tables for products, customers, sales, tire_telemetry, service_events
- sql/02_load_synthetic.sql — generate deterministic synthetic data with SQL (no external deps)
- sql/03_semantics.sql — create semantic views and helper views for analysis; includes notes to create a Semantic View in UI
- sql/99_cleanup.sql — teardown script
- AGENT_SETUP.md — step-by-step to configure an Intelligence Agent to chat with your data

Prerequisites
- Snowflake account and role with privileges to create warehouses, roles, and objects (or adjust scripts to your role model)
- Snowsight access

Quickstart
1) Open Snowsight Worksheets.
2) Run files in order from the `sql` folder: 00_setup.sql → 01_tables.sql → 02_load_synthetic.sql → 03_semantics.sql
3) In Snowsight UI, create the Semantic View `YOKOHAMA_SEMANTIC` per AGENT_SETUP.md, then configure the Agent to use it.
4) Ask questions like:
   - "Show monthly sales revenue by product category for 2024."
   - "Which tire models show abnormal wear rate in the last 90 days?"
   - "What factors correlate with warranty claims?"

Notes
- Synthetic data is deterministic per run using seeded random generators to keep your demo consistent.
- Scripts are idempotent where practical (IF NOT EXISTS, CREATE OR REPLACE views) to support repeatability.
- You can safely rerun to refresh the demo.


