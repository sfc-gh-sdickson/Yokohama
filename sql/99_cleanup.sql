-- Yokohama Tires - Snowflake Intelligence Demo
-- 99_cleanup.sql: Drop demo objects (safe to run if you used defaults)

SET ROLE_NAME = 'YOKO_DEMO_ROLE';
SET WH_NAME   = 'YOKO_DEMO_WH';
SET DB_NAME   = 'YOKO_DEMO_DB';
SET SCH_CORE  = 'CORE';
SET SCH_SEM   = 'SEMANTICS';

-- Drop agent (if created via SQL and exists) - UI-created agents can be dropped in Snowsight
-- DROP AGENT IF EXISTS IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_SEM)||'.YOKOHAMA_TIRES_AGENT';
-- Drop semantic view (if created via SQL) - UI-created semantic views can be deleted in Snowsight
-- DROP SEMANTIC VIEW IF EXISTS IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_SEM)||'.YOKOHAMA_SEMANTIC';

-- Drop schemas and database
DROP SCHEMA IF EXISTS IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_SEM) CASCADE;
DROP SCHEMA IF EXISTS IDENTIFIER($DB_NAME)||'.'||IDENTIFIER($SCH_CORE) CASCADE;
DROP DATABASE IF EXISTS IDENTIFIER($DB_NAME);

-- Drop warehouse
DROP WAREHOUSE IF EXISTS IDENTIFIER($WH_NAME);

-- Drop role (requires SECURITYADMIN/ACCOUNTADMIN)
DROP ROLE IF EXISTS IDENTIFIER($ROLE_NAME);


