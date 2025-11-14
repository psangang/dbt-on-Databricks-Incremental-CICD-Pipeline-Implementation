{{ config(
    materialized='incremental',
    unique_key='sales_id'
) }}

-- ================================================================
-- WHY WE USE A SEPARATE CTE FOR MAX(dsk)
-- ---------------------------------------------------------------
-- Databricks does NOT allow aggregate functions such as MAX(), MIN(),
-- SUM(), COUNT(), etc. inside WHERE clauses when the SQL is rewritten
-- as part of a MERGE statement (which dbt uses for incremental models).
--
-- Example of what FAILS in Databricks:
--     WHERE date_sk > (SELECT MAX(date_sk) FROM {{ this }})
--
-- Databricks returns:
--   INVALID_WHERE_CONDITION: aggregate functions not allowed here
--
-- Therefore, we must compute aggregates (MAX) OUTSIDE the WHERE clause.
-- We do this using a CTE ("last_run") so we can safely reference  
-- the value in a simple comparison.
-- ================================================================

with last_run as (
    -- Store the max value from the existing target table
    -- This CTE runs only during an incremental run ({{ this }} exists)
    select max(dsk) as max_dsk
    from {{ this }}
),

src as (
    -- Source dataset from your bronze table
    select
        sales_id,
        date_sk as dsk,
        gross_amount
    from {{ ref('bronze_sales') }}
)

-- ================================================================
-- MAIN SELECT
-- ---------------------------------------------------------------
-- During FIRST run:
--   - {{ this }} does not exist
--   - dbt runs full SELECT and creates the entire table
--
-- During INCREMENTAL runs:
--   - We apply the incremental filter using max_dsk from the CTE
--   - This avoids aggregates directly in the WHERE clause
--   - This is compatible with Databricks MERGE requirements
-- ================================================================
select
    src.*
from src
{% if is_incremental() %}
where src.dsk > (select max_dsk from last_run)
{% endif %}
