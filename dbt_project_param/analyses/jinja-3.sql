{{ config(materialized='incremental', unique_key='sales_id') }}

select
    sales_id,
    date_sk,
    gross_amount
from {{ ref('bronze_sales') }}

{% if is_incremental() %}
    where date_sk > (select max(date_sk) from {{ this }})
{% endif %}
