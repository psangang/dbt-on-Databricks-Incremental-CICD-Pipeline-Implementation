{{config(materialized = 'table')}}

select 
*
from
{{ source('source', 'dim_product') }} 

