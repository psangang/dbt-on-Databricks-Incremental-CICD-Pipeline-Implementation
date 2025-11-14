WITH sales as(
    select
        sales_id,
        gross_amount,
        product_sk,
        customer_sk,
        {{ multiply('unit_price','quantity')}} as calculated_gross_amount,
        payment_method
    from
        {{ ref('bronze_sales') }}

),

products as(

    select 
        product_sk,
        category
    from
        {{ ref('bronze_product') }}
),

customer as(
    select
        customer_sk,
        gender
    from
        {{ ref('bronze_customer') }}
),

joined_query as(
select
    sales.sales_id,
    sales.product_sk,
    sales.customer_sk,
    sales.gross_amount,
    sales.payment_method,
    products.category,
    customer.gender
from
    sales
join
    products on sales.product_sk = products.product_sk
join
    customer on customer.customer_sk = sales.customer_sk
),

aggregated AS (
    select
        category,
        gender,
        sum(gross_amount) as total_sales
    from joined_query
    group by category, gender
)

select
    category,
    gender,
    total_sales,
    rank() over (partition by category order by total_sales desc) as rk
from aggregated
order by category, total_sales desc

