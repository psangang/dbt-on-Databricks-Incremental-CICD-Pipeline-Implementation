{% set inc_flag = 1 %}

{% set cols_list = ['sales_id','date_sk','gross_amount'] %}

select
    {% for i in cols_list %}
        {{ i }}
        {% if not loop.last%}, {% endif%}
    {% endfor %}
from
    {{ ref('bronze_sales') }}

{% if inc_flag == 1%}

    where date_sk > (select max(date_sk) from {{ this }})

{% endif %}