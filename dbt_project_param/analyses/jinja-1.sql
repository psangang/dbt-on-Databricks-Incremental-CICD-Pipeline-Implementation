{% set apples = ['Gala','Fuji','Honeycrisp','Red Delicious','McIntosh'] %}
{{ apples }}

{% for i in apples %}

    {% if i != "McIntosh" %}
        {{ i }}

    {% else %}
        i hate {{ i }}
    
    {% endif %}

{% endfor %}
