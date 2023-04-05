{{ config(
    enabled=true,
    database="frostbyte_tasty_bytes",
    schema="analytics"

) }}

SELECT DATE(o.order_ts) AS date, * FROM {{ref('orders_v')}} as o