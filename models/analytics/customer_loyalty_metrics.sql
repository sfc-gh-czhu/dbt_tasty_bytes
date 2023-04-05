{{ config(
    enabled=true,
    database="frostbyte_tasty_bytes",
    schema="analytics"

) }}

SELECT * FROM {{ref('customer_loyalty_metrics_v')}}