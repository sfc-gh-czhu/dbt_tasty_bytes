{{ config(
    enabled=true,
    database="frostbyte_tasty_bytes",
    schema="harmonized"

) }}

SELECT 
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM {{ source('raw_customer','customer_loyalty') }} as cl 
join {{ source('raw_pos', 'order_header') }} as oh 
on cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name, cl.last_name, cl.phone_number, cl.e_mail