{{ config(
    enabled=true,
    database="frostbyte_tasty_bytes",
    schema="harmonized"

) }}

SELECT 
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    m.truck_brand_name,
    m.menu_type,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    m.menu_item_name,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total
FROM {{ source('raw_pos','order_detail') }} as od
join {{ source('raw_pos', 'order_header') }} as oh 
on od.order_id = oh.order_id
join {{ source('raw_pos','truck') }} as t 
on oh.truck_id = t.truck_id
join {{ source('raw_pos','menu') }} as m 
on od.menu_item_id = m.menu_item_id
join {{ source('raw_pos','franchise') }} as f 
ON t.franchise_id = f.franchise_id
join {{ source('raw_pos','location') }} as l 
on oh.location_id = l.location_id
left join {{ source('raw_customer','customer_loyalty') }} as cl 
on oh.customer_id = cl.customer_id
