{{
  config(
    materialized='table'
  )
}}

with orders as (
    
    select *,
           cast(ordered_at as date) as ordered_at_date
      from {{ ref('stg_orders') }} o
),
     order_items as (
    select *
      from {{ ref('stg_order_items') }}
),
     customers as (
    
    select *
      from {{ref('stg_customers')}}
),
     locations as (

    select *
      from {{ref('stg_locations')}}
),
     products as (
    
    select *
      from {{ref('stg_products')}}
),
     products_order as (
     select ord.ordered_at_date,
            ord.order_id,
            count(oi.product_id) as product_count, -- per order
            pr.product_price,
            pr.product_name,
            pr.product_id,
            cu.customer_name,
            lo.location_name,
            ord.tax_paid,
            ord.order_total,
            row_number() over(partition by ord.order_id) rn
       from orders ord
       left join order_items oi
         on oi.order_id = ord.order_id
       join products pr
         on pr.product_id = oi.product_id
  left join customers cu
    on cu.customer_id = ord.customer_id
  left join locations lo
    on lo.location_id = ord.location_id
      group by order_id, product_price, pr.product_name, product_id,cu.customer_name, lo.location_name, ord.tax_paid,ord.order_total,ord.ordered_at_date
)
 select ordered_at_date as order_date,
        order_id,
        product_count, -- per order
        product_price,
        product_name,
        product_id,
        customer_name,
        location_name,
        order_total,
        case when rn > 1 then 0 else tax_paid
        end tax_paid
   from products_order
  order by ordered_at_date, order_id, customer_name 
