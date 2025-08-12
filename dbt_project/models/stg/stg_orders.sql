with source as (

    select * from {{ source('main', 'data_orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        payment_id,
        quantity,
        total_amount,
        order_date
    from source

)

select * from renamed