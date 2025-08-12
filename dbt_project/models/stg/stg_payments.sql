with source as (

    select * from {{ source('main', 'data_payments') }}

),

renamed as (

    select
        payment_id,
        amount,
        payment_date
    from source

)

select * from renamed