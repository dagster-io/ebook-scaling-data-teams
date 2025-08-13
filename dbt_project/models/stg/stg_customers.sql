with source as (

    select * from {{ source('main', 'data_customers') }}

),

renamed as (

    select
        customer_id,
        first_name,
        last_name,
        email
    from source

)

select * from renamed