{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='sync_all_columns'
) }}

with source as (
    select *
    from delta.default.orders
    {% if is_incremental() %}
      where CreationTime >
            (
              select cast(to_unixtime(max(created_at)) * 1000000 as bigint)
              from {{ this }}
            )
    {% endif %}
),

renamed as (
    select
        cast(Id as BIGINT) as order_id,
        cast(OrderNumber as VARCHAR) as order_code,
        cast(StoreId as BIGINT) as store_id,
        cast(CustomerId as VARCHAR) as customer_id_raw,
        try_cast(OrderTotal as DECIMAL(18,2)) as total_amount,
        from_unixtime(CreationTime / 1000000.0) as created_at,
        OrderStatus as status_id,
        cast(is_deleted_final as boolean) as is_deleted
    from source
    where cast(is_deleted_final as boolean) = false
)

select * from renamed
