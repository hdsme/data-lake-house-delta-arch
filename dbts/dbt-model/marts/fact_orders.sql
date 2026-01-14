{{ config(materialized='view') }}

with staging as (
    select * from {{ ref('stg_orders') }}
)

select
    order_id,
    order_code,
    store_id,
    total_amount,
    created_at,
    
    -- Derived Columns cho Dashboard (Doanh thu theo khung giờ)
    hour(created_at) as order_hour,
    date(created_at) as order_date,
    format_datetime(created_at, 'yyyy-MM') as order_month,

    -- Mapping trạng thái (Giả định logic, cần check lại document nghiệp vụ)
    status_id,
    CASE 
        WHEN status_id = 150 THEN 'Completed'
        WHEN status_id = 70 THEN 'Cancelled'
        ELSE 'Processing'
    END as status_desc,

    -- Phân loại khung giờ (Yêu cầu số 10 trong requirements.md)
    CASE
        WHEN hour(created_at) BETWEEN 6 AND 10 THEN 'Sáng (6h-11h)'
        WHEN hour(created_at) BETWEEN 11 AND 14 THEN 'Trưa (11h-14h)'
        WHEN hour(created_at) BETWEEN 15 AND 18 THEN 'Chiều (15h-18h)'
        ELSE 'Tối (19h-05h)'
    END as time_slot

from staging
where total_amount > 0 -- Data Quality check cơ bản