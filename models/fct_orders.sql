{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    on_schema_change='sync_all_columns',
    cluster_by=['order_date::date']
) }}

select *
from {{ ref('stg_orders') }}
{% if is_incremental() %}
  -- If order_date is DATE:
  where order_date > (
    select coalesce(max(order_date), to_date('1900-01-01'))
    from {{ this }}
  )
  -- If order_date is TIMESTAMP, use this instead:
  -- where order_date > (
  --   select coalesce(max(order_date), to_timestamp_ntz('1900-01-01'))
  --   from {{ this }}
  -- )
{% endif %}
