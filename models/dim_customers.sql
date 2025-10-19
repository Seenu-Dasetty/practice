{{ config(materialized='table', schema='MART') }}

select
  c.customer_id,
  c.first_name,
  c.last_name,
  c.email,
  c.created_at
from {{ ref('stg_customers') }} c
-- This is the updated customer table.