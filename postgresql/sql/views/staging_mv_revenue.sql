CREATE MATERIALIZED VIEW staging.mv_revenue_data
TABLESPACE pg_default
AS SELECT row_id,
    order_id,
    order_date,
    status,
    source_path,
    user_id,
    promocode,
    item_id,
    country_id,
    category_id,
    revenue_raw,
    first_source,
    last_source,
    revenue
   FROM staging.v_revenue_data
WITH DATA;