CREATE OR REPLACE VIEW staging.v_revenue_data
AS WITH cte AS (
         SELECT o.id AS order_id,
            o.order_date,
            o.status,
            o.source_path,
            o.user_id,
            o.promocode,
            i.item_id,
            i.country_id,
            i.category_id,
            sum(o.rent_days::numeric * i.price_per_day) AS revenue_raw
           FROM staging.orders o
             LEFT JOIN staging.items i ON o.id = i.order_id
          GROUP BY o.id, o.order_date, o.status, o.source_path, o.user_id, o.promocode, i.item_id, i.country_id, i.category_id
          ORDER BY o.id, i.item_id
        )
 SELECT row_number() OVER () AS row_id,
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
    staging.get_first_source(source_path::character varying) AS first_source,
    staging.get_last_source(source_path::character varying) AS last_source,
        CASE
            WHEN promocode IS NULL THEN revenue_raw
            WHEN promocode::text ~~ '%10'::text THEN revenue_raw * 0.9
            WHEN promocode::text ~~ '%20'::text THEN revenue_raw * 0.8
            WHEN promocode::text ~~ '%30'::text THEN revenue_raw * 0.7
            ELSE revenue_raw
        END AS revenue
   FROM cte;