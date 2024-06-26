CREATE VIEW my_database.rolling_metrics
(
    `order_date` Date,
    `last_source` String,
    `revenue` Decimal(18,2),
    `orders` UInt64,
    `visits` Int32,
    `costs` Decimal(18,2),
    `cumul_orders` UInt64,
    `cumul_revenue` Decimal(38,2),
    `cumul_visits` Int64,
    `cumul_costs` Decimal(38,2),
    `rolling_orders` Float64,
    `rolling_revenue` Float64,
    `rolling_visits` Float64,
    `rolling_costs` Float64
) AS
WITH
    cte AS
    (
        SELECT toDate('2024-01-01') + number AS order_date
        FROM numbers(toUInt32((today() + 1) - toDate('2024-01-01')))
    ),
    last_src AS
    (
        SELECT last_source
        FROM my_database.data_revenue
        GROUP BY last_source
    ),
    cross_date_source AS
    (
        SELECT
            cte.order_date,
            last_src.last_source
        FROM cte
        CROSS JOIN last_src
    ),
    cte_data_revenue AS
    (
        SELECT
            order_date,
            last_source,
            CAST(round(sum(revenue),2),'Decimal64(2)') AS revenue,
            countDistinct(order_id) AS orders
        FROM
        (
            SELECT *
            FROM my_database.inter_data_revenue
            WHERE status = 'pending'
            UNION ALL
            SELECT *
            FROM my_database.data_revenue
            WHERE status = 'accepted'
        ) AS subq
        GROUP BY
            order_date,
            last_source
    ),
    joined_revenue_data AS
    (
        SELECT
            crs.order_date AS order_date,
            crs.last_source AS last_source,
            coalesce(dr.revenue, 0) AS revenue,
            coalesce(dr.orders, 0) AS orders,
            coalesce(c.visits, 0) AS visits,
            coalesce(c.costs, 0) AS costs
        FROM cross_date_source AS crs
        LEFT JOIN cte_data_revenue AS dr 
        ON (crs.order_date = dr.order_date) AND (crs.last_source = dr.last_source)
        LEFT JOIN my_database.costs AS c 
        ON (crs.order_date = c.cost_date) AND (crs.last_source = c.cost_source)
    )
SELECT
    *,
    avg(orders) OVER (PARTITION BY last_source ORDER BY order_date ASC Rows BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_orders,
    avg(revenue) OVER (PARTITION BY last_source ORDER BY order_date ASC Rows BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_revenue,
    avg(visits) OVER (PARTITION BY last_source ORDER BY order_date ASC Rows BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_visits,
    avg(costs) OVER (PARTITION BY last_source ORDER BY order_date ASC Rows BETWEEN 4 PRECEDING AND CURRENT ROW) AS rolling_costs
FROM
(
    SELECT
        *,
        sum(orders) OVER (PARTITION BY last_source ORDER BY order_date ASC) AS cumul_orders,
        sum(revenue) OVER (PARTITION BY last_source ORDER BY order_date ASC) AS cumul_revenue,
        sum(visits) OVER (PARTITION BY last_source ORDER BY order_date ASC) AS cumul_visits,
        sum(costs) OVER (PARTITION BY last_source ORDER BY order_date ASC) AS cumul_costs
    FROM joined_revenue_data
    ORDER BY order_date ASC
) AS subq_total;