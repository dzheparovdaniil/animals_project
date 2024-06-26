CREATE VIEW my_database.cohort
(
    `min_date` Date,
    `order_date` Date,
    `first_cohort_source` String,
    `last_source` String,
    `delta` Int32,
    `revenue` Decimal(38,2),
    `orders` UInt64,
    `visits` Nullable(Int32),
    `costs` Nullable(Decimal(18,2))
) AS
WITH
    cte AS
    (
        SELECT *
        FROM my_database.inter_data_revenue
        WHERE status = 'pending'
        UNION ALL
        SELECT *
        FROM my_database.data_revenue
        WHERE status = 'accepted'
    ),
    user_min_date AS
    (
        SELECT
            user_id,
            min(order_date) AS min_date
        FROM cte
        GROUP BY user_id
    ),
    cte_x_min_date AS
    (
        SELECT
            cte.row_id,
            cte.order_id,
            cte.user_id,
            cte.order_date,
            cte.last_source,
            user_min_date.min_date,
            cte.order_date - user_min_date.min_date AS delta,
            CAST(round(cte.revenue,2),'Decimal64(2)') AS revenue
        FROM cte
        LEFT JOIN user_min_date ON cte.user_id = user_min_date.user_id
    ),
    cte_x_min_date_b AS
    (
        SELECT
            *,
            argMin(last_source, delta) OVER (PARTITION BY user_id ORDER BY delta ASC) AS first_cohort_source
        FROM cte_x_min_date
    ),
    union_data AS
    (
        SELECT
            min_date,
            order_date,
            first_cohort_source,
            last_source,
            delta,
            sum(revenue) AS revenue,
            countDistinct(order_id) AS orders,
            0 AS visits,
            0 AS costs
        FROM cte_x_min_date_b
        GROUP BY
            min_date,
            order_date,
            first_cohort_source,
            last_source,
            delta
        UNION ALL
        SELECT
            cost_date AS min_date,
            cost_date AS order_date,
            cost_source AS first_cohort_source,
            cost_source AS last_source,
            0 AS delta,
            0 AS revenue,
            0 AS orders,
            visits,
            costs
        FROM my_database.costs
    )
SELECT *
FROM union_data;