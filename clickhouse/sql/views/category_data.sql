CREATE VIEW my_database.category_data
(
    `order_month` UInt8,
    `category_name` String,
    `orders` UInt64,
    `revenue` Decimal(18,2),
    `part_orders` UInt64,
    `part_revenue` Decimal(38,2),
    `orders_percent` Decimal(18,2),
    `revenue_percent` Decimal(18,2)
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

    cte_x_month AS
    (
        SELECT
            *,
            MONTH(cte.order_date) AS order_month
        FROM cte
    ),
    total_data AS
    (
        SELECT
            cte.order_month,
            c.category_name,
            countDistinct(order_id) AS orders,
            CAST(round(sum(revenue),2),'Decimal64(2)') AS revenue
        FROM cte_x_month AS cte
        LEFT JOIN my_database.category AS c ON cte.category_id = c.id
        GROUP BY
            cte.order_month,
            c.category_name
    )
    
SELECT *,
    CAST(round(orders / part_orders, 2), 'Decimal64(2)') AS orders_percent,
    CAST(round(revenue / part_revenue, 2), 'Decimal64(2)') AS revenue_percent
FROM
(
    SELECT *,
        sum(orders) OVER (PARTITION BY order_month) AS part_orders,
        sum(revenue) OVER (PARTITION BY order_month) AS part_revenue
    FROM total_data
) AS subq;