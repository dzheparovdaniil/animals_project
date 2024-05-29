CREATE TABLE my_database.data_revenue
(
    row_id UInt32,
    order_id UInt32,
    order_date Date,
    status String,
    source_path String,
    user_id UInt32,
    promocode Nullable(String),
    item_id UInt32,
    country_id UInt32,
    category_id UInt32,
    revenue_raw Float32,
    first_source String,
    last_source String,
    revenue Float32
)
ENGINE = MergeTree
ORDER BY row_id;