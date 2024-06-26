CREATE TABLE my_database.category
(
    `id` UInt32,
    `category_name` String,
    `category_license` String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;