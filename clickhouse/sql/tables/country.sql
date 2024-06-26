CREATE TABLE my_database.country
(
    `id` UInt32,
    `country_name` String,
    `continent` String,
    `population` UInt32
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192;