CREATE TABLE my_database.costs
(
    `cost_date` Date,
    `cost_source` String,
    `visits` Nullable(Int32),
    `costs` Nullable(Decimal(10,2))
)
ENGINE = MergeTree
ORDER BY cost_date
SETTINGS index_granularity = 8192;