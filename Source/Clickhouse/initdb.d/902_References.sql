CREATE DATABASE IF NOT EXISTS References;


CREATE TABLE IF NOT EXISTS References.Tax
(
    country String,
    city String,
    tax_rate Float32
)
ENGINE = MergeTree()
ORDER BY (country, city);


CREATE TABLE IF NOT EXISTS References.Shipping
(
    country String,
    city String,
    base_fee Float32,
    per_kg_fee Float32
)
ENGINE = MergeTree()
ORDER BY (country, city);


CREATE TABLE IF NOT EXISTS References.Promotions
(
    promo_code String,
    discount_value UInt32
)
ENGINE = MergeTree()
ORDER BY promo_code;