CREATE TABLE staging.items (
	id serial4 NOT NULL,
	order_id int4 NOT NULL,
	item_id int4 NOT NULL,
	item_name varchar(30) NOT NULL,
	country_id int4 NOT NULL,
	price_per_day numeric(9, 2) NULL,
	total_price numeric(9, 2) NULL,
	category_id int4 NOT NULL,
	CONSTRAINT items_pkey PRIMARY KEY (id)
);