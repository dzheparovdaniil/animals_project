CREATE TABLE master.items (
	id serial4 NOT NULL,
	item_name varchar(50) NOT NULL,
	country_id int4 NOT NULL,
	price_per_day numeric(9, 2) NULL,
	total_price numeric(9, 2) NULL,
	animal_age int4 NULL,
	category_id int4 NOT NULL,
	CONSTRAINT items_pkey PRIMARY KEY (id),
	CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES master.сategory(id),
	CONSTRAINT fk_country FOREIGN KEY (country_id) REFERENCES master.country(id)
);