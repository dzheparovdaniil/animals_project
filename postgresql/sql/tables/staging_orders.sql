CREATE TABLE staging.orders (
	id serial4 NOT NULL,
	order_date date NOT NULL,
	status varchar(30) NOT NULL,
	user_id int4 NULL,
	source_path text NULL,
	rent_start date NOT NULL,
	rent_end date NOT NULL,
	rent_days int4 NULL,
	promocode varchar(30) NULL,
	CONSTRAINT orders_pkey PRIMARY KEY (id)
);