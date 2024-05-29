CREATE TABLE master.orders (
	id serial4 NOT NULL,
	order_date date NOT NULL,
	status varchar(30) NOT NULL,
	user_id int4 NULL,
	source_path text NULL,
	created_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
	updated_at timestamp NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT orders_pkey PRIMARY KEY (id)
);