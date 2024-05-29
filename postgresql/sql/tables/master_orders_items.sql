CREATE TABLE master.orders_items (
	id serial4 NOT NULL,
	order_id int4 NOT NULL,
	item_id int4 NOT NULL,
	CONSTRAINT orders_items_pkey PRIMARY KEY (id),
	CONSTRAINT fk_item_id FOREIGN KEY (item_id) REFERENCES master.items(id),
	CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES master.orders(id)
);