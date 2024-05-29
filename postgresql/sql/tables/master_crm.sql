CREATE TABLE master.crm_rent (
	id serial4 NOT NULL,
	order_date date NOT NULL,
	rent_start date NOT NULL,
	rent_end date NOT NULL,
	promocode varchar(30) NULL,
	CONSTRAINT crm_rent_pkey PRIMARY KEY (id)
);