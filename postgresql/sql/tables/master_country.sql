CREATE TABLE master.country (
	id serial4 NOT NULL,
	country_name varchar(50) NOT NULL,
	continent varchar(50) NOT NULL,
	population int8 NOT NULL,
	CONSTRAINT country_pkey PRIMARY KEY (id)
);