CREATE TABLE staging.costs (
	cost_date date NOT NULL,
	cost_source varchar(30) NOT NULL,
	visits int4 NULL,
	costs numeric(10, 2) NULL
);