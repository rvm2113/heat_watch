CREATE TABLE dairy(
	year integer NOT NULL,
	retail_cost integer NOT NULL,
	farm_value integer NOT NULL,
	farm_to_retail_spread integer NOT NULL,
	farm_value_share integer NOT NULL
);


CREATE TABLE international_hazards(
	region varchar(20) NOT NULL,
	date_hazard date NOT NULL,
	type integer NOT NULL,
	geom geometry
);
CREATE TABLE global_surface_temperatures(
	temperature integer NOT NULL,
	number_of_vertices integer NOT NULL,
	unit varchar(20) NOT NULL,
	geom geometry(Polygon)
);

CREATE TABLE address_hazard(
	address_id serial NOT NULL,
	address varchar(100) unique NOT NULL,
	geom geometry
);



CREATE TABLE drought(
	improvement integer NOT NULL,
	persistent integer NOT NULL,
	development integer NOT NULL
	date_drought date NOT NULL,
	target_date date NOT NULL,
	removal integer NOT NULL,
	geom geometry
);
