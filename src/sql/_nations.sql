CREATE TABLE _countries (
	countries_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    country_en VARCHAR(100) NOT NULL,
    country_de VARCHAR(100) NOT NULL,
    latitude double,
    longitude double,
    iso_3166-1_alpha3 varchar(2) NOT NULL UNIQUE,
    iso_3166-1_alpha2 varchar(3) NOT NULL UNIQUE,
    iso_3166-1_numeric int NOT NULL UNIQUE,
    nuts-0 varchar(2) NOT NULL UNIQUE
) AUTO_INCREMENT = 1;

CREATE TABLE _country_subdivs_1 (
	country_subdivs_1_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
    subdivision_1 VARCHAR(255) NOT NULL,
    latitude double,
    longitude double,
    iso_3166_2 varchar(5) UNIQUE,
    nuts_1 varchar(3) UNIQUE,
    FOREIGN KEY (countries_fk) REFERENCES _countries (countries_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE _country_subdivs_2 (
	country_subdivs_2_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	country_subdivs_1_fk int NOT NULL,
    subdivision_2 VARCHAR(255) NOT NULL,
    latitude double,
    longitude double,
    nuts_2 varchar(4) UNIQUE,
    FOREIGN KEY (country_subdivs_1_fk) REFERENCES _country_subdivs_1 (country_subdivs_1_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE _country_subdivs_3 (
	country_subdivs_3_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	country_subdivs_2_fk int NOT NULL,
    subdivision_3 VARCHAR(255) NOT NULL,
    latitude double,
    longitude double,
    nuts_3 varchar(5) UNIQUE,
    FOREIGN KEY (country_subdivs_2_fk) REFERENCES _country_subdivs_2 (country_subdivs_2_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;