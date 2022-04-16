CREATE TABLE population_countries_agegroups_10y (
	population_countries_agegroups_10y_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    countries_fk int NOT NULL,
	calendar_years_fk int NOT NULL,
	agegroups_10y_fk int NOT NULL,
    population int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
	FOREIGN KEY (countries_fk) REFERENCES countries (countries_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
	FOREIGN KEY (calendar_years_fk) REFERENCES calendar_years (calendar_years_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (agegroups_10y_fk) REFERENCES agegroups_10y (agegroups_10y_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE population_countries (
	population_countries_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
	calendar_years_fk int NOT NULL,
    population int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
	FOREIGN KEY (countries_fk) REFERENCES _countries (countries_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
	FOREIGN KEY (calendar_years_fk) REFERENCES _calendar_years (calendar_years_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE population_subdivs_1 (
	population_subdivs_1_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	country_subdivs_1_fk int NOT NULL,
	calendar_years_fk int NOT NULL,
    population int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
	FOREIGN KEY (country_subdivs_1_fk) REFERENCES _country_subdivs_1 (country_subdivs_1_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
	FOREIGN KEY (calendar_years_fk) REFERENCES _calendar_years (calendar_years_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE population_subdivs_2 (
	population_subdivs_2_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	country_subdivs_2_fk int NOT NULL,
	calendar_years_fk int NOT NULL,
    population int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
	FOREIGN KEY (country_subdivs_2_fk) REFERENCES _country_subdivs_2 (country_subdivs_2_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
	FOREIGN KEY (calendar_years_fk) REFERENCES _calendar_years (calendar_years_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE population_subdivs_3 (
	population_subdivs_3_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	country_subdivs_3_fk int NOT NULL,
	calendar_years_fk int NOT NULL,
    population int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
	FOREIGN KEY (country_subdivs_3_fk) REFERENCES _country_subdivs_3 (country_subdivs_3_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
	FOREIGN KEY (calendar_years_fk) REFERENCES _calendar_years (calendar_years_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE life_expectancy (
	life_expectancy_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
	calendar_years_fk int NOT NULL,
    life_expectancy double,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
	FOREIGN KEY (countries_fk) REFERENCES _countries (countries_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
	FOREIGN KEY (calendar_years_fk) REFERENCES _calendar_years (calendar_years_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE median_age (
	median_age_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
	calendar_years_fk int NOT NULL,
    median_age double,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
	FOREIGN KEY (countries_fk) REFERENCES _countries (countries_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
	FOREIGN KEY (calendar_years_fk) REFERENCES _calendar_years (calendar_years_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;