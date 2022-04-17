CREATE TABLE _vaccines (
	vaccines_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    brand_name_1 varchar(100) NOT NULL UNIQUE,
    brand_name_2 varchar(100) UNIQUE,
    manufacturer varchar(100) NOT NULL,
    vaccine_type varchar(50)
) AUTO_INCREMENT = 1 ;


CREATE TABLE _vaccine_series (
	vaccine_series_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    series int NOT NULL UNIQUE,
    description varchar(100)
) AUTO_INCREMENT = 1 ;


CREATE TABLE vaccinations_daily (
	vaccinations_daily int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	country_subdivs_1_fk int NOT NULL,
    calendar_days_fk int NOT NULL,
    vaccine_series_fk int,
    amount int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
    FOREIGN KEY (country_subdivs_1_fk) REFERENCES _country_subdivs_1 (country_subdivs_1_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_days_fk) REFERENCES _calendar_days (calendar_days_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (vaccine_series_fk) REFERENCES _vaccine_series (vaccine_series_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;


CREATE TABLE vaccinations_daily_states (
	vaccinations_daily_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	country_subdivs_1_fk int NOT NULL,
    calendar_days_fk int NOT NULL,
    vaccines_fk int NOT NULL,
    vaccine_series_fk int,
    amount int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
    FOREIGN KEY (country_subdivs_1_fk) REFERENCES _country_subdivs_1 (country_subdivs_1_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_days_fk) REFERENCES _calendar_days (calendar_days_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (vaccines_fk) REFERENCES _vaccines (vaccines_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (vaccine_series_fk) REFERENCES _vaccine_series (vaccine_series_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;