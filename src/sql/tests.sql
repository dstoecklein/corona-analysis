CREATE TABLE tests_weekly (
	tests_weekly_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
    calendar_weeks_fk int NOT NULL,
    amount int,
    positive int,
    positive_percentage float,
    amount_transferring_laboratories int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE
    FOREIGN KEY (countries_fk) REFERENCES _countries (counties_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_weeks_fk) REFERENCES _calendar_weeks (calendar_weeks_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE tests_weekly_states (
	tests_weekly_states_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    country_subdivs_1_fk int NOT NULL,
    calendar_weeks_fk int NOT NULL,
    amount_tests int,
    positive_percentage float,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE
	FOREIGN KEY (country_subdivs_1_fk) REFERENCES _country_subdivs_1 (country_subdivs_1_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_weeks_fk) REFERENCES _calendar_weeks (calendar_weeks_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;