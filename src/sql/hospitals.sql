CREATE TABLE itcu_daily_counties (
	itcu_daily_counties_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	country_subdivs_3_fk int NOT NULL,
    calendar_days_fk int NOT NULL,
    amount_hospital_locations int,
    amount_reporting_areas int,
	cases_covid int,
    cases_covid_invasive_ventilated int,
    itcu_free int,
    itcu_free_adults int,
	itcu_occupied int,
    itcu_occupied_adults int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
    FOREIGN KEY (country_subdivs_3_fk) REFERENCES _country_subdivs_3 (country_subdivs_3_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_days_fk) REFERENCES _calendar_days (calendar_days_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE itcu_daily_states (
	itcu_daily_states_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	country_subdivs_1_fk int NOT NULL,
    calendar_days_fk int NOT NULL,
    treatment_group VARCHAR(100),
    amount_reporting_areas int,
	cases_covid int,
    itcu_occupied int,
    itcu_free int,
    7_day_emergency_reserve int,
	free_capacities_invasive_treatment int,
    free_capacities_invasive_treatment_covid int,
    operating_situation_regular int,
    operating_situation_partially_restricted int,
    operating_situation_restricted int,
    operating_situation_not_specified int,
    cases_covid_initial_reception int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
    FOREIGN KEY (country_subdivs_1_fk) REFERENCES _country_subdivs_1 (country_subdivs_1_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_days_fk) REFERENCES _calendar_days (calendar_days_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE hospitals_annual (
	hospitals_annual_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
    calendar_years_fk int NOT NULL,
    amount_hospitals int,
    amount_beds int,
	amount_beds_per_100000_population int,
    amount_patients int,
    amount_patients_per_100000_population int,
    occupancy_days int,
	avg_days_of_hospitalization double,
    avg_bed_occupancy_percent double,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
    FOREIGN KEY (countries_fk) REFERENCES _countries (countries_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_years_fk) REFERENCES _calendar_years (calendar_years_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE hospital_staff_annual (
	hospital_staff_annual_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
    calendar_years_fk int NOT NULL,
    total_staff int,
    full_time_doctors int,
	non_medical_staff int,
    non_medical_staff_in_nursing_service int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
    FOREIGN KEY (countries_fk) REFERENCES _countries (countries_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_years_fk) REFERENCES _calendar_years (calendar_years_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;
