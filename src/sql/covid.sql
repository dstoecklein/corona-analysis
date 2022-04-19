CREATE TABLE covid_daily (
	ID int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
    calendar_days_fk int NOT NULL,
    calendar_weeks_fk int NOT NULL,
    cases int,
    cases_delta int,
    cases_delta_ref int,
    cases_delta_ref_sympt int,
    cases_7d int,
    cases_7d_sympt int,
    cases_7d_ref int,
    cases_7d_ref_sympt int,
    deaths int,
    deaths_delta int,
    recovered int,
    recovered_delta int,
    active_cases int,
    active_cases_delta int,
    incidence_7d float,
    incidence_7d_sympt float,
    incidence_7d_ref float,
    incidence_7d_ref_sympt float,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE
    FOREIGN KEY (countries_fk) REFERENCES _countries (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_days_fk) REFERENCES _calendar_days (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_weeks_fk) REFERENCES _calendar_weeks (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE covid_yearly (
	covid_yearly_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
    calendar_years_fk int NOT NULL,
    cases int,
    deaths int,
    recovered int,
    active_cases int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
    FOREIGN KEY (countries_fk) REFERENCES _countries (countries_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_years_fk) REFERENCES _calendar_years (calendar_years_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE covid_weekly_cumulative (
	covid_weekly_cumulative_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
    calendar_weeks_fk int NOT NULL,
    cases int,
    cases_delta int,
	cases_delta_ref int,
    cases_delta_ref_sympt int,
    deaths int,
    deaths_delta int,
    recovered int,
    recovered_delta int,
    active_cases int,
    active_cases_delta int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
    FOREIGN KEY (countries_fk) REFERENCES _countries (covid_weekly_cumulative_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_weeks_fk) REFERENCES _calendar_weeks (calendar_weeks_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE covid_daily_agegroups (
	covid_daily_agegroups_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
    calendar_days_fk int NOT NULL,
    agegroups_rki_fk int NOT NULL,
    cases int,
    cases_delta int,
    cases_delta_ref int,
    cases_delta_ref_sympt int,
    cases_7d int,
    cases_7d_sympt int,
    cases_7d_ref int,
    cases_7d_ref_sympt int,
    deaths int,
    deaths_delta int,
    recovered int,
    recovered_delta int,
    active_cases int,
    active_cases_delta int,
    incidence_7d float,
    incidence_7d_sympt float,
    incidence_7d_ref float,
    incidence_7d_ref_sympt float,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
    FOREIGN KEY (countries_fk) REFERENCES _countries (countries_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_days_fk) REFERENCES _calendar_days (calendar_days_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (agegroups_rki_fk) REFERENCES _agegroups_rki (agegroups_rki_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE covid_daily_by_states (
	covid_daily_by_states_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	country_subdivs_1_fk int NOT NULL,
    calendar_days_fk int NOT NULL,
    cases int,
    cases_delta int,
	cases_delta_ref int,
    cases_delta_ref_sympt int,
    cases_7d int,
	cases_7d_sympt int,
    cases_7d_ref int,
    cases_7d_ref_sympt int,
    deaths int,
    deaths_delta int,
    recovered int,
    recovered_delta int,
    active_cases int,
    active_cases_delta int,
    incidence_7d float,
	incidence_7d_sympt float,
    incidence_7d_ref float,
    incidence_7d_ref_sympt float,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
    FOREIGN KEY (country_subdivs_1_fk) REFERENCES _country_subdivs_1 (country_subdivs_1_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_days_fk) REFERENCES _calendar_days (calendar_days_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE covid_daily_by_counties (
	covid_daily_by_counties_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	country_subdivs_3_fk int NOT NULL,
    calendar_days_fk int NOT NULL,
    cases int,
    cases_delta int,
	cases_delta_ref int,
    cases_delta_ref_sympt int,
    cases_7d int,
	cases_7d_sympt int,
    cases_7d_ref int,
    cases_7d_ref_sympt int,
    deaths int,
    deaths_delta int,
    recovered int,
    recovered_delta int,
    active_cases int,
    active_cases_delta int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
    FOREIGN KEY (country_subdivs_3_fk) REFERENCES _country_subdivs_3 (country_subdivs_3_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_days_fk) REFERENCES _calendar_days (calendar_days_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE rvalue_daily (
	rvalue_daily_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
    calendar_days_fk int NOT NULL,
    point_estimation_covid int,
    ll_prediction_interval_covid int,
    ul_prediction_interval_covid int,
    point_estimation_covid_smoothed int,
    ll_prediction_interval_covid_smoothed int,
    ul_prediction_interval_covid_smoothed int,
    point_estimation_7_day_rvalue double,
    ll_prediction_interval_7_day_rvalue double,
    ul_prediction_interval_7_day_rvalue double,
	last_update DATETIME,
	unique_key VARCHAR(255) NOT NULL UNIQUE,
    FOREIGN KEY (countries_fk) REFERENCES _countries (countries_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (calendar_days_fk) REFERENCES _calendar_days (calendar_days_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE sweden_weekly_deaths (
	sweden_weekly_deaths_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    weeks_id int NOT NULL,
    deaths int,
    flags text,
    last_update DATETIME,
    FOREIGN KEY (weeks_id) REFERENCES calendar.weeks (weeks_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE israel_weekly_deaths (
	israel_weekly_deaths_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    weeks_id int NOT NULL,
    deaths int,
    flags text,
    last_update DATETIME,
    FOREIGN KEY (weeks_id) REFERENCES calendar.weeks (weeks_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;