CREATE TABLE sweden_daily (
	owid_daily_covid_swe_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_cw_id int NOT NULL,
    reporting_date date NOT NULL,
    cases int,
    cases_delta int,
    deaths int,
    deaths_delta int,
    recovered int,
    active_cases int,
	rvalue double,
    icu_patients int,
    hosp_patients int,
    tests int,
    tests_delta int,
    positive_rate double,
    tests_per_case double,
    vaccinations int,
    vaccinations_delta int,
    people_vaccinated int,
    people_fully_vaccinated int,
    total_boosters int,
	stringency_index int,
    population int,
    median_age int,
    life_expectancy float,
    last_update DATETIME,
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;

CREATE TABLE israel_daily (
	owid_daily_covid_isr_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_cw_id int NOT NULL,
    reporting_date date NOT NULL,
    cases int,
    cases_delta int,
    deaths int,
    deaths_delta int,
    recovered int,
    active_cases int,
	rvalue double,
    icu_patients int,
    hosp_patients int,
    tests int,
    tests_delta int,
    positive_rate double,
    tests_per_case double,
    vaccinations int,
    vaccinations_delta int,
    people_vaccinated int,
    people_fully_vaccinated int,
    total_boosters int,
	stringency_index int,
    population int,
    median_age int,
    life_expectancy float,
    last_update DATETIME,
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;





