USE projektarbeit;

CREATE TABLE calendar_yr (
	calendar_yr_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    iso_year int NOT NULL UNIQUE
) AUTO_INCREMENT = 1 ;

CALL insert_years();

CREATE TABLE calendar_cw (
	calendar_cw_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_yr_id int NOT NULL,
    iso_cw int NOT NULL,
    iso_key int NOT NULL UNIQUE,
    FOREIGN KEY (calendar_yr_id) REFERENCES calendar_yr (calendar_yr_id)
) AUTO_INCREMENT = 1 ;

CREATE TABLE agegroups_10y (
	agegroups_10y_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    agegroup varchar(5) UNIQUE
) AUTO_INCREMENT = 1 ;

INSERT INTO agegroups_10y (agegroup) VALUES ('0-9'), ('10-19'), ('20-29'), ('30-39'), ('40-49'), ('50-59'), ('60-69'), ('70-79'), ('80+');

CREATE TABLE ger_states (
	ger_states_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    state text
) AUTO_INCREMENT = 1;

CREATE TABLE icd10_codes (
	icd10_codes_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    icd10 varchar(20) UNIQUE NOT NULL,
    description_eng text,
    description_ger text
) AUTO_INCREMENT = 1;

CREATE TABLE estat_annual_population_ger (
	estat_annual_population_ger_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	agegroups_10y_id int NOT NULL,
    calendar_yr_id int NOT NULL,
    population int,
    last_update DATETIME,
    FOREIGN KEY (agegroups_10y_id) REFERENCES agegroups_10y (agegroups_10y_id),
    FOREIGN KEY (calendar_yr_id) REFERENCES calendar_yr (calendar_yr_id)
) AUTO_INCREMENT = 1;

CREATE TABLE estat_annual_population_swe (
	estat_annual_population_swe_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	agegroups_10y_id int NOT NULL,
    calendar_yr_id int NOT NULL,
    population int,
    last_update DATETIME,
    FOREIGN KEY (agegroups_10y_id) REFERENCES agegroups_10y (agegroups_10y_id),
    FOREIGN KEY (calendar_yr_id) REFERENCES calendar_yr (calendar_yr_id)
) AUTO_INCREMENT = 1;

CREATE TABLE rki_daily_covid_ger (
	rki_daily_covid_ger_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_cw_id int NOT NULL,
    reporting_date date NOT NULL,
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
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;

CREATE TABLE rki_daily_rvalue_ger (
	rki_daily_rvalue_ger_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_cw_id int NOT NULL,
    datum date NOT NULL,
    ps_covid_faelle int,
    ug_pi_covid_faelle int,
    og_pi_covid_faelle int,
    ps_covid_faelle_ma4 int,
    ug_pi_covid_faelle_ma4 int,
    og_pi_covid_faelle_ma4 int,
    ps_7_tage_r_wert float,
    ug_pi_7_tage_r_wert float,
    og_pi_7_tage_r_wert float,
	last_update DATETIME,
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;

CREATE TABLE rki_weekly_tests_ger (
	rki_weekly_tests_ger_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_cw_id int NOT NULL,
    amount_tests int,
    positiv_tests int,
    positiv_percentage float,
    amount_transferring_laboratories int,
    last_update DATETIME,
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;

CREATE TABLE rki_yearly_covid_ger (
	rki_yearly_covid_ger_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_yr_id int NOT NULL,
    covid_cases int,
    covid_deaths int,
    covid_recovered int,
    covid_active_cases int,
    last_update DATETIME,
    FOREIGN KEY (calendar_yr_id) REFERENCES calendar_yr (calendar_yr_id)
) AUTO_INCREMENT = 1;

CREATE TABLE estat_weekly_deaths_ger (
	estat_weekly_deaths_ger_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	agegroups_10y_id int NOT NULL,
    calendar_cw_id int NOT NULL,
    deaths int,
    last_update DATETIME,
	FOREIGN KEY (agegroups_10y_id) REFERENCES agegroups_10y (agegroups_10y_id),
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;

CREATE TABLE estat_weekly_deaths_swe (
	estat_weekly_deaths_swe_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	agegroups_10y_id int NOT NULL,
    calendar_cw_id int NOT NULL,
    deaths int,
    last_update DATETIME,
	FOREIGN KEY (agegroups_10y_id) REFERENCES agegroups_10y (agegroups_10y_id),
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;

CREATE TABLE destatis_annual_population_ger (
	destatis_annual_population_ger_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    agegroups_10y_id int NOT NULL,
    calendar_yr_id int NOT NULL,
    ger_states_id int NOT NULL,
    population int,
    last_update DATETIME,
	FOREIGN KEY (agegroups_10y_id) REFERENCES agegroups_10y (agegroups_10y_id),
    FOREIGN KEY (calendar_yr_id) REFERENCES calendar_yr (calendar_yr_id),
    FOREIGN KEY (ger_states_id) references ger_states (ger_states_id)
) AUTO_INCREMENT = 1;

CREATE TABLE rki_weekly_covid_ger (
	rki_weekly_covid_ger_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_cw_id int NOT NULL,
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
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;

CREATE TABLE rki_daily_covid_agegroups_ger (
	rki_daily_covid_agegroups_ger_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_cw_id int NOT NULL,
    reporting_date date NOT NULL,
    rki_agegroups text,
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
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;

CREATE TABLE rki_daily_covid_states_ger (
	rki_daily_covid_states_ger_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    ger_states_id int NOT NULL,
    calendar_cw_id int NOT NULL,
    reporting_date date NOT NULL,
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
    FOREIGN KEY (ger_states_id) REFERENCES ger_states (ger_states_id),
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;

INSERT INTO ger_states (state) VALUES ('Schleswig-Holstein'), ('Hamburg'), ('Niedersachsen'), ('Bremen'), ('Nordrhein-Westfalen'), ('Hessen'), ('Rheinland-Pfalz'), ('Baden-Württemberg'), ('Bayern'), ('Saarland'), ('Berlin'), ('Brandenburg'), ('Mecklenburg-Vorpommern'), ('Sachsen'), ('Sachsen-Anhalt'), ('Thüringen');

CREATE TABLE estatis_annual_death_causes_ger (
	estatis_annual_death_causes_ger_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_yr_id int NOT NULL,
    agegroups_10y_id int NOT NULL,
    icd10_codes_id int NOT NULL,
    resid text,
    deaths int,
    last_update DATETIME,
    FOREIGN KEY (calendar_yr_id) REFERENCES calendar_yr (calendar_yr_id),
    FOREIGN KEY (agegroups_10y_id) REFERENCES agegroups_10y (agegroups_10y_id),
    FOREIGN KEY (icd10_codes_id) REFERENCES icd10_codes (icd10_codes_id)
) AUTO_INCREMENT = 1;

CREATE TABLE estatis_annual_death_causes_swe (
	estatis_annual_death_causes_swe_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_yr_id int NOT NULL,
    agegroups_10y_id int NOT NULL,
    icd10_codes_id int NOT NULL,
    resid text,
    deaths int,
    last_update DATETIME,
    FOREIGN KEY (calendar_yr_id) REFERENCES calendar_yr (calendar_yr_id),
    FOREIGN KEY (agegroups_10y_id) REFERENCES agegroups_10y (agegroups_10y_id),
    FOREIGN KEY (icd10_codes_id) REFERENCES icd10_codes (icd10_codes_id)
) AUTO_INCREMENT = 1;

CREATE TABLE owid_daily_covid_swe (
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

CREATE TABLE owid_daily_covid_isr (
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

CREATE TABLE oecd_weekly_deaths_swe (
	oecd_weekly_deaths_swe_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_cw_id int NOT NULL,
    deaths int,
    flags text,
    last_update DATETIME,
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;

CREATE TABLE oecd_weekly_deaths_isr (
	oecd_weekly_deaths_isr_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_cw_id int NOT NULL,
    deaths int,
    flags text,
    last_update DATETIME,
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;

CREATE TABLE oecd_weekly_covid_deaths_swe (
	oecd_weekly_covid_deaths_swe_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_cw_id int NOT NULL,
    deaths int,
    flags text,
    last_update DATETIME,
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;

CREATE TABLE oecd_weekly_covid_deaths_isr (
	oecd_weekly_covid_deaths_isr_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    calendar_cw_id int NOT NULL,
    deaths int,
    flags text,
    last_update DATETIME,
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;

CREATE TABLE rki_weekly_tests_states_ger (
	rki_weekly_tests_states_ger_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    ger_states_id int NOT NULL,
    calendar_cw_id int NOT NULL,
    amount_tests int,
    positiv_percentage float,
    last_update DATETIME,
    FOREIGN KEY (ger_states_id) REFERENCES ger_states (ger_states_id),
    FOREIGN KEY (calendar_cw_id) REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1;

