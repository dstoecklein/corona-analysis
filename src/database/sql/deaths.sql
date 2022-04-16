CREATE TABLE deaths_by_agegroups_weekly (
	ID int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
	weeks_fk int NOT NULL,
	agegroups_10y_fk int NOT NULL,
    deaths int,
    last_update DATETIME,
    unique_key int NOT NULL UNIQUE,
    FOREIGN KEY (countries_fk) REFERENCES countries (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
	FOREIGN KEY (weeks_fk) REFERENCES calendar_weeks (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (agegroups_fk) REFERENCES agegroups_10y (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE death_causes_by_agegroups_annual (
	ID int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
    years_fk int NOT NULL,
    agegroups_10y_fk int NOT NULL,
    icd10_fk int NOT NULL,
    resid text,
    deaths int,
    last_update DATETIME,
    unique_key int NOT NULL UNIQUE,
    FOREIGN KEY (countries_fk) REFERENCES countries (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
	FOREIGN KEY (years_fk) REFERENCES calendar_years (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (agegroups_fk) REFERENCES agegroups_10y (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (icd10_fk) REFERENCES classifications_icd10 (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

CREATE TABLE deaths_weekly (
	ID int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	countries_fk int NOT NULL,
    weeks_fk int NOT NULL,
    deaths int,
    flags text,
    last_update DATETIME,
    unique_key int NOT NULL UNIQUE,
       FOREIGN KEY (countries_fk) REFERENCES countries (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (weeks_fk) REFERENCES calendar_weeks (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;

