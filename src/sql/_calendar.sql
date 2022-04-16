CREATE TABLE _calendar_years (
	ID int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    iso_year int NOT NULL UNIQUE
) AUTO_INCREMENT = 1 ;

CALL insert_years();

CREATE TABLE _calendar_weeks (
	ID int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    years_fk int NOT NULL,
    iso_week int NOT NULL,
    iso_key int NOT NULL UNIQUE,
    FOREIGN KEY (years_fk) REFERENCES _calendar_years (ID)
    	ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1 ;

CREATE TABLE _calendar_days(
    ID INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    weeks_fk INT NOT NULL,
    iso_day date NOT NULL UNIQUE,
    FOREIGN KEY (weeks_fk) REFERENCES _calendar_weeks (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1 ;