CREATE TABLE vaccinations_daily_manufacturer (
	vaccinations_daily_manufacturer_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	calendar_days_fk int NOT NULL,
    countries_fk int NOT NULL,
    vaccines_fk int NOT NULL,
    total_vaccinations int,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
    FOREIGN KEY (calendar_days_fk) REFERENCES _calendar_days (calendar_days_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (countries_fk) REFERENCES _countries (countries_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (vaccines_fk) REFERENCES _vaccines (vaccines_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;