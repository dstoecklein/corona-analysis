CREATE TABLE vaccinations_daily (
	vaccinations_daily_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	calendar_days_fk int NOT NULL,
    countries_fk int NOT NULL,
    total_vaccinations int,
    first_vaccinated int,
    second_vaccinated int,
    booster_vaccinated int,
    new_doses_raw int,
    new_doses_7d_smoothed int,
    total_rate float,
    first_rate float,
    second_rate float,
    booster_rate float,
    daily_vaccinated_first_shot int,
    daily_vaccinated_first_shot_rate float,
    last_update DATETIME,
    unique_key VARCHAR(255) NOT NULL UNIQUE,
    FOREIGN KEY (calendar_days_fk) REFERENCES _calendar_days (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    FOREIGN KEY (countries_fk) REFERENCES _countries (ID)
        ON DELETE CASCADE
        ON UPDATE CASCADE
) AUTO_INCREMENT = 1;