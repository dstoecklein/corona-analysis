CREATE TABLE _classifications_icd10 (
	ID int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    icd10 varchar(20) UNIQUE NOT NULL,
    description_en text,
    description_de text,
    last_update DATETIME
) AUTO_INCREMENT = 1;