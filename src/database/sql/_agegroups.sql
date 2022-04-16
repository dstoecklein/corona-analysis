CREATE TABLE _agegroups_10y (
	ID int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    agegroup varchar(5) NOT NULL UNIQUE
) AUTO_INCREMENT = 1 ;

CREATE TABLE _agegroups_05y (
	ID int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    agegroup varchar(5) NOT NULL UNIQUE
) AUTO_INCREMENT = 1 ;

CREATE TABLE _agegroups_rki (
	agegroups_rki_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
    agegroup varchar(5) NOT NULL UNIQUE
) AUTO_INCREMENT = 1 ;

INSERT INTO _agegroups_10y (agegroup) VALUES ('00-09'), ('10-19'), ('20-29'), ('30-39'), ('40-49'), ('50-59'), ('60-69'), ('70-79'), ('80+');

INSERT INTO _agegroups_05y (agegroup) VALUES ('00-04'), ('05-09'), ('10-14'), ('15-19'), ('20-24'), ('25-29'), ('30-34'), ('35-39'), ('40-44'),
('45-49'), ('50-54'), ('55-59'), ('60-64'), ('65-69'), ('70-74'), ('75-79'), ('80+');