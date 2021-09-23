USE projektarbeit;

CREATE TABLE divi_daily_beds_total(
    divi_daily_beds_total_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    datum date,
    faelle_covid int,
    faelle_covid_invasiv int,
    betten_frei int,
    betten_belegt int,
    betten_gesamt int
) AUTO_INCREMENT = 1 ;

CREATE TABLE divi_beds_zeitreihen_daten (
    divi_beds_zeitreihen_daten_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    calendar_cw_id INT NOT NULL,
    ger_states_id INT NOT NULL,
    datum DATE,
    bundesland TEXT,
    anzahl_meldebereiche_erwachsene INT,
    aktuelle_covid_faelle_erwachsene_its INT,
    belegte_intensivbetten_erwachsene INT,
    freie_intensivbetten_erwachsene INT,
    7_tage_notfallreserve_erwachsene INT,
    freie_iv_kapazitaeten_gesamt INT,
    freie_iv_kapazitaeten_davon_covid INT,
    betriebssituation_regulaerer_betrieb INT,
    betriebssituation_teilweise_eingeschraenkt INT,
    betriebssituation_eingeschraenkt INT,
    betriebssituation_keine_angabe INT,
    gesamt_intensivbetten_erwechsene INT,
    anteil_belegte_betten_an_gesamtbetten DOUBLE,
    anteil_covid_patienen_an_gesamtbetten DOUBLE,
    behandlungs_ueberschuss int,
    behandlungs_ueberschuss_kumulativ int,
    FOREIGN KEY (calendar_dt_id)
        REFERENCES calendar_cw (calendar_dt_id),
    Foreign KEY (ger_states_id)
        REFERENCES ger_states (ger_states_id)
)  AUTO_INCREMENT=1;

CREATE TABLE calendar_dt(
    calendar_dt_id INT NOT NULL PRIMARY KEY AUTO_INCREMENT,
    datum date,
    calendar_cw_id INT NOT NULL,
    FOREIGN KEY (calendar_cw_id)
        REFERENCES calendar_cw (calendar_cw_id)
) AUTO_INCREMENT = 1 ;

CREATE TABLE rki_recent_daily_covid_ger (
	rki_recent_daily_covid_ger_id int NOT NULL AUTO_INCREMENT PRIMARY KEY,
	ger_states_id int NOT NULL,
    calendar_dt_id int NOT NULL,
    reporting_date date NOT NULL,
    cases int,
    cases_7d_rm double,
    deaths int,
    deaths_7d_rm double,
    recovered int,
    recovered_7d_rm double,
    FOREIGN KEY (calendar_dt_id) REFERENCES calendar_cw (calendar_cw_id),
    FOREIGN KEY (ger_states_id) REFERENCES ger_states (ger_states_id)
) AUTO_INCREMENT = 1;

