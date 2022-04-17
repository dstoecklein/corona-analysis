git checkout master
@ECHO OFF 
:: Section 2: Execute python script.
ECHO ============================
ECHO Receiving daily data...
ECHO ============================
CALL conda activate data_science
python ..\main_daily.py
=======
tox -e daily
ECHO ============================
ECHO Moving files to archive...
ECHO ============================
move ..\files\covid\*.csv ..\files\covid\archive
move ..\files\covid_vaccinations\*.csv ..\files\covid_vaccinations\archive
move ..\files\itcus\*.csv ..\files\itcus\archive
move ..\files\covid_rvalue\*.csv ..\files\covid_rvalue\archive
ECHO ============================
ECHO End
ECHO ============================
CALL conda deactivate
PAUSE