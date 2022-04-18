git checkout master
@ECHO OFF
ECHO Please Wait...
:: Section 2: Execute python script.
ECHO ============================
ECHO Receiving weekly data...
ECHO ============================
CALL conda activate data_science
=======
tox -e weekly
ECHO ============================
ECHO Moving files to archive...
ECHO ============================
move ..\files\covid_tests\*.ftr ..\files\covid_tests\archive
move ..\files\mortalities\*.ftr ..\files\mortalities\archive
ECHO End
ECHO ============================
CALL conda deactivate
PAUSE