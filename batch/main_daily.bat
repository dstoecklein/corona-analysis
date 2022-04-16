git checkout master
@ECHO OFF 
:: Section 2: Execute python script.
ECHO ============================
ECHO Receiving daily data...
ECHO ============================
CALL conda activate data_science
tox -e daily
ECHO ============================
ECHO Moving files to archive...
ECHO ============================
move ..\files\covid\*.csv ..\files\covid\archive
move ..\files\hospitals\*.csv ..\files\hospitals\archive
ECHO ============================
ECHO End
ECHO ============================
CALL conda deactivate
PAUSE