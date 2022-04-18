git checkout master
@ECHO OFF 
:: Section 2: Execute python script.
ECHO ============================
ECHO Receiving daily data...
ECHO ============================
CALL conda activate data_science
=======
tox -e daily
ECHO ============================
ECHO Moving files to archive...
ECHO ============================
move ..\files\covid\*.ftr ..\files\covid\archive
move ..\files\covid_vaccinations\*.ftr ..\files\covid_vaccinations\archive
move ..\files\itcus\*.ftr ..\files\itcus\archive
move ..\files\covid_rvalue\*.ftr ..\files\covid_rvalue\archive
ECHO ============================
ECHO End
ECHO ============================
CALL conda deactivate
PAUSE