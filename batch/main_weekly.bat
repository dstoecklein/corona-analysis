@ECHO OFF
ECHO Please Wait...
:: Section 2: Execute python script.
ECHO ============================
ECHO Receiving weekly data...
ECHO ============================
python ..\main_weekly.py
ECHO ============================
ECHO Moving files to archive...
ECHO ============================
move ..\files\tests\*.csv ..\files\tests\Archive
move ..\files\mortality\*.csv ..\files\mortality\Archive
ECHO End
ECHO ============================

PAUSE