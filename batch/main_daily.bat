@ECHO OFF 
:: Section 2: Execute python script.
ECHO ============================
ECHO Receiving daily data...
ECHO ============================
python ..\main_daily.py
ECHO ============================
ECHO Moving file to archive...
ECHO ============================
move ..\files\covid19\GER\*.csv ..\files\covid19\GER\Archive
ECHO ============================
ECHO End
ECHO ============================

PAUSE