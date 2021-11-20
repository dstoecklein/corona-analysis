@ECHO OFF 
:: Section 2: Execute python script.
ECHO ============================
ECHO Receiving daily data...
ECHO ============================
python ..\main_daily.py
ECHO ============================
ECHO Moving files to archive...
ECHO ============================
move ..\files\covid19\GER\*.csv ..\files\covid19\GER\Archive
move ..\files\hospitals\GER\*.csv ..\files\hospitals\GER\Archive
ECHO ============================
ECHO End
ECHO ============================

PAUSE