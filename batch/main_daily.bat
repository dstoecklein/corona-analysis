@ECHO OFF 
:: Section 2: Execute python script.
ECHO ============================
ECHO Receiving daily data...
ECHO ============================
python ..\main_daily.py
ECHO ============================
ECHO Moving files to archive...
ECHO ============================
move ..\files\covid\*.csv ..\files\covid\Archive
move ..\files\hospitals\*.csv ..\files\hospitals\Archive
ECHO ============================
ECHO End
ECHO ============================

PAUSE