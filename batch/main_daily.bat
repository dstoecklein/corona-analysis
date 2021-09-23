@ECHO OFF 
TITLE Execute python script on anaconda environment
ECHO Please Wait...
:: Section 1: Activate the environment.
ECHO ============================
ECHO Conda Activate
ECHO ============================
@CALL "%USERPROFILE%\anaconda3\Scripts\activate.bat" %USERPROFILE%\anaconda3
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