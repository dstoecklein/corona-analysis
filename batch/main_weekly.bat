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
ECHO Receiving weekly data...
ECHO ============================
python ..\main_weekly.py
ECHO ============================
ECHO End
ECHO ============================

PAUSE