@echo off
REM ============================================================
REM  GSE Pipeline — Windows Task Scheduler Setup
REM  Run this ONCE as Administrator to register the daily task.
REM ============================================================

REM --- EDIT THESE TWO LINES TO MATCH YOUR MACHINE ---
SET PROJECT_DIR=C:\Users\ENVY 15\OneDrive\Desktop\GSE_PIPE
SET PYTHON_EXE=C:\Users\ENVY 15\AppData\Local\Programs\Python\Python311\python.exe
REM ---------------------------------------------------

SET TASK_NAME=GSE_Intelligence_Pipeline
SET RUN_TIME=08:00

echo Creating scheduled task: %TASK_NAME%
echo Project: %PROJECT_DIR%
echo Python:  %PYTHON_EXE%
echo Runs at: %RUN_TIME% daily
echo.

SCHTASKS /CREATE ^
  /TN "%TASK_NAME%" ^
  /TR "\"%PYTHON_EXE%\" \"%PROJECT_DIR%\pipeline.py\"" ^
  /SC DAILY ^
  /ST %RUN_TIME% ^
  /RL HIGHEST ^
  /F

IF %ERRORLEVEL% EQU 0 (
    echo.
    echo Task created successfully.
    echo The pipeline will run every day at %RUN_TIME%.
    echo Logs will appear in: %PROJECT_DIR%\logs\
) ELSE (
    echo.
    echo Failed to create task. Make sure you ran this as Administrator.
)

pause
