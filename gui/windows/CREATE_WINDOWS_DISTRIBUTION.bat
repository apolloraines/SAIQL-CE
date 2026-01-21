@echo off
REM SAIQL-Charlie Windows Distribution Creator
REM ==========================================
REM Creates a complete Windows distribution package after building

echo.
echo  ╔══════════════════════════════════════════════════════════════╗
echo  ║               SAIQL-Charlie Distribution Creator             ║
echo  ║                                                              ║
echo  ║  This script creates a complete Windows distribution         ║
echo  ║  package ready for end users.                               ║
echo  ║                                                              ║
echo  ║  Prerequisites: SAIQL-Charlie.exe must exist in dist/       ║
echo  ╚══════════════════════════════════════════════════════════════╝
echo.

REM Check if executable exists
if not exist "dist\SAIQL-Charlie.exe" (
    echo ❌ SAIQL-Charlie.exe not found in dist\ directory
    echo.
    echo Please run QUICK_BUILD.bat first to build the executable
    echo.
    pause
    exit /b 1
)

echo ✅ Found SAIQL-Charlie.exe
echo 📦 Creating distribution package...

REM Create distribution directory
if exist "SAIQL-Charlie-Windows" rmdir /s /q "SAIQL-Charlie-Windows"
mkdir "SAIQL-Charlie-Windows"

REM Copy executable
copy "dist\SAIQL-Charlie.exe" "SAIQL-Charlie-Windows\"
echo    ✓ Copied executable

REM Copy documentation
copy "USER_README.txt" "SAIQL-Charlie-Windows\README.txt"
copy "..\..\LICENSE" "SAIQL-Charlie-Windows\LICENSE.txt"
echo    ✓ Copied documentation

REM Create sample configuration
echo { > "SAIQL-Charlie-Windows\sample_config.json"
echo   "deployment_mode": "translation_layer", >> "SAIQL-Charlie-Windows\sample_config.json"
echo   "database": { >> "SAIQL-Charlie-Windows\sample_config.json"
echo     "type": "postgresql", >> "SAIQL-Charlie-Windows\sample_config.json"
echo     "host": "localhost", >> "SAIQL-Charlie-Windows\sample_config.json"
echo     "port": 5432, >> "SAIQL-Charlie-Windows\sample_config.json"
echo     "database": "myapp", >> "SAIQL-Charlie-Windows\sample_config.json"
echo     "username": "user", >> "SAIQL-Charlie-Windows\sample_config.json"
echo     "password": "password" >> "SAIQL-Charlie-Windows\sample_config.json"
echo   }, >> "SAIQL-Charlie-Windows\sample_config.json"
echo   "compression_level": 7, >> "SAIQL-Charlie-Windows\sample_config.json"
echo   "demo_mode": true >> "SAIQL-Charlie-Windows\sample_config.json"
echo } >> "SAIQL-Charlie-Windows\sample_config.json"
echo    ✓ Created sample configuration

REM Create sample queries
echo REM SAIQL Sample Queries > "SAIQL-Charlie-Windows\sample_queries.saiql"
echo REM ==================== >> "SAIQL-Charlie-Windows\sample_queries.saiql"
echo. >> "SAIQL-Charlie-Windows\sample_queries.saiql"
echo REM Basic selection queries >> "SAIQL-Charlie-Windows\sample_queries.saiql"
echo *5[users]::name,email>>oQ >> "SAIQL-Charlie-Windows\sample_queries.saiql"
echo *10[products]::name,price^|category='electronics'>>oQ >> "SAIQL-Charlie-Windows\sample_queries.saiql"
echo. >> "SAIQL-Charlie-Windows\sample_queries.saiql"
echo REM Aggregation queries >> "SAIQL-Charlie-Windows\sample_queries.saiql"
echo *COUNT[orders]::*>>oQ >> "SAIQL-Charlie-Windows\sample_queries.saiql"
echo *SUM[sales]::amount^|date^>'2024-01-01'>>oQ >> "SAIQL-Charlie-Windows\sample_queries.saiql"
echo. >> "SAIQL-Charlie-Windows\sample_queries.saiql"
echo REM Advanced semantic queries >> "SAIQL-Charlie-Windows\sample_queries.saiql"
echo *5[customers]::*^|SIMILAR(name,'John')>>oQ >> "SAIQL-Charlie-Windows\sample_queries.saiql"
echo =J[users+orders]::users.name,COUNT(orders.id)>>GROUP(users.id)>>oQ >> "SAIQL-Charlie-Windows\sample_queries.saiql"
echo    ✓ Created sample queries

REM Create launcher script
echo @echo off > "SAIQL-Charlie-Windows\Start SAIQL-Charlie.bat"
echo REM SAIQL-Charlie Launcher >> "SAIQL-Charlie-Windows\Start SAIQL-Charlie.bat"
echo REM ====================== >> "SAIQL-Charlie-Windows\Start SAIQL-Charlie.bat"
echo. >> "SAIQL-Charlie-Windows\Start SAIQL-Charlie.bat"
echo echo Starting SAIQL-Charlie... >> "SAIQL-Charlie-Windows\Start SAIQL-Charlie.bat"
echo start "" "SAIQL-Charlie.exe" >> "SAIQL-Charlie-Windows\Start SAIQL-Charlie.bat"
echo    ✓ Created launcher script

REM Create ZIP package
if exist "SAIQL-Charlie-Windows-v1.0.zip" del "SAIQL-Charlie-Windows-v1.0.zip"
powershell -command "Compress-Archive -Path 'SAIQL-Charlie-Windows' -DestinationPath 'SAIQL-Charlie-Windows-v1.0.zip'"

if exist "SAIQL-Charlie-Windows-v1.0.zip" (
    echo    ✓ Created ZIP package
) else (
    echo    ⚠️  ZIP creation may have failed, but folder is ready
)

echo.
echo 🎉 DISTRIBUTION PACKAGE READY!
echo.
echo 📁 Files created:
echo    ✅ SAIQL-Charlie-Windows\ - Complete distribution folder
if exist "SAIQL-Charlie-Windows-v1.0.zip" (
    echo    ✅ SAIQL-Charlie-Windows-v1.0.zip - Ready to distribute
)

REM Show package contents
echo.
echo 📋 Package contents:
dir /b "SAIQL-Charlie-Windows"

REM Calculate sizes
for %%F in ("SAIQL-Charlie-Windows\SAIQL-Charlie.exe") do echo    📏 Executable size: %%~zF bytes
if exist "SAIQL-Charlie-Windows-v1.0.zip" (
    for %%F in ("SAIQL-Charlie-Windows-v1.0.zip") do echo    📏 ZIP package size: %%~zF bytes
)

echo.
echo 🚀 DISTRIBUTION READY!
echo.
echo 📤 To distribute:
echo    1. Share SAIQL-Charlie-Windows-v1.0.zip with users
echo    2. Users extract and run SAIQL-Charlie.exe
echo    3. No Python installation required for end users
echo.
echo 🧪 To test:
echo    1. Navigate to SAIQL-Charlie-Windows\
echo    2. Double-click SAIQL-Charlie.exe
echo    3. Try demo mode and sample queries
echo.
pause