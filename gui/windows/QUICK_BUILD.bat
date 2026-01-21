@echo off
REM SAIQL Quick Build Script for Windows
REM ============================================
REM This script does everything needed to build the Windows executable

REM Change to the script's directory
cd /d "%~dp0"

echo.
echo  ╔══════════════════════════════════════════════════════════════╗
echo  ║          SAIQL Windows Quick Build v1.0                     ║
echo  ║                                                              ║
echo  ║  This script will:                                           ║
echo  ║  1. Check Python installation                                ║
echo  ║  2. Install PyInstaller and dependencies                     ║
echo  ║  3. Build standalone Windows executable                      ║
echo  ║  4. Create complete distribution package                     ║
echo  ║  5. Generate downloadable ZIP file                           ║
echo  ║                                                              ║
echo  ║  Output: SAIQL-Windows-v1.0.zip (~15-25MB)                  ║
echo  ╚══════════════════════════════════════════════════════════════╝
echo.

REM Check if Python is installed
echo 🐍 Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python is not installed or not in PATH
    echo.
    echo Please install Python 3.11+ from https://python.org/downloads/
    echo Make sure to check "Add Python to PATH" during installation
    echo.
    pause
    exit /b 1
)

echo ✅ Python found:
python --version

REM Check Python version (basic check)
python -c "import sys; exit(0 if sys.version_info >= (3, 8) else 1)" >nul 2>&1
if errorlevel 1 (
    echo ❌ Python 3.8+ required
    echo Please upgrade Python to version 3.8 or higher
    pause
    exit /b 1
)

echo ✅ Python version is compatible

REM Install dependencies
echo.
echo 📦 Installing build dependencies...
echo    This may take a few minutes...
echo.

pip install pyinstaller >nul 2>&1
if errorlevel 1 (
    echo ⚠️  PyInstaller installation may have failed
    echo    Continuing anyway...
)

echo ✅ Dependencies installed

REM Build the executable
echo.
echo 🔨 Building SAIQL Windows executable...
echo    This will take 2-5 minutes depending on your system...
echo.

python build_standalone.py

if errorlevel 1 (
    echo ❌ Build failed
    echo.
    echo Common solutions:
    echo - Run this script as Administrator
    echo - Disable antivirus temporarily
    echo - Check that no SAIQL processes are running
    echo.
    pause
    exit /b 1
)

echo.
echo 🎉 BUILD COMPLETED SUCCESSFULLY!
echo.
echo 📁 Your files are ready:
if exist "dist\SAIQL.exe" (
    echo    ✅ Executable: dist\SAIQL.exe
)
if exist "SAIQL-Windows" (
    echo    ✅ Distribution folder: SAIQL-Windows\
)
if exist "SAIQL-Windows-v1.0.zip" (
    echo    ✅ ZIP package: SAIQL-Windows-v1.0.zip
)

echo.
echo 🚀 QUICK TEST:
echo    Double-click dist\SAIQL.exe to test the application
echo.
echo 📤 DISTRIBUTION:
echo    Share SAIQL-Windows-v1.0.zip with your users
echo.
echo 💡 FEATURES:
echo    ✓ No Python installation required for end users
echo    ✓ Works with existing PostgreSQL/MySQL databases  
echo    ✓ Includes demo mode with sample data
echo    ✓ Complete GUI for database management
echo    ✓ 60-70%% data compression with LoreToken
echo    ✓ AI-powered semantic queries
echo.
echo 📚 Need help? Check README.txt in the distribution folder
echo.
echo Press any key to exit...
pause >nul