@echo off
REM SAIQL Windows Build Script
REM ==================================
REM This script installs dependencies and builds the Windows executable

echo.
echo  ╔══════════════════════════════════════════════════════════════╗
echo  ║              SAIQL Windows Builder v1.0                     ║
echo  ║                                                              ║
echo  ║  This script will:                                           ║
echo  ║  1. Install required Python packages                        ║
echo  ║  2. Build the Windows executable                             ║
echo  ║  3. Create distribution package                              ║
echo  ╚══════════════════════════════════════════════════════════════╝
echo.

REM Check if Python is installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python is not installed or not in PATH
    echo Please install Python 3.11+ from https://python.org/downloads/
    pause
    exit /b 1
)

echo ✅ Python found
python --version

REM Check if pip is available
pip --version >nul 2>&1
if errorlevel 1 (
    echo ❌ pip is not available
    echo Please ensure pip is installed with Python
    pause
    exit /b 1
)

echo ✅ pip found

REM Install required packages
echo.
echo 📦 Installing required packages...
echo.

pip install pyinstaller psycopg2-binary mysql-connector-python requests Pillow

if errorlevel 1 (
    echo ⚠️  Some packages failed to install - continuing anyway
    echo This may cause build issues
    pause
)

echo.
echo ✅ Package installation completed

REM Build the executable
echo.
echo 🔨 Building SAIQL executable...
echo.

python build_exe.py

if errorlevel 1 (
    echo ❌ Build failed
    echo Check the error messages above
    pause
    exit /b 1
)

echo.
echo 🎉 Build completed successfully!
echo.
echo 📁 Your files are ready:
echo    - Executable: dist\SAIQL.exe
echo    - Distribution: SAIQL-Windows\
echo    - ZIP package: SAIQL-Windows-v1.0.zip
echo.
echo 💡 To test: Double-click dist\SAIQL.exe
echo 📤 To distribute: Share SAIQL-Windows-v1.0.zip
echo.
echo Press any key to exit...
pause >nul