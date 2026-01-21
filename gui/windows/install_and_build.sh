#!/bin/bash
# SAIQL-Charlie Windows Build Script (Linux/WSL)
# ==============================================
# This script builds the Windows executable from Linux/WSL

echo
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║              SAIQL-Charlie Windows Builder v1.0             ║"
echo "║                                                              ║"
echo "║  Building Windows executable from Linux/WSL                 ║"
echo "║  1. Install required Python packages                        ║"
echo "║  2. Build the Windows executable                             ║"
echo "║  3. Create distribution package                              ║"
echo "╚══════════════════════════════════════════════════════════════╝"
echo

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed"
    echo "Please install Python 3.11+ using your package manager"
    exit 1
fi

echo "✅ Python found: $(python3 --version)"

# Check if pip is available
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 is not available"
    echo "Please install pip3 using your package manager"
    exit 1
fi

echo "✅ pip found: $(pip3 --version)"

# Install required packages
echo
echo "📦 Installing required packages..."
echo

pip3 install pyinstaller psycopg2-binary mysql-connector-python requests Pillow

if [ $? -ne 0 ]; then
    echo "⚠️  Some packages failed to install - continuing anyway"
    echo "This may cause build issues"
    read -p "Press Enter to continue..."
fi

echo
echo "✅ Package installation completed"

# Build the executable
echo
echo "🔨 Building SAIQL-Charlie executable..."
echo

python3 build_exe.py

if [ $? -ne 0 ]; then
    echo "❌ Build failed"
    echo "Check the error messages above"
    exit 1
fi

echo
echo "🎉 Build completed successfully!"
echo
echo "📁 Your files are ready:"
echo "   - Executable: dist/SAIQL-Charlie.exe"
echo "   - Distribution: SAIQL-Charlie-Windows/"
echo "   - ZIP package: SAIQL-Charlie-Windows-v1.0.zip"
echo
echo "💡 To test on Windows: Copy files to Windows and run SAIQL-Charlie.exe"
echo "📤 To distribute: Share SAIQL-Charlie-Windows-v1.0.zip"
echo

# Make the script executable
chmod +x "$0"
