#!/bin/bash
# SAIQL Quick Build Script for Linux
# ==========================================
# This script does everything needed to build the Linux executable

set -e

echo
echo "  ╔══════════════════════════════════════════════════════════════╗"
echo "  ║           SAIQL Linux Quick Build v1.0                      ║"
echo "  ║                                                              ║"
echo "  ║  This script will:                                           ║"
echo "  ║  1. Check Python installation                                ║"
echo "  ║  2. Install PyInstaller and dependencies                     ║"
echo "  ║  3. Build standalone Linux executable                        ║"
echo "  ║  4. Create complete distribution package                     ║"
echo "  ║  5. Generate downloadable tarball                            ║"
echo "  ║                                                              ║"
echo "  ║  Output: SAIQL-Linux-*.tar.gz (~15-25MB)                    ║"
echo "  ╚══════════════════════════════════════════════════════════════╝"
echo

# Color codes for better output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored messages
print_status() {
    echo -e "${BLUE}🐧 $1${NC}"
}

print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Detect Linux distribution
if [ -f /etc/os-release ]; then
    . /etc/os-release
    DISTRO_NAME=$ID
    DISTRO_VERSION=$VERSION_ID
    DISTRO_PRETTY=$PRETTY_NAME
else
    DISTRO_NAME="linux"
    DISTRO_VERSION="unknown"
    DISTRO_PRETTY="Linux"
fi

print_status "Detected distribution: $DISTRO_PRETTY"
print_status "Architecture: $(uname -m)"

# Check if Python is installed
print_status "Checking Python installation..."
if command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
elif command -v python &> /dev/null; then
    PYTHON_CMD="python"
else
    print_error "Python is not installed or not in PATH"
    echo
    echo "Please install Python 3.6+ using your package manager:"
    case $DISTRO_NAME in
        ubuntu|debian|mint)
            echo "  sudo apt update && sudo apt install python3 python3-pip python3-dev python3-tk"
            ;;
        fedora)
            echo "  sudo dnf install python3 python3-pip python3-devel python3-tkinter"
            ;;
        centos|rhel)
            echo "  sudo yum install python3 python3-pip python3-devel tkinter"
            ;;
        arch|manjaro)
            echo "  sudo pacman -S python python-pip tk"
            ;;
        opensuse*)
            echo "  sudo zypper install python3 python3-pip python3-devel python3-tk"
            ;;
        *)
            echo "  Please install Python 3.6+ and tkinter using your distribution's package manager"
            ;;
    esac
    echo
    exit 1
fi

print_success "Python found:"
$PYTHON_CMD --version

# Check Python version (basic check)
PYTHON_VERSION=$($PYTHON_CMD -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
PYTHON_MAJOR=$($PYTHON_CMD -c "import sys; print(sys.version_info.major)")
PYTHON_MINOR=$($PYTHON_CMD -c "import sys; print(sys.version_info.minor)")

if [ "$PYTHON_MAJOR" -lt 3 ] || ([ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 6 ]); then
    print_error "Python 3.6+ required, found $PYTHON_VERSION"
    echo "Please upgrade Python to version 3.6 or higher"
    exit 1
fi

print_success "Python version is compatible ($PYTHON_VERSION)"

# Check if tkinter is available
print_status "Checking tkinter (GUI toolkit)..."
if $PYTHON_CMD -c "import tkinter" 2>/dev/null; then
    print_success "tkinter is available"
else
    print_error "tkinter is not available"
    echo
    echo "Please install tkinter using your package manager:"
    case $DISTRO_NAME in
        ubuntu|debian|mint)
            echo "  sudo apt install python3-tk"
            ;;
        fedora)
            echo "  sudo dnf install python3-tkinter"
            ;;
        centos|rhel)
            echo "  sudo yum install tkinter"
            ;;
        arch|manjaro)
            echo "  sudo pacman -S tk"
            ;;
        opensuse*)
            echo "  sudo zypper install python3-tk"
            ;;
        *)
            echo "  Please install tkinter using your distribution's package manager"
            ;;
    esac
    echo
    exit 1
fi

# Install dependencies
echo
print_status "Installing build dependencies..."
echo "   This may take a few minutes..."
echo

# Try to install PyInstaller
if $PYTHON_CMD -m pip install pyinstaller --user --quiet; then
    print_success "PyInstaller installed successfully"
elif $PYTHON_CMD -m pip install pyinstaller --quiet 2>/dev/null; then
    print_success "PyInstaller installed successfully (system-wide)"
else
    print_warning "PyInstaller installation may have failed"
    echo "    Attempting to install system packages..."
    
    # Try to install system dependencies
    case $DISTRO_NAME in
        ubuntu|debian|mint)
            if command -v apt &> /dev/null; then
                sudo apt update && sudo apt install -y python3-dev python3-pip
            fi
            ;;
        fedora)
            if command -v dnf &> /dev/null; then
                sudo dnf install -y python3-devel python3-pip
            fi
            ;;
        centos|rhel)
            if command -v yum &> /dev/null; then
                sudo yum install -y python3-devel python3-pip
            fi
            ;;
    esac
    
    # Try PyInstaller installation again
    if ! $PYTHON_CMD -m pip install pyinstaller --user --quiet; then
        print_error "Failed to install PyInstaller"
        echo "Please try installing manually:"
        echo "  pip3 install --user pyinstaller"
        exit 1
    fi
    print_success "PyInstaller installed after installing system dependencies"
fi

# Build the executable
echo
print_status "Building SAIQL Linux executable..."
echo "   This will take 3-7 minutes depending on your system..."
echo

if $PYTHON_CMD build_linux.py --install-deps; then
    print_success "Build completed successfully!"
else
    print_error "Build failed"
    echo
    echo "Common solutions:"
    echo "- Install missing system packages for your distribution"
    echo "- Try running with sudo if permissions are needed"
    echo "- Check that no SAIQL processes are running"
    echo "- Ensure you have enough disk space (500MB required)"
    echo
    exit 1
fi

echo
echo "🎉 BUILD COMPLETED SUCCESSFULLY!"
echo

print_success "Your files are ready:"
if [ -f "dist/SAIQL-Charlie" ]; then
    echo "   ✅ Executable: dist/SAIQL"
fi
if [ -d "SAIQL-Charlie-Linux" ]; then
    echo "   ✅ Distribution folder: SAIQL-Linux/"
fi
if ls SAIQL-Charlie-Linux-*.tar.gz 1> /dev/null 2>&1; then
    TARBALL=$(ls SAIQL-Linux-*.tar.gz | head -1)
    echo "   ✅ Distribution package: $TARBALL"
fi

echo
print_status "QUICK TEST:"
echo "   ./dist/SAIQL"
echo "   (GUI application will open)"
echo

print_status "INSTALLATION OPTIONS:"
echo "   1. Portable: Run directly from any directory"
echo "   2. System-wide: cd SAIQL-Linux && sudo ./install.sh"
echo "   3. User-only: cd SAIQL-Linux && ./install.sh"
echo

print_status "DISTRIBUTION:"
if ls SAIQL-Charlie-Linux-*.tar.gz 1> /dev/null 2>&1; then
    TARBALL=$(ls SAIQL-Linux-*.tar.gz | head -1)
    echo "   Share $TARBALL with your users"
fi
echo

print_status "FEATURES:"
echo "   ✓ No Python installation required for end users"
echo "   ✓ Works with existing PostgreSQL/MySQL/MariaDB databases"
echo "   ✓ Includes demo mode with sample data"
echo "   ✓ Complete GUI for database management"
echo "   ✓ 60-70% data compression with LoreToken"
echo "   ✓ AI-powered semantic queries"
echo "   ✓ Native Linux integration"
echo

print_status "COMPATIBILITY:"
echo "   ✓ Ubuntu 18.04+ / Debian 9+"
echo "   ✓ Fedora 30+ / CentOS 7+"
echo "   ✓ Arch Linux / Manjaro"
echo "   ✓ openSUSE Leap 15+"
echo "   ✓ Linux Mint 19+"
echo

echo "📚 Need help? Check README.txt in the distribution folder"
echo

echo "Press any key to exit..."
read -n 1 -s
