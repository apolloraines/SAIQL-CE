#!/usr/bin/env python3
"""
SAIQL-Delta Windows Executable Builder
=======================================

This script builds a standalone Windows executable (.exe) for SAIQL-Delta
using PyInstaller. The resulting executable includes all dependencies and
can run on Windows systems without Python installed.

Usage:
    python build_exe.py

Output:
    dist/SAIQL-Delta.exe - Standalone executable
    
Requirements:
    pip install pyinstaller
    
Author: Apollo & Claude
Version: 1.0.0
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path
from datetime import datetime

def build_executable():
    """Build the Windows executable using PyInstaller"""
    
    print("🚀 Building SAIQL-Delta Windows Executable")
    print("=" * 50)
    
    # Get the current directory
    current_dir = Path(__file__).parent
    project_root = current_dir.parent.parent
    
    # Change to the GUI directory
    os.chdir(current_dir)
    
    # PyInstaller command
    pyinstaller_cmd = [
        "pyinstaller",
        "--onefile",                           # Create single executable
        "--windowed",                          # Hide console window
        "--name=SAIQL-Delta",               # Executable name
        "--icon=assets/saiql_icon.ico",       # Application icon (if available)
        "--add-data=../../core;core",         # Include SAIQL core modules
        "--add-data=../../extensions;extensions",  # Include extensions
        "--add-data=../../data;data",         # Include data files
        "--add-data=../../config;config",     # Include config files
        "--hidden-import=tkinter",            # Ensure tkinter is included
        "--hidden-import=tkinter.ttk",        # Ensure ttk is included
        "--hidden-import=psycopg2",           # PostgreSQL adapter
        "--hidden-import=mysql.connector",   # MySQL adapter
        "--clean",                            # Clean build
        "saiql_gui.py"               # Main script
    ]
    
    try:
        print("📦 Running PyInstaller...")
        print(f"Command: {' '.join(pyinstaller_cmd)}")
        print()
        
        # Run PyInstaller
        result = subprocess.run(pyinstaller_cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Build successful!")
            print(f"📁 Executable created: {current_dir}/dist/SAIQL-Delta.exe")
            
            # Get file size
            exe_path = current_dir / "dist" / "SAIQL-Delta.exe"
            if exe_path.exists():
                size_mb = exe_path.stat().st_size / (1024 * 1024)
                print(f"📏 File size: {size_mb:.1f} MB")
            
            # Create distribution package
            create_distribution_package()
            
        else:
            print("❌ Build failed!")
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)
            return False
            
    except FileNotFoundError:
        print("❌ PyInstaller not found!")
        print("Please install PyInstaller: pip install pyinstaller")
        return False
    except Exception as e:
        print(f"❌ Build error: {str(e)}")
        return False
    
    return True

def create_distribution_package():
    """Create a complete distribution package"""
    print("\n📦 Creating distribution package...")
    
    current_dir = Path(__file__).parent
    dist_dir = current_dir / "dist"
    package_dir = current_dir / "SAIQL-Delta-Windows"
    
    # Create package directory
    if package_dir.exists():
        shutil.rmtree(package_dir)
    package_dir.mkdir()
    
    # Copy executable
    exe_source = dist_dir / "SAIQL-Delta.exe"
    exe_dest = package_dir / "SAIQL-Delta.exe"
    
    if exe_source.exists():
        shutil.copy2(exe_source, exe_dest)
        print(f"✅ Copied executable to {exe_dest}")
    
    # Create README for distribution
    readme_content = """SAIQL-Delta Database Manager for Windows
=========================================

Thank you for downloading SAIQL-Delta!

INSTALLATION:
1. Extract all files to a folder (e.g., C:\\Program Files\\SAIQL-Delta\\)
2. Double-click SAIQL-Delta.exe to run the application
3. No additional installation required!

FIRST TIME SETUP:
1. Choose your deployment mode:
   - Translation Layer: Use with existing PostgreSQL/MySQL databases
   - Standalone: Use SAIQL-Delta as your primary database

2. Configure your database connection:
   - For Translation mode: Enter your existing database credentials
   - For Standalone mode: Use default settings (localhost:5433)

3. Click "Connect" to establish connection

FEATURES:
- Visual SAIQL query builder
- Real-time performance monitoring
- Support for PostgreSQL, MySQL, Oracle, SQL Server
- 60-70% data compression with LoreToken technology
- AI-powered semantic queries
- Cross-database federation

SAMPLE QUERIES:
- *10[users]::name,email>>oQ
- *COUNT[orders]::*>>oQ
- *[products]::*|SIMILAR(name,'laptop')>>oQ

SYSTEM REQUIREMENTS:
- Windows 10 or Windows 11
- 2GB RAM minimum, 4GB recommended
- 500MB disk space

SUPPORT:
- Documentation: https://github.com/saiql/saiql-delta/docs/
- Issues: https://github.com/saiql/saiql-delta/issues/
- Community: https://discord.gg/saiql

LICENSE:
This software is released under the MIT License.
See LICENSE file for details.

Version: 1.0.0
Build Date: """ + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + """

Happy querying!
The SAIQL Team
"""
    
    # Write README
    readme_path = package_dir / "README.txt"
    with open(readme_path, 'w') as f:
        f.write(readme_content)
    
    # Copy license file if it exists
    license_source = current_dir.parent.parent / "LICENSE"
    if license_source.exists():
        shutil.copy2(license_source, package_dir / "LICENSE.txt")
    
    # Create sample configuration file
    sample_config = {
        "host": "localhost",
        "port": "5432",
        "database": "your_database",
        "username": "your_username",
        "compression_level": "6",
        "mode": "Translation"
    }
    
    config_path = package_dir / "sample_config.json"
    import json
    with open(config_path, 'w') as f:
        json.dump(sample_config, f, indent=2)
    
    # Create batch file for easy launching
    batch_content = """@echo off
title SAIQL-Delta Database Manager
echo Starting SAIQL-Delta...
echo.
echo If this is your first time running SAIQL-Delta:
echo 1. Choose Translation mode for existing databases
echo 2. Choose Standalone mode for new SAIQL installations
echo.
echo Press any key to continue...
pause > nul
SAIQL-Delta.exe
"""
    
    batch_path = package_dir / "Start SAIQL-Delta.bat"
    with open(batch_path, 'w') as f:
        f.write(batch_content)
    
    print(f"✅ Distribution package created: {package_dir}")
    print("\n📋 Package contents:")
    for item in package_dir.iterdir():
        size = item.stat().st_size if item.is_file() else 0
        size_str = f"({size // 1024}KB)" if size > 0 else ""
        print(f"   - {item.name} {size_str}")
    
    # Create ZIP file for easy distribution
    create_zip_distribution(package_dir)

def create_zip_distribution(package_dir):
    """Create ZIP file for distribution"""
    print("\n📦 Creating ZIP distribution...")
    
    import zipfile
    
    zip_path = package_dir.parent / "SAIQL-Delta-Windows-v1.0.zip"
    
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for file_path in package_dir.rglob('*'):
            if file_path.is_file():
                arcname = file_path.relative_to(package_dir.parent)
                zipf.write(file_path, arcname)
    
    zip_size = zip_path.stat().st_size / (1024 * 1024)
    print(f"✅ ZIP package created: {zip_path}")
    print(f"📏 ZIP size: {zip_size:.1f} MB")
    
    print(f"\n🎉 Windows distribution ready!")
    print(f"📁 Download: {zip_path}")
    print(f"📁 Folder: {package_dir}")

def install_requirements():
    """Install required packages for building"""
    requirements = [
        "pyinstaller",
        "psycopg2-binary",  # PostgreSQL adapter
        "mysql-connector-python",  # MySQL adapter
    ]
    
    print("📦 Installing build requirements...")
    for req in requirements:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", req])
            print(f"✅ Installed {req}")
        except subprocess.CalledProcessError:
            print(f"⚠️  Could not install {req} - continuing anyway")

if __name__ == "__main__":
    print("SAIQL-Delta Windows Executable Builder")
    print("=======================================")
    
    # Check if we should install requirements
    if "--install-deps" in sys.argv:
        install_requirements()
        print()
    
    # Build the executable
    success = build_executable()
    
    if success:
        print("\n🎉 Build completed successfully!")
        print("\n📋 Next steps:")
        print("1. Test the executable: ./dist/SAIQL-Delta.exe")
        print("2. Distribute the ZIP file: SAIQL-Delta-Windows-v1.0.zip")
        print("3. Share with users for testing")
    else:
        print("\n❌ Build failed - check error messages above")
        sys.exit(1)
