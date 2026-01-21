#!/usr/bin/env python3
"""
SAIQL-Delta GUI Test Script
============================

Quick test to verify that the GUI can be imported and basic functionality works
before building the executable.

Usage:
    python test_gui.py

Author: Apollo & Claude
Version: 1.0.0
"""

import sys
import os

def test_imports():
    """Test that all required modules can be imported"""
    print("🔍 Testing module imports...")
    
    try:
        import tkinter as tk
        print("✅ tkinter imported successfully")
    except ImportError as e:
        print(f"❌ tkinter import failed: {e}")
        return False
    
    try:
        from tkinter import ttk, messagebox, filedialog, scrolledtext
        print("✅ tkinter submodules imported successfully")
    except ImportError as e:
        print(f"❌ tkinter submodules import failed: {e}")
        return False
    
    try:
        import threading, json, time, datetime, webbrowser
        print("✅ Standard library modules imported successfully")
    except ImportError as e:
        print(f"❌ Standard library import failed: {e}")
        return False
    
    # Test optional database modules
    database_modules = [
        ("psycopg2", "PostgreSQL adapter"),
        ("mysql.connector", "MySQL adapter"),
    ]
    
    for module, description in database_modules:
        try:
            __import__(module)
            print(f"✅ {description} available")
        except ImportError:
            print(f"⚠️  {description} not available (will use demo mode)")
    
    return True

def test_gui_creation():
    """Test that the GUI can be created without errors"""
    print("\n🖥️  Testing GUI creation...")
    
    try:
        # Add parent directories to path
        sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        
        # Import the GUI class
        from saiql_gui import SAIQLDelta
        
        print("✅ GUI class imported successfully")
        
        # Create GUI instance (but don't run mainloop)
        app = SAIQLDelta()
        print("✅ GUI instance created successfully")
        
        # Test some basic functionality
        app.log_message("Test message")
        print("✅ Logging functionality works")
        
        app.update_performance_display()
        print("✅ Performance display update works")
        
        # Cleanup
        app.root.destroy()
        print("✅ GUI cleanup successful")
        
        return True
        
    except Exception as e:
        print(f"❌ GUI creation failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_build_requirements():
    """Test that build requirements are available"""
    print("\n🔧 Testing build requirements...")
    
    try:
        import PyInstaller
        print("✅ PyInstaller is available")
    except ImportError:
        print("⚠️  PyInstaller not found - install with: pip install pyinstaller")
        return False
    
    # Check for UPX (optional)
    import subprocess
    try:
        subprocess.run(["upx", "--version"], capture_output=True, check=True)
        print("✅ UPX compression available")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("⚠️  UPX not found (optional compression tool)")
    
    return True

def main():
    """Run all tests"""
    print("SAIQL-Delta GUI Test Suite")
    print("============================")
    
    all_passed = True
    
    # Test imports
    if not test_imports():
        all_passed = False
    
    # Test GUI creation
    if not test_gui_creation():
        all_passed = False
    
    # Test build requirements
    if not test_build_requirements():
        all_passed = False
    
    print("\n" + "="*50)
    if all_passed:
        print("🎉 All tests passed! Ready to build executable.")
        print("\nTo build the Windows executable:")
        print("1. Run: python build_exe.py")
        print("2. Or use: install_and_build.bat (Windows)")
        print("3. Or use: ./install_and_build.sh (Linux/WSL)")
    else:
        print("❌ Some tests failed. Please fix issues before building.")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
