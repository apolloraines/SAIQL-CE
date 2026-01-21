#!/usr/bin/env python3
"""
SAIQL Smoke Test
================
A simple "Hello World" script to verify that SAIQL is installed and working correctly.
Run this after installation to ensure everything is set up properly.
"""

import sys
import os
import time
from pathlib import Path

# Add project root to path if running from source
sys.path.append(str(Path(__file__).parent.parent))

try:
    from core.engine import SAIQLEngine
    print("   SAIQL Core imported successfully")
except ImportError as e:
    print(f"   Failed to import SAIQL Core: {e}")
    print("   Did you run 'pip install -r requirements.txt'?")
    sys.exit(1)

def run_smoke_test():
    print("\n   Running SAIQL Smoke Test...")
    print("==============================")
    
    # 1. Initialize Engine
    try:
        db = SAIQLEngine(debug=True)
        print("   Engine initialized")
    except Exception as e:
        print(f"   Engine initialization failed: {e}")
        return False

    # 2. Create Table
    try:
        db.execute("CREATE TABLE smoke_test (id INT, message TEXT)")
        print("   Table 'smoke_test' created")
    except Exception as e:
        print(f"   Table creation failed: {e}")
        return False

    # 3. Insert Data
    try:
        db.execute("INSERT INTO smoke_test VALUES (1, 'Hello SAIQL')")
        print("   Data inserted")
    except Exception as e:
        print(f"   Data insertion failed: {e}")
        return False

    # 4. Query Data
    try:
        result = db.execute("SELECT * FROM smoke_test")
        if result.success:
            print(f"   Query processed successfully")
            if result.sql_generated:
                print(f"   SQL Generated: {result.sql_generated}")
            if result.data:
                print(f"   Response: {result.data[0]}")
        else:
            print(f"   Query failed: {result.error_message}")
            return False
    except Exception as e:
        print(f"   Query execution failed: {e}")
        return False

    print("\n   Smoke test PASSED! SAIQL Engine is running.")
    return True

if __name__ == "__main__":
    success = run_smoke_test()
    sys.exit(0 if success else 1)
