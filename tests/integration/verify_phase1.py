#!/usr/bin/env python3
"""
Verification script for Phase 1: Stability & API
Tests the new public API, trace IDs, and error hierarchy.
"""

import sys
import os
from pathlib import Path

# Ensure we can import SAIQL
# We are in SAIQL/tests, so we need to go up two levels to find the SAIQL package
# if we want to import it as "import SAIQL"
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import SAIQL
from SAIQL.core.errors import SAIQLError, ErrorCode

def test_public_api():
    print("1. Testing Public API Façade...")
    
    # Test get_engine
    engine = SAIQL.get_engine()
    assert engine is not None
    print("   [OK] get_engine() returned instance")
    
    # Test execute
    print("   Executing query via SAIQL.execute()...")
    # Using a simple query that should pass parsing/compilation even without DB
    result = SAIQL.execute("* >> users") 
    
    assert result is not None
    print(f"   [OK] execute() returned result (success={result.success})")
    
    # Test connect
    db = SAIQL.connect("test.db")
    assert db is not None
    print("   [OK] connect() returned instance")

def test_trace_id():
    print("\n2. Testing Trace IDs...")
    result = SAIQL.execute("* >> users")
    
    trace_id = result.metadata.get('trace_id')
    print(f"   Trace ID: {trace_id}")
    assert trace_id is not None
    assert len(trace_id) > 0
    print("   [OK] Trace ID present in result metadata")

def test_error_hierarchy():
    print("\n3. Testing Error Hierarchy...")
    
    # Force a syntax error
    print("   Executing invalid query...")
    result = SAIQL.execute("INVALID QUERY SYNTAX")
    
    print(f"   Success: {result.success}")
    print(f"   Error Message: {result.error_message}")
    print(f"   Error Phase: {result.error_phase}")
    print(f"   Error Code: {result.metadata.get('error_code')}")
    
    # Note: The current pipeline might catch this as a generic exception or specific ParseError
    # We want to verify it's handled gracefully and has metadata
    
    assert result.success is False
    assert result.error_message is not None
    print("   [OK] Error handled gracefully")

if __name__ == "__main__":
    print("SAIQL Phase 1 Verification")
    print("==========================")
    
    try:
        test_public_api()
        test_trace_id()
        test_error_hierarchy()
        print("\n[SUCCESS] All Phase 1 tests passed!")
    except Exception as e:
        print(f"\n[FAILURE] Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
