#!/usr/bin/env python3
"""
Test the LoreToken safety integration in SAIQL-Delta
"""

from pathlib import Path
import os
import sys
import os
sys.path.insert(0, str(Path(os.environ.get('SAIQL_HOME', Path.home() / 'SAIQL'))))

from core import SAIQLStorageEngine, SAIQLSchema, SAIQLDataType

def test_safety_integration():
    print("Testing SAIQL-Delta Safety Integration")
    print("=" * 50)
    
    # Create test database
    test_db = "/tmp/test_saiql_safety"
    os.makedirs(test_db, exist_ok=True)
    
    # Initialize engine
    print("\n1. Initializing storage engine...")
    engine = SAIQLStorageEngine(test_db)
    
    # Check if safety is enabled
    if engine.safety:
        print("✅ Safety module loaded successfully")
        print(f"  - Strict mode: {engine.safety.strict_mode}")
        print(f"  - Drift budget: {engine.safety.drift_budget}")
        print(f"  - Ambiguity policy: {engine.safety.ambiguity_policy}")
    else:
        print("❌ Safety module not available")
        return
    
    # Create test table
    print("\n2. Creating test table...")
    columns = {
        "symbol": SAIQLDataType.TEXT,
        "price": SAIQLDataType.TEXT,
        "volume": SAIQLDataType.TEXT,
        "timestamp": SAIQLDataType.TIMESTAMP
    }
    engine.create_table("test_prices", columns, compression_level=7)
    print("✅ Table created")
    
    # Insert test record
    print("\n3. Inserting test record with safety validation...")
    test_record = {
        "symbol": "BTC-USD",
        "price": "117000",
        "volume": "1.234",
        "timestamp": "2025-08-18T12:00:00"
    }
    
    record_id = engine.insert_record("test_prices", test_record)
    if record_id:
        print(f"✅ Record inserted with ID: {record_id}")
    else:
        print("❌ Record insertion failed")
    
    # Check if safety headers were added
    print("\n4. Checking for safety headers in storage file...")
    table_file = os.path.join(test_db, "test_prices.sdt")
    if os.path.exists(table_file):
        with open(table_file, 'r') as f:
            first_lines = []
            for i, line in enumerate(f):
                if i < 5:
                    first_lines.append(line.strip())
                else:
                    break
        
        has_safety = False
        for line in first_lines:
            if "$LORE" in line or "$CTX" in line or "$TIME" in line:
                has_safety = True
                print(f"  Found: {line[:50]}...")
        
        if has_safety:
            print("✅ Safety headers present in storage")
        else:
            print("⚠️ No safety headers found (checking records...)")
            
            # Check if records have safety metadata
            for line in first_lines:
                if "$SIG" in line or "$TIME" in line:
                    print(f"  Safety metadata in record: {line[:50]}...")
                    has_safety = True
            
            if has_safety:
                print("✅ Safety metadata present in records")
    
    # Query records back
    print("\n5. Querying records...")
    results = engine.select_records("test_prices")
    if results:
        print(f"✅ Retrieved {len(results)} record(s)")
        for record in results:
            metadata = record.get('__saiql_metadata__', {})
            print(f"  - Record {metadata.get('record_id')}: compression ratio {metadata.get('compression_ratio', 0):.2f}")
    
    print("\n" + "=" * 50)
    print("Safety integration test complete!")
    
    # Cleanup
    import shutil
    shutil.rmtree(test_db, ignore_errors=True)

if __name__ == "__main__":
    test_safety_integration()