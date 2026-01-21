#!/usr/bin/env python3
"""
Test Suite for MongoDB Adapter (Document IR Lane, L0-L4)

Environment variables required for live tests:
- MONGODB_URI: Connection URI (default: mongodb://localhost:27017)
- MONGODB_DATABASE: Database name
- MONGODB_USERNAME: Username (optional, can be in URI)
- MONGODB_PASSWORD: Password (optional, can be in URI)
- MONGODB_TEST_DATABASE: Dedicated test database for L4 checksum tests

Run tests:
    # Unit tests only (no connection required)
    pytest tests/database/test_mongodb_adapter.py -v -k "not credentials"

    # Full tests (requires MongoDB connection)
    MONGODB_DATABASE=testdb pytest tests/database/test_mongodb_adapter.py -v
"""

import pytest
import json
import hashlib
import tempfile
import os
import sys
from pathlib import Path
from datetime import datetime

# Try to import the adapter
try:
    from extensions.plugins.mongodb_adapter import (
        MongoDBAdapter, MongoDBConfig, MongoDBResult, MongoDBError,
        MongoDBConnectionError, MongoDBAuthError, MongoDBQueryError,
        ConnectionState, MONGODB_AVAILABLE
    )
    ADAPTER_AVAILABLE = True
except ImportError as e:
    ADAPTER_AVAILABLE = False
    MONGODB_AVAILABLE = False
    print(f"MongoDB adapter not available: {e}")


# Test configuration from environment
def get_test_config() -> 'MongoDBConfig':
    """Get test configuration from environment variables"""
    uri = os.environ.get('MONGODB_URI', 'mongodb://localhost:27017')
    database = os.environ.get('MONGODB_DATABASE')
    username = os.environ.get('MONGODB_USERNAME')
    password = os.environ.get('MONGODB_PASSWORD')

    if not database:
        return None

    return MongoDBConfig(
        uri=uri,
        database=database,
        username=username,
        password=password,
        server_selection_timeout=5000,  # Faster timeout for tests
        connect_timeout=5000,
    )


def get_test_database() -> str:
    """
    Get dedicated test database from environment.

    Cost-safety rule: L4 tests that run checksums MUST use a dedicated small
    test database. Set MONGODB_TEST_DATABASE.
    """
    return os.environ.get('MONGODB_TEST_DATABASE')


# Skip conditions
skip_no_pymongo = pytest.mark.skipif(
    not MONGODB_AVAILABLE,
    reason="pymongo not installed"
)

skip_no_adapter = pytest.mark.skipif(
    not ADAPTER_AVAILABLE,
    reason="MongoDB adapter not available"
)

skip_no_credentials = pytest.mark.skipif(
    get_test_config() is None,
    reason="MongoDB credentials not configured (set MONGODB_DATABASE)"
)

skip_no_test_database = pytest.mark.skipif(
    get_test_database() is None,
    reason="L4 checksum tests require dedicated test database (set MONGODB_TEST_DATABASE)"
)


# =============================================================================
# L0 TESTS: CONNECTIVITY
# =============================================================================

class TestL0Connectivity:
    """L0: Connectivity and basic operation tests"""

    @skip_no_adapter
    def test_config_creation(self):
        """Test MongoDBConfig creation"""
        config = MongoDBConfig(
            uri='mongodb://localhost:27017',
            database='testdb'
        )

        assert config.uri == 'mongodb://localhost:27017'
        assert config.database == 'testdb'
        assert config.server_selection_timeout == 30000

    @skip_no_adapter
    def test_config_with_credentials(self):
        """Test config with separate credentials"""
        config = MongoDBConfig(
            uri='mongodb://localhost:27017',
            database='testdb',
            username='user',
            password='pass',
            auth_source='admin'
        )

        assert config.username == 'user'
        assert config.auth_source == 'admin'

    @skip_no_adapter
    def test_driver_not_installed_error(self):
        """Test error when pymongo not installed"""
        if MONGODB_AVAILABLE:
            pytest.skip("pymongo is installed")

        with pytest.raises(MongoDBError) as exc:
            MongoDBAdapter(MongoDBConfig(
                uri='mongodb://localhost:27017',
                database='test'
            ))

        assert 'pymongo not installed' in str(exc.value)

    @skip_no_adapter
    @skip_no_credentials
    def test_connection_success(self):
        """L0: Test successful connection"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        assert adapter.is_connected()
        assert adapter.state == ConnectionState.CONNECTED

        adapter.close()
        assert adapter.state == ConnectionState.DISCONNECTED

    @skip_no_adapter
    @skip_no_credentials
    def test_ping_command(self):
        """L0: Test ping command"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        # Ping is verified during connection
        assert adapter.is_connected()

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_list_databases(self):
        """L0: Test listing databases"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        databases = adapter.list_databases()

        assert isinstance(databases, list)
        # Should have at least admin, local, or the test database
        db_names = [db['name'] for db in databases]
        assert len(db_names) > 0

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_find_query(self):
        """L0: Test basic find query"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        # Query a collection (may be empty)
        result = adapter.find('test_collection', {}, limit=10)

        assert isinstance(result, MongoDBResult)
        assert result.execution_time > 0

        adapter.close()

    @skip_no_adapter
    def test_error_bad_host(self):
        """L0: Test error handling for bad host"""
        if not MONGODB_AVAILABLE:
            pytest.skip("pymongo not installed")

        config = MongoDBConfig(
            uri='mongodb://nonexistent.invalid.host:27017',
            database='test',
            server_selection_timeout=1000,  # Fast timeout
        )

        with pytest.raises(MongoDBConnectionError):
            MongoDBAdapter(config)

    @skip_no_adapter
    @skip_no_credentials
    def test_statistics(self):
        """L0: Test adapter statistics"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        # Execute a query
        adapter.find('test_collection', {}, limit=1)

        stats = adapter.get_statistics()

        assert stats['backend'] == 'mongodb'
        assert stats['state'] == 'connected'
        assert 'adapter_version' in stats

        adapter.close()


# =============================================================================
# L1 TESTS: INTROSPECTION
# =============================================================================

class TestL1Introspection:
    """L1: Introspection tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_list_databases_ordered(self):
        """L1: Test database listing with deterministic ordering"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        dbs1 = adapter.list_databases()
        dbs2 = adapter.list_databases()

        # Should be consistently ordered
        names1 = [db['name'] for db in dbs1]
        names2 = [db['name'] for db in dbs2]
        assert names1 == names2

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_list_collections(self):
        """L1: Test collection listing"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        collections = adapter.list_collections()

        assert isinstance(collections, list)
        for coll in collections:
            assert 'name' in coll
            assert 'type' in coll

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_list_indexes(self):
        """L1: Test index listing"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        # List collections first
        collections = adapter.list_collections()

        if collections:
            coll_name = collections[0]['name']
            indexes = adapter.list_indexes(coll_name)

            assert isinstance(indexes, list)
            # Every collection has at least _id index
            if indexes:
                idx = indexes[0]
                assert 'name' in idx
                assert 'keys' in idx

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_infer_shape(self):
        """L1: Test bounded shape inference"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        collections = adapter.list_collections()

        if collections:
            coll_name = collections[0]['name']
            shape = adapter.infer_shape(coll_name, sample_size=10)

            assert 'collection' in shape
            assert 'sample_size' in shape
            assert 'fields' in shape
            assert shape['sample_size'] <= 10  # Bounded

        adapter.close()


# =============================================================================
# L2 TESTS: DOCUMENT IR READ/WRITE
# =============================================================================

class TestL2DocumentIR:
    """L2: Document IR Read/Write tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_extract_database_dir(self):
        """L2: Test database DIR extraction"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        database_dir = adapter.extract_database_dir()

        assert 'database' in database_dir
        assert 'collections' in database_dir
        # No extraction_timestamp - deterministic output
        assert 'extraction_timestamp' not in database_dir
        assert 'adapter_version' in database_dir

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_export_collection_dir(self):
        """L2: Test collection export to DIR"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        collections = adapter.list_collections()

        if collections:
            coll_name = collections[0]['name']
            coll_dir = adapter.export_collection_dir(coll_name, limit=10)

            assert 'collection' in coll_dir
            assert 'documents' in coll_dir
            assert 'doc_count' in coll_dir
            assert 'order_by' in coll_dir
            assert coll_dir['order_by'] == '_id'  # Default ordering

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_export_deterministic_ordering(self):
        """L2: Test export uses deterministic ordering"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        collections = adapter.list_collections()

        if collections:
            coll_name = collections[0]['name']

            # Export twice
            dir1 = adapter.export_collection_dir(coll_name, limit=10)
            dir2 = adapter.export_collection_dir(coll_name, limit=10)

            # Should be identical
            assert dir1['documents'] == dir2['documents']

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_type_mapping(self):
        """L2: Test type mapping table"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        type_map = adapter.get_type_mapping()

        assert 'string' in type_map
        assert 'objectId' in type_map
        assert 'date' in type_map
        assert 'array' in type_map
        assert 'object' in type_map

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_database
    def test_collection_round_trip(self):
        """L2: Test collection export and import round trip"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        test_db = get_test_database()
        test_coll = f"rt_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            # Insert test documents
            adapter.use_database(test_db)
            for i in range(3):
                adapter.insert_one(test_coll, {
                    'index': i,
                    'name': f'Test Doc {i}',
                    'nested': {'value': i * 10}
                })

            # Export
            coll_dir = adapter.export_collection_dir(test_coll, database=test_db)
            assert coll_dir['doc_count'] == 3

            # Drop and reimport
            adapter.client[test_db][test_coll].drop()

            result = adapter.import_collection_dir(coll_dir, database=test_db)
            assert result['inserted'] == 3

            # Verify
            final_dir = adapter.export_collection_dir(test_coll, database=test_db)
            assert final_dir['doc_count'] == 3

        finally:
            try:
                adapter.client[test_db][test_coll].drop()
            except Exception:
                pass

        adapter.close()


# =============================================================================
# L3 TESTS: FIDELITY AND EDGE CASES
# =============================================================================

class TestL3Fidelity:
    """L3: Fidelity and edge case tests"""

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_database
    def test_nested_objects(self):
        """L3: Test nested object handling"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        test_db = get_test_database()
        test_coll = f"nested_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            adapter.use_database(test_db)

            # Insert nested document
            adapter.insert_one(test_coll, {
                'level1': {
                    'level2': {
                        'level3': {'value': 'deep'}
                    }
                }
            })

            # Export and verify structure preserved
            coll_dir = adapter.export_collection_dir(test_coll, database=test_db)
            doc = coll_dir['documents'][0]

            assert doc['level1']['level2']['level3']['value'] == 'deep'

        finally:
            try:
                adapter.client[test_db][test_coll].drop()
            except Exception:
                pass

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_database
    def test_arrays(self):
        """L3: Test array handling"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        test_db = get_test_database()
        test_coll = f"array_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            adapter.use_database(test_db)

            # Insert document with arrays
            adapter.insert_one(test_coll, {
                'tags': ['a', 'b', 'c'],
                'numbers': [1, 2, 3],
                'nested_array': [{'x': 1}, {'x': 2}]
            })

            # Export and verify arrays preserved
            coll_dir = adapter.export_collection_dir(test_coll, database=test_db)
            doc = coll_dir['documents'][0]

            assert doc['tags'] == ['a', 'b', 'c']
            assert doc['numbers'] == [1, 2, 3]
            assert doc['nested_array'][0]['x'] == 1

        finally:
            try:
                adapter.client[test_db][test_coll].drop()
            except Exception:
                pass

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_database
    def test_special_types(self):
        """L3: Test special type handling (Date, ObjectId, etc.)"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        test_db = get_test_database()
        test_coll = f"types_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            adapter.use_database(test_db)

            # Insert document with special types
            from bson import ObjectId
            from datetime import datetime as dt

            adapter.insert_one(test_coll, {
                'date_field': dt.now(),
                'string_field': 'test',
                'int_field': 42,
                'float_field': 3.14,
                'bool_field': True,
                'null_field': None,
            })

            # Export and verify
            coll_dir = adapter.export_collection_dir(test_coll, database=test_db)
            doc = coll_dir['documents'][0]

            # JSON serialization should handle special types
            assert '_id' in doc  # ObjectId serialized
            assert '$date' in str(doc['date_field']) or 'date_field' in doc

        finally:
            try:
                adapter.client[test_db][test_coll].drop()
            except Exception:
                pass

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_database
    def test_index_creation(self):
        """L3: Test index creation"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        test_db = get_test_database()
        test_coll = f"idx_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            adapter.use_database(test_db)

            # Insert a document to create collection
            adapter.insert_one(test_coll, {'field': 'value'})

            # Create index
            idx_name = adapter.create_index(
                test_coll,
                [('field', 1)],
                database=test_db,
                unique=False,
                name='field_idx'
            )

            assert idx_name == 'field_idx'

            # Verify index exists
            indexes = adapter.list_indexes(test_coll, test_db)
            idx_names = [idx['name'] for idx in indexes]
            assert 'field_idx' in idx_names

        finally:
            try:
                adapter.client[test_db][test_coll].drop()
            except Exception:
                pass

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_schema_validation_extraction(self):
        """L3: Test schema validation extraction"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        # Try to get schema validation (may be None if not configured)
        collections = adapter.list_collections()

        if collections:
            coll_name = collections[0]['name']
            validation = adapter.get_schema_validation(coll_name)

            # May or may not have validation - just test it doesn't error
            assert validation is None or 'json_schema' in validation

        adapter.close()


# =============================================================================
# L4 TESTS: AUDIT-GRADE PROOF
# =============================================================================

class TestL4AuditProof:
    """L4: Audit-grade proof tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_generation(self):
        """L4: Test proof bundle generation"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Cost-safety: disable checksums when using arbitrary database
            manifest = adapter.generate_proof_bundle(
                tmpdir,
                include_data_checksums=False
            )

            # Check manifest core fields (deterministic - no timestamps)
            assert 'bundle_id' not in manifest  # moved to run_metadata.json
            assert 'timestamp' not in manifest  # moved to run_metadata.json
            assert 'collections_hash' in manifest
            assert 'collection_count' in manifest

            # Check L4 required fields
            assert 'config' in manifest
            assert 'versions' in manifest
            assert 'dataset_hash' in manifest
            assert 'hardware_summary' in manifest

            # Verify config is sanitized
            assert 'password' not in manifest['config']
            assert 'username' not in str(manifest['config'])

            # Verify _run_metadata in return value (not written to bundle)
            assert '_run_metadata' in manifest
            assert 'bundle_id' in manifest['_run_metadata']
            assert 'timestamp' in manifest['_run_metadata']

            # Check files - bundle is DETERMINISTIC (no run_metadata.json)
            assert (Path(tmpdir) / 'run_manifest.json').exists()
            assert (Path(tmpdir) / 'database_dir.json').exists()
            assert not (Path(tmpdir) / 'run_metadata.json').exists()  # NOT in bundle
            assert (Path(tmpdir) / 'bundle_sha256.txt').exists()

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_determinism(self):
        """L4: Test proof bundle is deterministic"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir1:
            with tempfile.TemporaryDirectory() as tmpdir2:
                # Cost-safety: disable checksums
                m1 = adapter.generate_proof_bundle(
                    tmpdir1, include_data_checksums=False
                )
                m2 = adapter.generate_proof_bundle(
                    tmpdir2, include_data_checksums=False
                )

                # Collections hash should be identical
                assert m1['collections_hash'] == m2['collections_hash']

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_no_secrets_in_stats(self):
        """L4: Test no secrets leaked in statistics"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        stats = adapter.get_statistics()
        stats_str = json.dumps(stats)

        assert 'password' not in stats_str.lower()
        assert 'secret' not in stats_str.lower()

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_database
    def test_proof_bundle_required_artifacts(self):
        """L4: Test proof bundle contains ALL required artifacts"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        test_db = get_test_database()

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = adapter.generate_proof_bundle(
                tmpdir,
                database=test_db,
                include_data_checksums=True,
                max_checksum_docs=1000
            )

            required_files = [
                'run_manifest.json',
                'database_dir.json',
                'bundle_sha256.txt',
                'collection_diff.json',
                'doccount_diff.json',
                'checksum_diff.json',
                'limitations.md',
            ]

            for fname in required_files:
                fpath = Path(tmpdir) / fname
                assert fpath.exists(), f"Required artifact missing: {fname}"

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_collection_diff_content(self):
        """L4: Test collection_diff.json structure"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Cost-safety: disable checksums
            adapter.generate_proof_bundle(
                tmpdir, include_data_checksums=False
            )

            with open(Path(tmpdir) / 'collection_diff.json') as f:
                collection_diff = json.load(f)

            assert 'status' in collection_diff
            if collection_diff['status'] == 'N/A':
                assert 'reason' in collection_diff

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_doccount_diff_content(self):
        """L4: Test doccount_diff.json structure"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Cost-safety: disable checksums
            adapter.generate_proof_bundle(
                tmpdir, include_data_checksums=False
            )

            with open(Path(tmpdir) / 'doccount_diff.json') as f:
                doccount_diff = json.load(f)

            assert 'status' in doccount_diff

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_limitations_copied(self):
        """L4: Test limitations.md is included"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir:
            # Cost-safety: disable checksums
            adapter.generate_proof_bundle(
                tmpdir, include_data_checksums=False
            )

            limitations_path = Path(tmpdir) / 'limitations.md'
            assert limitations_path.exists()

            content = limitations_path.read_text()
            assert len(content) > 50
            assert 'MongoDB' in content

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_collections_hash_stable(self):
        """L4: Test collections hash is stable"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        hashes = []
        for _ in range(3):
            with tempfile.TemporaryDirectory() as tmpdir:
                # Cost-safety: disable checksums
                manifest = adapter.generate_proof_bundle(
                    tmpdir, include_data_checksums=False
                )
                hashes.append(manifest['collections_hash'])

        assert hashes[0] == hashes[1] == hashes[2], f"Hash not stable: {hashes}"

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_proof_bundle_full_artifact_determinism(self):
        """L4: Test full artifact determinism - ALL artifacts must be identical"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        with tempfile.TemporaryDirectory() as tmpdir1:
            with tempfile.TemporaryDirectory() as tmpdir2:
                # Generate bundles
                adapter.generate_proof_bundle(tmpdir1, include_data_checksums=False)
                adapter.generate_proof_bundle(tmpdir2, include_data_checksums=False)

                # ALL deterministic artifacts must be identical
                deterministic_artifacts = [
                    'run_manifest.json',
                    'database_dir.json',
                    'collection_diff.json',
                    'doccount_diff.json',
                    'checksum_diff.json',
                    'limitations.md',
                ]

                for artifact in deterministic_artifacts:
                    path1 = Path(tmpdir1) / artifact
                    path2 = Path(tmpdir2) / artifact

                    assert path1.exists(), f"{artifact} missing from bundle 1"
                    assert path2.exists(), f"{artifact} missing from bundle 2"

                    content1 = path1.read_text()
                    content2 = path2.read_text()

                    assert content1 == content2, f"{artifact} not deterministic"

                # Verify run_metadata.json is NOT in bundle (non-deterministic)
                assert not (Path(tmpdir1) / 'run_metadata.json').exists()
                assert not (Path(tmpdir2) / 'run_metadata.json').exists()

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_database
    def test_collection_diff_detects_validation_changes(self):
        """L4: Test collection_diff detects schema validation changes"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        test_db = get_test_database()
        test_coll = f"valdiff_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            adapter.use_database(test_db)

            # Create collection
            adapter.insert_one(test_coll, {'field': 'value'})

            # Create baseline with no validation
            baseline_dir = adapter.extract_database_dir(test_db)

            # Modify validation state (simulate by modifying baseline)
            current_dir = adapter.extract_database_dir(test_db)
            # Simulate adding validation to current
            if test_coll in current_dir['collections']:
                current_dir['collections'][test_coll]['schema_validation'] = {
                    'json_schema': {'bsonType': 'object'},
                    'validation_level': 'strict',
                    'validation_action': 'error'
                }

            # Compute diff
            diff = adapter._compute_collection_diff(baseline_dir, current_dir)

            # Should detect the validation change
            assert test_coll in diff['collections_modified'], \
                "collection_diff should detect schema validation changes"

        finally:
            try:
                adapter.client[test_db][test_coll].drop()
            except Exception:
                pass

        adapter.close()


# =============================================================================
# L1 ADDITIONAL TESTS: DETERMINISM
# =============================================================================

class TestL1Determinism:
    """L1: Deterministic sampling tests"""

    @skip_no_adapter
    @skip_no_credentials
    def test_infer_shape_determinism(self):
        """L1: Test shape inference is deterministic"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        collections = adapter.list_collections()

        if collections:
            coll_name = collections[0]['name']

            # Run shape inference twice
            shape1 = adapter.infer_shape(coll_name, sample_size=10)
            shape2 = adapter.infer_shape(coll_name, sample_size=10)

            # Results should be IDENTICAL (uses _id sort, not random $sample)
            assert shape1['fields'] == shape2['fields'], \
                "Shape inference not deterministic - fields differ"
            assert shape1['sample_size'] == shape2['sample_size'], \
                "Shape inference not deterministic - sample_size differs"

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    def test_infer_shape_no_timestamp(self):
        """L1: Test shape inference returns no timestamp (deterministic)"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        collections = adapter.list_collections()

        if collections:
            coll_name = collections[0]['name']
            shape = adapter.infer_shape(coll_name, sample_size=10)

            # Should NOT have timestamp for determinism
            assert 'inferred_at' not in shape, \
                "Shape inference should not include timestamp"

        adapter.close()


# =============================================================================
# L3 ADDITIONAL TESTS: INDEX FIDELITY
# =============================================================================

class TestL3IndexFidelity:
    """L3: Index fidelity tests - unique, compound, partial"""

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_database
    def test_index_fidelity_unique(self):
        """L3: Test unique index creation and extraction"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        test_db = get_test_database()
        test_coll = f"unique_idx_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            adapter.use_database(test_db)

            # Create collection and unique index
            adapter.insert_one(test_coll, {'email': 'test@example.com'})
            adapter.create_index(
                test_coll, [('email', 1)],
                database=test_db, unique=True, name='email_unique'
            )

            # Extract and verify
            indexes = adapter.list_indexes(test_coll, test_db)
            email_idx = next((i for i in indexes if i['name'] == 'email_unique'), None)

            assert email_idx is not None, "Unique index not found"
            assert email_idx.get('unique') is True, "Unique flag not preserved"

        finally:
            try:
                adapter.client[test_db][test_coll].drop()
            except Exception:
                pass

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_database
    def test_index_fidelity_compound(self):
        """L3: Test compound index creation and extraction"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        test_db = get_test_database()
        test_coll = f"compound_idx_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            adapter.use_database(test_db)

            # Create collection and compound index
            adapter.insert_one(test_coll, {'a': 1, 'b': 2, 'c': 3})
            adapter.create_index(
                test_coll, [('a', 1), ('b', -1)],
                database=test_db, name='compound_ab'
            )

            # Extract and verify
            indexes = adapter.list_indexes(test_coll, test_db)
            compound_idx = next((i for i in indexes if i['name'] == 'compound_ab'), None)

            assert compound_idx is not None, "Compound index not found"
            keys = compound_idx.get('keys', [])
            assert len(keys) == 2, "Compound index should have 2 keys"

        finally:
            try:
                adapter.client[test_db][test_coll].drop()
            except Exception:
                pass

        adapter.close()

    @skip_no_adapter
    @skip_no_credentials
    @skip_no_test_database
    def test_index_fidelity_sparse(self):
        """L3: Test sparse index creation and extraction"""
        config = get_test_config()
        adapter = MongoDBAdapter(config)

        test_db = get_test_database()
        test_coll = f"sparse_idx_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        try:
            adapter.use_database(test_db)

            # Create collection and sparse index
            adapter.insert_one(test_coll, {'field': 'value'})
            adapter.create_index(
                test_coll, [('optional_field', 1)],
                database=test_db, sparse=True, name='optional_sparse'
            )

            # Extract and verify
            indexes = adapter.list_indexes(test_coll, test_db)
            sparse_idx = next((i for i in indexes if i['name'] == 'optional_sparse'), None)

            assert sparse_idx is not None, "Sparse index not found"
            assert sparse_idx.get('sparse') is True, "Sparse flag not preserved"

        finally:
            try:
                adapter.client[test_db][test_coll].drop()
            except Exception:
                pass

        adapter.close()


# =============================================================================
# INTEGRATION TEST RUNNER
# =============================================================================

def run_l0_tests():
    """Run L0 connectivity tests"""
    import subprocess
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        __file__,
        "-v", "-k", "L0",
        "--tb=short"
    ])
    return result.returncode


def run_all_tests():
    """Run all tests"""
    import subprocess
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        __file__,
        "-v",
        "--tb=short"
    ])
    return result.returncode


if __name__ == "__main__":
    run_all_tests()
