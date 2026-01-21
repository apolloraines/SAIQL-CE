#!/usr/bin/env python3
"""
SAIQL Redis Adapter Test Suite - L0-L4

Tests for Key Value IR (KVIR) lane functionality.

Environment variables:
    REDIS_HOST: Redis host (default: localhost)
    REDIS_PORT: Redis port (default: 6379)
    REDIS_PASSWORD: Redis password (optional)
    REDIS_TEST_DATABASE: Database number for tests (default: 15)

Run with:
    pytest tests/database/test_redis_adapter.py -v
    REDIS_HOST=localhost pytest tests/database/test_redis_adapter.py -v
"""

import pytest
import os
import json
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, MagicMock

# Import adapter
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from extensions.plugins.redis_adapter import (
    RedisAdapter,
    RedisConfig,
    RedisResult,
    KeyValueIR,
    RedisError,
    RedisConnectionException,
    RedisAuthException,
    RedisOperationError,
    REDIS_AVAILABLE,
    ConnectionState,
    RedisDataType,
)


# Test configuration from environment
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD')
REDIS_TEST_DB = int(os.environ.get('REDIS_TEST_DATABASE', '15'))


def get_test_config() -> RedisConfig:
    """Get test configuration"""
    return RedisConfig(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        database=REDIS_TEST_DB,
    )


def redis_available() -> bool:
    """Check if Redis is available for testing"""
    if not REDIS_AVAILABLE:
        return False
    try:
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()
        adapter.close()
        return True
    except Exception:
        return False


# Skip decorator for tests requiring live Redis
requires_redis = pytest.mark.skipif(
    not redis_available(),
    reason="Redis not available or not configured"
)


# =============================================================================
# L0: Connectivity Tests
# =============================================================================

class TestL0Connectivity:
    """L0 Connectivity test suite"""

    def test_config_creation(self):
        """Test configuration creation"""
        config = RedisConfig(
            host='testhost',
            port=6380,
            database=1,
        )
        assert config.host == 'testhost'
        assert config.port == 6380
        assert config.database == 1

    def test_config_with_auth(self):
        """Test configuration with authentication"""
        config = RedisConfig(
            host='localhost',
            password='secret',
            username='testuser',
        )
        assert config.password == 'secret'
        assert config.username == 'testuser'

    def test_config_to_uri(self):
        """Test URI generation"""
        config = RedisConfig(
            host='localhost',
            port=6379,
            database=0,
        )
        uri = config.to_uri()
        assert uri == 'redis://localhost:6379/0'

        # With password
        config.password = 'secret'
        uri = config.to_uri()
        assert 'secret' in uri

        # With SSL
        config.ssl = True
        uri = config.to_uri()
        assert uri.startswith('rediss://')

    def test_driver_not_installed_error(self):
        """Test graceful error when redis-py not installed"""
        # Test that adapter raises error when REDIS_AVAILABLE is False
        with patch('extensions.plugins.redis_adapter.REDIS_AVAILABLE', False):
            config = RedisConfig(host='localhost', port=6379)
            with pytest.raises(RedisError) as exc_info:
                RedisAdapter(config)
            assert 'not installed' in str(exc_info.value).lower()

    @requires_redis
    def test_connection_success(self):
        """Test successful connection"""
        config = get_test_config()
        adapter = RedisAdapter(config)

        adapter._connect()
        assert adapter.is_connected()
        assert adapter.state == ConnectionState.CONNECTED

        adapter.close()
        assert adapter.state == ConnectionState.DISCONNECTED

    @requires_redis
    def test_ping_command(self):
        """Test PING command"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        result = adapter.ping()
        assert result is True

        adapter.close()

    def test_error_bad_host(self):
        """Test connection error with bad host"""
        config = RedisConfig(
            host='nonexistent.invalid.host',
            port=6379,
            socket_connect_timeout=1.0,
        )
        adapter = RedisAdapter(config)

        with pytest.raises(RedisConnectionException):
            adapter._connect()

    @requires_redis
    def test_statistics(self):
        """Test statistics retrieval"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        stats = adapter.get_statistics()
        assert 'adapter_version' in stats
        assert 'state' in stats
        assert 'config' in stats
        assert 'password' not in str(stats)  # No secrets

        adapter.close()


# =============================================================================
# L1: Introspection Tests
# =============================================================================

class TestL1Introspection:
    """L1 Introspection test suite"""

    @requires_redis
    def test_get_info(self):
        """Test INFO command"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        info = adapter.get_info()
        assert 'redis_version' in info

        # Test specific section
        server_info = adapter.get_info('server')
        assert 'redis_version' in server_info

        adapter.close()

    @requires_redis
    def test_get_server_fingerprint(self):
        """Test server fingerprint for proof bundles"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        fingerprint = adapter.get_server_fingerprint()
        assert 'redis_version' in fingerprint
        assert 'redis_mode' in fingerprint
        # No secrets in fingerprint
        assert 'password' not in str(fingerprint).lower()

        adapter.close()

    @requires_redis
    def test_bounded_key_scan(self):
        """Test bounded SCAN with deterministic ordering"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        # Clear test database
        adapter._client.flushdb()

        # Create test keys
        for i in range(10):
            adapter._client.set(f'test:key:{i}'.encode(), f'value{i}'.encode())

        # Scan with bound
        keys = adapter.scan_keys(pattern='test:key:*', max_keys=5)
        assert len(keys) <= 5
        # Verify sorted order
        assert keys == sorted(keys)

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_get_key_info(self):
        """Test key metadata retrieval"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Create test key with TTL
        adapter._client.set(b'test:info', b'value', ex=3600)

        info = adapter.get_key_info('test:info')
        assert info['exists'] is True
        assert info['type'] == 'string'
        assert info['ttl_seconds'] is not None
        assert info['ttl_seconds'] > 0

        # Test non-existent key
        info = adapter.get_key_info('nonexistent:key')
        assert info['exists'] is False

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_list_key_types(self):
        """Test type discovery for multiple keys"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Create keys of different types
        adapter._client.set(b'test:string', b'value')
        adapter._client.hset(b'test:hash', b'field', b'value')
        adapter._client.lpush(b'test:list', b'item')
        adapter._client.sadd(b'test:set', b'member')
        adapter._client.zadd(b'test:zset', {b'member': 1.0})

        types = adapter.list_key_types(pattern='test:*')
        assert types['test:string'] == 'string'
        assert types['test:hash'] == 'hash'
        assert types['test:list'] == 'list'
        assert types['test:set'] == 'set'
        assert types['test:zset'] == 'zset'

        adapter._client.flushdb()
        adapter.close()


# =============================================================================
# L2: KVIR Export/Import Tests
# =============================================================================

class TestL2KVIR:
    """L2 Key Value IR test suite"""

    @requires_redis
    def test_export_string_key(self):
        """Test string key export"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()
        adapter._client.set(b'test:string', b'hello world')

        kvir = adapter.export_key('test:string')
        assert kvir.key == 'test:string'
        assert kvir.type == 'string'
        assert kvir.value['encoding'] == 'base64'
        assert kvir.key_hash  # Has hash

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_export_hash_key(self):
        """Test hash key export with sorted fields"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()
        adapter._client.hset(b'test:hash', mapping={b'z_field': b'z', b'a_field': b'a', b'm_field': b'm'})

        kvir = adapter.export_key('test:hash')
        assert kvir.type == 'hash'
        fields = list(kvir.value['fields'].keys())
        # Verify sorted order
        assert fields == sorted(fields)

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_export_list_key(self):
        """Test list key export with order preserved"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()
        adapter._client.rpush(b'test:list', b'first', b'second', b'third')

        kvir = adapter.export_key('test:list')
        assert kvir.type == 'list'
        assert len(kvir.value['elements']) == 3

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_export_set_key(self):
        """Test set key export with sorted members"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()
        adapter._client.sadd(b'test:set', b'zebra', b'apple', b'mango')

        kvir = adapter.export_key('test:set')
        assert kvir.type == 'set'
        members = kvir.value['members']
        # Verify sorted order
        assert members == sorted(members)

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_export_zset_key(self):
        """Test sorted set key export with scores"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()
        adapter._client.zadd(b'test:zset', {b'low': 1.0, b'mid': 5.0, b'high': 10.0})

        kvir = adapter.export_key('test:zset')
        assert kvir.type == 'zset'
        assert len(kvir.value['members']) == 3
        # Verify scores preserved
        scores = [m['score'] for m in kvir.value['members']]
        assert 1.0 in scores
        assert 5.0 in scores
        assert 10.0 in scores

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_export_keys_deterministic_ordering(self):
        """Test multi-key export has deterministic ordering"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()
        for i in range(5):
            adapter._client.set(f'key:{i}'.encode(), f'value{i}'.encode())

        export1 = adapter.export_keys(pattern='key:*')
        export2 = adapter.export_keys(pattern='key:*')

        # Exports should be identical
        assert json.dumps(export1, sort_keys=True) == json.dumps(export2, sort_keys=True)

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_import_string_key(self):
        """Test string key import"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Create KVIR manually
        import base64
        kvir = KeyValueIR(
            key='imported:string',
            key_hash='abc123',
            type='string',
            encoding='embstr',
            ttl_seconds=None,
            value={'encoding': 'base64', 'data': base64.b64encode(b'imported value').decode()},
        )

        adapter.import_key(kvir)

        # Verify import
        value = adapter._client.get(b'imported:string')
        assert value == b'imported value'

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_import_with_ttl_replay(self):
        """Test TTL replay on import"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        import base64
        kvir = KeyValueIR(
            key='imported:ttl',
            key_hash='abc123',
            type='string',
            encoding='embstr',
            ttl_seconds=3600,
            value={'encoding': 'base64', 'data': base64.b64encode(b'value').decode()},
        )

        adapter.import_key(kvir, replay_ttl=True)

        # Verify TTL was set
        ttl = adapter._client.ttl(b'imported:ttl')
        assert ttl > 0
        assert ttl <= 3600

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_import_conflict_policy_skip(self):
        """Test skip conflict policy"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Create existing key
        adapter._client.set(b'existing:key', b'original')

        import base64
        kvir = KeyValueIR(
            key='existing:key',
            key_hash='abc123',
            type='string',
            encoding='embstr',
            ttl_seconds=None,
            value={'encoding': 'base64', 'data': base64.b64encode(b'new').decode()},
        )

        result = adapter.import_key(kvir, conflict_policy='skip')
        assert result is False

        # Original value unchanged
        value = adapter._client.get(b'existing:key')
        assert value == b'original'

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_import_conflict_policy_overwrite(self):
        """Test overwrite conflict policy"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Create existing key
        adapter._client.set(b'existing:key', b'original')

        import base64
        kvir = KeyValueIR(
            key='existing:key',
            key_hash='abc123',
            type='string',
            encoding='embstr',
            ttl_seconds=None,
            value={'encoding': 'base64', 'data': base64.b64encode(b'new').decode()},
        )

        result = adapter.import_key(kvir, conflict_policy='overwrite')
        assert result is True

        # Value overwritten
        value = adapter._client.get(b'existing:key')
        assert value == b'new'

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_round_trip_all_types(self):
        """Test full round trip for all supported types"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Create keys of all types
        adapter._client.set(b'rt:string', b'value')
        adapter._client.hset(b'rt:hash', mapping={b'f1': b'v1', b'f2': b'v2'})
        adapter._client.rpush(b'rt:list', b'a', b'b', b'c')
        adapter._client.sadd(b'rt:set', b'x', b'y', b'z')
        adapter._client.zadd(b'rt:zset', {b'm1': 1.0, b'm2': 2.0})

        # Export
        export = adapter.export_keys(pattern='rt:*')

        # Clear and reimport
        adapter._client.flushdb()
        result = adapter.import_keys(export)

        assert result['keys_imported'] == 5
        assert result['errors'] == []

        # Verify all types restored
        assert adapter._client.get(b'rt:string') == b'value'
        assert adapter._client.hgetall(b'rt:hash') == {b'f1': b'v1', b'f2': b'v2'}
        assert adapter._client.lrange(b'rt:list', 0, -1) == [b'a', b'b', b'c']
        assert adapter._client.smembers(b'rt:set') == {b'x', b'y', b'z'}
        assert adapter._client.zrange(b'rt:zset', 0, -1, withscores=True) == [(b'm1', 1.0), (b'm2', 2.0)]

        adapter._client.flushdb()
        adapter.close()


# =============================================================================
# L3: Fidelity Tests
# =============================================================================

class TestL3Fidelity:
    """L3 Fidelity test suite"""

    @requires_redis
    def test_binary_safe_strings(self):
        """Test binary data fidelity"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Binary data with null bytes
        binary_data = b'\x00\x01\x02\xff\xfe\xfd'
        adapter._client.set(b'binary:key', binary_data)

        kvir = adapter.export_key('binary:key')

        # Clear and reimport
        adapter._client.flushdb()
        adapter.import_key(kvir)

        # Verify fidelity
        restored = adapter._client.get(b'binary:key')
        assert restored == binary_data

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_hash_field_ordering(self):
        """Test hash fields are sorted in KVIR"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Add fields in random order
        adapter._client.hset(b'hash:order', mapping={
            b'zebra': b'1',
            b'apple': b'2',
            b'mango': b'3',
            b'banana': b'4',
        })

        kvir = adapter.export_key('hash:order')
        fields = list(kvir.value['fields'].keys())

        # Must be sorted
        assert fields == ['apple', 'banana', 'mango', 'zebra']

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_list_order_preserved(self):
        """Test list order is preserved"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        adapter._client.rpush(b'list:order', b'first', b'second', b'third', b'fourth')

        kvir = adapter.export_key('list:order')

        # Clear and reimport
        adapter._client.flushdb()
        adapter.import_key(kvir)

        # Verify order
        items = adapter._client.lrange(b'list:order', 0, -1)
        assert items == [b'first', b'second', b'third', b'fourth']

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_set_canonicalized_in_kvir(self):
        """Test set members are sorted in KVIR (no order in Redis)"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        adapter._client.sadd(b'set:canon', b'z', b'a', b'm', b'b')

        kvir = adapter.export_key('set:canon')
        members = kvir.value['members']

        # Must be sorted in KVIR
        assert members == sorted(members)

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_zset_score_fidelity(self):
        """Test sorted set score precision"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Use precise float scores
        adapter._client.zadd(b'zset:scores', {
            b'a': 1.5,
            b'b': 2.7182818,
            b'c': 3.14159265,
        })

        kvir = adapter.export_key('zset:scores')

        # Clear and reimport
        adapter._client.flushdb()
        adapter.import_key(kvir)

        # Verify scores
        members = adapter._client.zrange(b'zset:scores', 0, -1, withscores=True)
        scores = {m: s for m, s in members}
        assert abs(scores[b'a'] - 1.5) < 0.0001
        assert abs(scores[b'b'] - 2.7182818) < 0.0001
        assert abs(scores[b'c'] - 3.14159265) < 0.0001

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_ttl_fidelity(self):
        """Test TTL preservation"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        adapter._client.set(b'ttl:key', b'value', ex=7200)

        kvir = adapter.export_key('ttl:key')
        assert kvir.ttl_seconds is not None
        assert kvir.ttl_seconds > 0

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_encoding_capture(self):
        """Test Redis internal encoding is captured"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Small string uses embstr encoding
        adapter._client.set(b'enc:small', b'small')

        encoding = adapter.get_encoding('enc:small')
        assert encoding in ['embstr', 'raw', 'int']

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_stream_export(self):
        """Test stream export with entry IDs and fields"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Create stream with entries
        adapter._client.xadd(b'stream:events', {b'action': b'login', b'user': b'alice'})
        adapter._client.xadd(b'stream:events', {b'action': b'click', b'user': b'alice'})
        adapter._client.xadd(b'stream:events', {b'action': b'logout', b'user': b'alice'})

        kvir = adapter.export_key('stream:events')
        assert kvir.type == 'stream'
        assert len(kvir.value['entries']) == 3

        # Verify entry structure
        entry = kvir.value['entries'][0]
        assert 'id' in entry
        assert 'fields' in entry
        assert 'action' in entry['fields']

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_stream_import_auto_id(self):
        """Test stream import uses auto-generated IDs (documented limitation)"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Create stream
        adapter._client.xadd(b'stream:test', {b'data': b'value1'})
        adapter._client.xadd(b'stream:test', {b'data': b'value2'})

        kvir = adapter.export_key('stream:test')
        original_ids = [e['id'] for e in kvir.value['entries']]

        # Clear and reimport
        adapter._client.flushdb()
        adapter.import_key(kvir)

        # Verify entries exist (IDs will be different - documented limitation)
        entries = adapter._client.xrange(b'stream:test')
        assert len(entries) == 2

        # Verify field content preserved
        for entry_id, fields in entries:
            assert b'data' in fields

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_stream_field_fidelity(self):
        """Test stream field values are preserved"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Create stream with binary field values
        adapter._client.xadd(b'stream:binary', {b'bin': b'\x00\x01\x02', b'text': b'hello'})

        kvir = adapter.export_key('stream:binary')

        adapter._client.flushdb()
        adapter.import_key(kvir)

        # Verify field content
        entries = adapter._client.xrange(b'stream:binary')
        assert len(entries) == 1
        _, fields = entries[0]
        assert fields[b'text'] == b'hello'
        # Binary field preserved via base64
        assert fields[b'bin'] == b'\x00\x01\x02'

        adapter._client.flushdb()
        adapter.close()

    def test_unsupported_type_fails_fast(self):
        """Test unsupported/module types raise deterministic error"""
        # This tests the _export_value path for unknown types
        # We can't easily create a module type without the module installed,
        # but we can test the error path is correct
        config = get_test_config()
        adapter = RedisAdapter(config)

        # Mock the _export_value to test the error path
        with pytest.raises(RedisOperationError) as exc_info:
            adapter._export_value(b'test', 'ReJSON-RL')

        assert 'unsupported' in str(exc_info.value).lower()


# =============================================================================
# L4: Proof Bundle Tests
# =============================================================================

class TestL4AuditProof:
    """L4 Audit-grade proof test suite"""

    @requires_redis
    def test_proof_bundle_generation(self):
        """Test proof bundle is generated"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Create test data
        adapter._client.set(b'proof:key1', b'value1')
        adapter._client.set(b'proof:key2', b'value2')

        with tempfile.TemporaryDirectory() as tmpdir:
            result = adapter.generate_proof_bundle(
                output_dir=tmpdir,
                pattern='proof:*',
            )

            assert result['key_count'] == 2
            assert 'bundle_hash' in result
            assert 'keys_hash' in result
            assert 'dataset_hash' in result

            # Check artifacts exist
            bundle_path = Path(tmpdir)
            assert (bundle_path / 'run_manifest.json').exists()
            assert (bundle_path / 'kvir_export.json').exists()
            assert (bundle_path / 'keycount_diff.json').exists()
            assert (bundle_path / 'ttl_diff.json').exists()
            assert (bundle_path / 'checksum_diff.json').exists()
            assert (bundle_path / 'limitations.md').exists()
            assert (bundle_path / 'bundle_sha256.txt').exists()

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_proof_bundle_determinism(self):
        """Test identical runs produce identical bundles"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Create test data
        for i in range(5):
            adapter._client.set(f'det:key{i}'.encode(), f'value{i}'.encode())

        results = []
        for _ in range(3):
            with tempfile.TemporaryDirectory() as tmpdir:
                result = adapter.generate_proof_bundle(
                    output_dir=tmpdir,
                    pattern='det:*',
                )
                results.append(result)

        # All bundle hashes should be identical
        assert results[0]['bundle_hash'] == results[1]['bundle_hash']
        assert results[1]['bundle_hash'] == results[2]['bundle_hash']

        # All keys hashes should be identical
        assert results[0]['keys_hash'] == results[1]['keys_hash']

        # All dataset hashes should be identical
        assert results[0]['dataset_hash'] == results[1]['dataset_hash']

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_proof_bundle_full_artifact_determinism(self):
        """Test ALL deterministic artifacts are identical across runs"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Create varied test data
        adapter._client.set(b'art:string', b'value')
        adapter._client.hset(b'art:hash', mapping={b'f1': b'v1'})
        adapter._client.rpush(b'art:list', b'a', b'b')
        adapter._client.sadd(b'art:set', b'x', b'y')
        adapter._client.zadd(b'art:zset', {b'm': 1.0})

        # Generate bundles in separate directories
        artifacts_by_run = []
        tmpdir_base = tempfile.mkdtemp()
        try:
            for i in range(2):
                output_dir = Path(tmpdir_base) / f'run_{i}'
                adapter.generate_proof_bundle(
                    output_dir=str(output_dir),
                    pattern='art:*',
                )

                # Read all deterministic artifacts
                artifacts = {}
                for artifact_name in [
                    'run_manifest.json',
                    'kvir_export.json',
                    'keycount_diff.json',
                    'ttl_diff.json',
                    'checksum_diff.json',
                ]:
                    with open(output_dir / artifact_name) as f:
                        artifacts[artifact_name] = f.read()
                artifacts_by_run.append(artifacts)

            # All artifacts should be identical
            for artifact_name in artifacts_by_run[0].keys():
                assert artifacts_by_run[0][artifact_name] == artifacts_by_run[1][artifact_name], \
                    f"Artifact {artifact_name} differs between runs"

        finally:
            shutil.rmtree(tmpdir_base)

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_no_secrets_in_bundle(self):
        """Test no secrets in proof bundle"""
        # Use standard test config but verify password would not appear
        config = get_test_config()
        # Simulate having a password configured (even though test Redis may not require it)
        original_password = config.password
        config.password = 'supersecretpassword'

        adapter = RedisAdapter(config)
        # Connect without the fake password (test Redis has no auth)
        config.password = original_password
        adapter.config = config
        adapter._connect()

        adapter._client.flushdb()
        adapter._client.set(b'secret:test', b'value')

        # Now set password back to simulate config with secret
        adapter.config.password = 'supersecretpassword'

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter.generate_proof_bundle(
                output_dir=tmpdir,
                pattern='secret:*',
            )

            # Check no secrets in manifest
            with open(Path(tmpdir) / 'run_manifest.json') as f:
                manifest = json.load(f)

            manifest_str = json.dumps(manifest)
            assert 'supersecretpassword' not in manifest_str
            # Password field should not be in config
            assert 'password' not in manifest['config']

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_proof_bundle_required_artifacts(self):
        """Test all required artifacts are present"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()
        adapter._client.set(b'artifact:key', b'value')

        with tempfile.TemporaryDirectory() as tmpdir:
            adapter.generate_proof_bundle(
                output_dir=tmpdir,
                pattern='artifact:*',
            )

            bundle_path = Path(tmpdir)

            # Required per spec
            assert (bundle_path / 'run_manifest.json').exists()
            assert (bundle_path / 'keycount_diff.json').exists()
            assert (bundle_path / 'ttl_diff.json').exists()
            assert (bundle_path / 'checksum_diff.json').exists()
            assert (bundle_path / 'limitations.md').exists()

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_proof_bundle_dataset_hash_is_data_based(self):
        """Test dataset_hash changes with data"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()
        adapter._client.set(b'data:key', b'value1')

        with tempfile.TemporaryDirectory() as tmpdir1:
            result1 = adapter.generate_proof_bundle(
                output_dir=tmpdir1,
                pattern='data:*',
            )

        # Change data
        adapter._client.set(b'data:key', b'value2')

        with tempfile.TemporaryDirectory() as tmpdir2:
            result2 = adapter.generate_proof_bundle(
                output_dir=tmpdir2,
                pattern='data:*',
            )

        # Dataset hash should be different
        assert result1['dataset_hash'] != result2['dataset_hash']

        adapter._client.flushdb()
        adapter.close()

    def test_flushdb_blocked(self):
        """Test FLUSHDB is blocked by default"""
        config = get_test_config()
        adapter = RedisAdapter(config)

        with pytest.raises(RedisOperationError) as exc_info:
            adapter.flushdb()

        assert 'blocked' in str(exc_info.value).lower()


# =============================================================================
# L1 Determinism Tests
# =============================================================================

class TestL1Determinism:
    """L1 Determinism verification tests"""

    @requires_redis
    def test_scan_determinism(self):
        """Test SCAN returns deterministic sorted results"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Create keys in random order
        for key in ['z', 'a', 'm', 'b', 'y']:
            adapter._client.set(f'scan:{key}'.encode(), b'value')

        keys1 = adapter.scan_keys(pattern='scan:*')
        keys2 = adapter.scan_keys(pattern='scan:*')

        # Results should be identical and sorted
        assert keys1 == keys2
        assert keys1 == sorted(keys1)

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_key_info_determinism(self):
        """Test key info returns consistent results"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()
        adapter._client.set(b'info:key', b'value')

        info1 = adapter.get_key_info('info:key')
        info2 = adapter.get_key_info('info:key')

        assert info1['type'] == info2['type']
        assert info1['encoding'] == info2['encoding']

        adapter._client.flushdb()
        adapter.close()


# =============================================================================
# Safety Tests
# =============================================================================

class TestSafety:
    """Safety and bounded operation tests"""

    @requires_redis
    def test_bounded_scan_enforced(self):
        """Test scan respects max_keys bound"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()

        # Create more keys than limit
        for i in range(100):
            adapter._client.set(f'bound:{i}'.encode(), b'value')

        keys = adapter.scan_keys(pattern='bound:*', max_keys=10)
        assert len(keys) <= 10

        adapter._client.flushdb()
        adapter.close()

    @requires_redis
    def test_delete_explicit_only(self):
        """Test delete only works with explicit keys, not patterns"""
        config = get_test_config()
        adapter = RedisAdapter(config)
        adapter._connect()

        adapter._client.flushdb()
        adapter._client.set(b'del:key1', b'v1')
        adapter._client.set(b'del:key2', b'v2')

        # Explicit delete works
        count = adapter.delete('del:key1')
        assert count == 1

        # key2 still exists
        assert adapter._client.exists(b'del:key2')

        adapter._client.flushdb()
        adapter.close()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
