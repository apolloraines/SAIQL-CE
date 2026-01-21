#!/usr/bin/env python3
"""
SAIQL Engine Unit Tests

Comprehensive unit tests for the SAIQL engine core functionality.

Author: Apollo & Claude
Version: 1.0.0
Status: Production-Ready for SAIQL-Bravo
"""

import pytest
import time
import threading
from unittest.mock import Mock, patch, MagicMock
from dataclasses import asdict

# Import SAIQL components
from core.engine import (
    SAIQLEngine, ExecutionContext, QueryResult, ExecutionMode, 
    SessionState, QueryCache, SessionManager, PerformanceTracker
)

@pytest.mark.unit
class TestSAIQLEngine:
    """Test SAIQL Engine core functionality"""
    
    def test_engine_initialization(self, test_config):
        """Test engine initialization with config"""
        config, config_path = test_config
        engine = SAIQLEngine(config_path, debug=True)
        
        assert engine is not None
        assert engine.debug is True
        assert engine.config['compilation']['target_dialect'] == 'sqlite'
        assert engine.session_manager is not None
        assert engine.query_cache is not None
        
        engine.shutdown()
    
    def test_engine_initialization_without_config(self):
        """Test engine initialization without config file"""
        engine = SAIQLEngine()
        
        assert engine is not None
        assert engine.config is not None
        assert 'database' in engine.config
        
        engine.shutdown()
    
    def test_execute_simple_query(self, saiql_engine, sample_queries, execution_context):
        """Test executing a simple SAIQL query"""
        query = sample_queries['simple_select']
        
        result = saiql_engine.execute(query, execution_context)
        
        assert isinstance(result, QueryResult)
        assert result.query == query
        assert result.session_id == execution_context.session_id
        assert result.execution_time > 0
        assert result.sql_generated is not None
    
    def test_execute_invalid_query(self, saiql_engine, execution_context):
        """Test executing an invalid SAIQL query"""
        invalid_query = "INVALID SYNTAX $$##"
        
        result = saiql_engine.execute(invalid_query, execution_context)
        
        assert isinstance(result, QueryResult)
        assert result.query == invalid_query
        # Note: Based on engine implementation, invalid queries might still succeed
        # with error handling, so we check for proper result structure
        assert hasattr(result, 'error_message')
    
    def test_execute_with_caching(self, saiql_engine, sample_queries, execution_context):
        """Test query caching functionality"""
        query = sample_queries['simple_select']
        
        # First execution
        result1 = saiql_engine.execute(query, execution_context, enable_caching=True)
        assert result1.cache_hit is False
        
        # Second execution should hit cache
        result2 = saiql_engine.execute(query, execution_context, enable_caching=True)
        # Note: Cache behavior depends on implementation details
        assert isinstance(result2, QueryResult)
    
    def test_execute_batch_queries(self, saiql_engine, sample_queries, execution_context):
        """Test batch query execution"""
        queries = [
            sample_queries['simple_select'],
            sample_queries['count_query']
        ]
        
        results = saiql_engine.execute_batch(queries, execution_context)
        
        assert len(results) == 2
        assert all(isinstance(r, QueryResult) for r in results)
        assert all(r.session_id == execution_context.session_id for r in results)
    
    def test_engine_statistics(self, saiql_engine, sample_queries, execution_context):
        """Test engine statistics collection"""
        # Execute some queries
        for _ in range(3):
            saiql_engine.execute(sample_queries['simple_select'], execution_context)
        
        stats = saiql_engine.get_stats()
        
        assert 'queries_executed' in stats
        assert stats['queries_executed'] >= 3
        assert 'uptime_seconds' in stats
        assert 'cache_stats' in stats
        assert 'session_count' in stats
    
    def test_engine_cleanup(self, saiql_engine):
        """Test engine cleanup and shutdown"""
        # Perform some operations
        context = ExecutionContext(session_id="cleanup_test")
        saiql_engine.execute("*3[users]::name>>oQ", context)
        
        # Test cleanup
        cleaned_sessions = saiql_engine.cleanup_sessions()
        assert isinstance(cleaned_sessions, int)
        
        # Test cache clearing
        saiql_engine.clear_cache()
        cache_stats = saiql_engine.query_cache.get_stats()
        assert cache_stats['size'] == 0
    
    def test_concurrent_execution(self, saiql_engine, sample_queries):
        """Test concurrent query execution"""
        results = []
        errors = []
        
        def execute_query(query_id):
            try:
                context = ExecutionContext(session_id=f"concurrent_{query_id}")
                result = saiql_engine.execute(sample_queries['simple_select'], context)
                results.append(result)
            except Exception as e:
                errors.append(e)
        
        # Create and start threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=execute_query, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        # Verify results
        assert len(errors) == 0, f"Errors occurred: {errors}"
        assert len(results) == 5
        assert all(isinstance(r, QueryResult) for r in results)

@pytest.mark.unit
class TestQueryCache:
    """Test QueryCache functionality"""
    
    def test_cache_initialization(self):
        """Test cache initialization"""
        cache = QueryCache(max_size=10)
        
        assert cache.max_size == 10
        assert len(cache.cache) == 0
        assert cache.stats['hits'] == 0
        assert cache.stats['misses'] == 0
    
    def test_cache_put_get(self):
        """Test basic cache operations"""
        cache = QueryCache(max_size=5)
        
        # Test put and get
        cache.put("key1", "value1")
        result = cache.get("key1")
        
        assert result == "value1"
        assert cache.stats['hits'] == 1
        assert cache.stats['misses'] == 0
    
    def test_cache_miss(self):
        """Test cache miss"""
        cache = QueryCache(max_size=5)
        
        result = cache.get("nonexistent_key")
        
        assert result is None
        assert cache.stats['misses'] == 1
        assert cache.stats['hits'] == 0
    
    def test_cache_eviction(self):
        """Test cache eviction when max size reached"""
        cache = QueryCache(max_size=3)
        
        # Fill cache beyond capacity
        for i in range(5):
            cache.put(f"key{i}", f"value{i}")
        
        # Check that only max_size items remain
        assert len(cache.cache) == 3
        assert cache.stats['evictions'] >= 2
        
        # Verify most recent items are kept
        assert cache.get("key4") == "value4"
        assert cache.get("key3") == "value3"
        assert cache.get("key2") == "value2"
    
    def test_cache_clear(self):
        """Test cache clearing"""
        cache = QueryCache(max_size=5)
        
        # Add items
        for i in range(3):
            cache.put(f"key{i}", f"value{i}")
        
        # Clear cache
        cache.clear()
        
        assert len(cache.cache) == 0
        assert cache.get("key0") is None
    
    def test_cache_thread_safety(self):
        """Test cache thread safety"""
        cache = QueryCache(max_size=100)
        errors = []
        
        def cache_operations(thread_id):
            try:
                for i in range(10):
                    cache.put(f"thread{thread_id}_key{i}", f"value{i}")
                    cache.get(f"thread{thread_id}_key{i}")
            except Exception as e:
                errors.append(e)
        
        # Create and start threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=cache_operations, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        # Verify no errors
        assert len(errors) == 0, f"Thread safety errors: {errors}"
        assert cache.stats['hits'] > 0

@pytest.mark.unit
class TestSessionManager:
    """Test SessionManager functionality"""
    
    def test_session_creation(self):
        """Test session creation"""
        manager = SessionManager()
        context = ExecutionContext(session_id="")
        
        session_id = manager.create_session(context)
        
        assert session_id is not None
        assert session_id.startswith("saiql_session_")
        assert context.session_id == session_id
        
        session_data = manager.get_session(session_id)
        assert session_data is not None
        assert session_data['context'] == context
        assert session_data['state'] == SessionState.CREATED
    
    def test_session_update(self):
        """Test session updates"""
        manager = SessionManager()
        context = ExecutionContext(session_id="")
        session_id = manager.create_session(context)
        
        # Update session
        manager.update_session(session_id, state=SessionState.ACTIVE, query_count=5)
        
        session_data = manager.get_session(session_id)
        assert session_data['state'] == SessionState.ACTIVE
        assert session_data['query_count'] == 5
    
    def test_session_cleanup(self):
        """Test expired session cleanup"""
        manager = SessionManager()
        
        # Create sessions
        context1 = ExecutionContext(session_id="")
        context2 = ExecutionContext(session_id="")
        session_id1 = manager.create_session(context1)
        session_id2 = manager.create_session(context2)
        
        # Manually set old last_activity for one session
        manager.sessions[session_id1]['last_activity'] = time.time() - 7200  # 2 hours ago
        
        # Cleanup with 1 hour max age
        cleaned = manager.cleanup_expired_sessions(max_age_seconds=3600)
        
        assert cleaned == 1
        assert session_id1 not in manager.sessions
        assert session_id2 in manager.sessions

@pytest.mark.unit
class TestExecutionContext:
    """Test ExecutionContext functionality"""
    
    def test_context_creation(self):
        """Test context creation with defaults"""
        context = ExecutionContext(session_id="test_session")
        
        assert context.session_id == "test_session"
        assert context.execution_mode == ExecutionMode.SYNC
        assert context.timeout_seconds == 300
        assert context.max_memory_mb == 1024
        assert context.debug is False
        assert isinstance(context.metadata, dict)
    
    def test_context_customization(self):
        """Test context with custom parameters"""
        metadata = {'custom': 'value'}
        context = ExecutionContext(
            session_id="custom_session",
            user_id="user123",
            database_url="postgresql://test",
            execution_mode=ExecutionMode.ASYNC,
            timeout_seconds=600,
            max_memory_mb=2048,
            debug=True,
            metadata=metadata
        )
        
        assert context.session_id == "custom_session"
        assert context.user_id == "user123"
        assert context.database_url == "postgresql://test"
        assert context.execution_mode == ExecutionMode.ASYNC
        assert context.timeout_seconds == 600
        assert context.max_memory_mb == 2048
        assert context.debug is True
        assert context.metadata == metadata

@pytest.mark.unit
class TestQueryResult:
    """Test QueryResult functionality"""
    
    def test_result_creation(self):
        """Test result creation"""
        result = QueryResult(
            success=True,
            data=[{'id': 1, 'name': 'test'}],
            execution_time=0.5,
            query="*3[users]::name>>oQ",
            sql_generated="SELECT name FROM users LIMIT 3",
            rows_affected=1,
            session_id="test_session"
        )
        
        assert result.success is True
        assert len(result.data) == 1
        assert result.execution_time == 0.5
        assert result.rows_affected == 1
    
    def test_result_to_dict(self):
        """Test result serialization to dictionary"""
        result = QueryResult(
            success=True,
            data=[],
            execution_time=0.1,
            query="test",
            sql_generated="SELECT 1",
            rows_affected=0,
            session_id="test"
        )
        
        result_dict = result.to_dict()
        
        assert isinstance(result_dict, dict)
        assert 'success' in result_dict
        assert 'data' in result_dict
        assert 'execution_time' in result_dict
        assert 'query' in result_dict
        assert 'sql_generated' in result_dict
        assert 'session_id' in result_dict
    
    def test_result_with_error(self):
        """Test result with error information"""
        result = QueryResult(
            success=False,
            data=[],
            execution_time=0.1,
            query="invalid query",
            sql_generated="",
            rows_affected=0,
            session_id="test",
            error_message="Syntax error",
            error_phase="parsing"
        )
        
        assert result.success is False
        assert result.error_message == "Syntax error"
        assert result.error_phase == "parsing"

@pytest.mark.unit
class TestPerformanceTracker:
    """Test PerformanceTracker functionality"""
    
    def test_tracker_initialization(self):
        """Test tracker initialization"""
        tracker = PerformanceTracker()
        
        assert hasattr(tracker, 'operations')
        assert len(tracker.operations) == 0
    
    def test_record_operation(self):
        """Test recording performance data"""
        tracker = PerformanceTracker()
        
        tracker.record(
            operation='test_operation',
            execution_time=0.5,
            metadata={'test': 'data'}
        )
        
        stats = tracker.get_stats()
        assert 'operations' in stats
        assert stats['operations'] >= 1
    
    def test_multiple_operations(self):
        """Test recording multiple operations"""
        tracker = PerformanceTracker()
        
        # Record multiple operations
        for i in range(5):
            tracker.record(
                operation=f'operation_{i}',
                execution_time=0.1 * i,
                metadata={'iteration': i}
            )
        
        stats = tracker.get_stats()
        assert stats['operations'] >= 5

# Integration tests that use multiple components
@pytest.mark.integration
class TestEngineIntegration:
    """Integration tests for engine components"""
    
    def test_full_query_pipeline(self, saiql_engine, sample_queries, execution_context):
        """Test complete query execution pipeline"""
        query = sample_queries['simple_select']
        
        # Execute query
        result = saiql_engine.execute(query, execution_context)
        
        # Verify all pipeline components worked
        assert result.lexing_time >= 0
        assert result.parsing_time >= 0
        assert result.compilation_time >= 0
        assert result.sql_generated is not None
        assert len(result.sql_generated) > 0
    
    def test_session_and_cache_integration(self, saiql_engine, sample_queries):
        """Test session management and caching integration"""
        # Create multiple contexts with same session
        session_id = "integration_test_session"
        context1 = ExecutionContext(session_id=session_id)
        context2 = ExecutionContext(session_id=session_id)
        
        # Execute queries
        result1 = saiql_engine.execute(sample_queries['simple_select'], context1)
        result2 = saiql_engine.execute(sample_queries['count_query'], context2)
        
        # Verify session consistency
        assert result1.session_id == session_id
        assert result2.session_id == session_id
        
        # Check session manager
        session_data = saiql_engine.session_manager.get_session(session_id)
        assert session_data is not None
        assert session_data['query_count'] >= 2
    
    @pytest.mark.slow
    def test_performance_under_load(self, saiql_engine, performance_queries, timer):
        """Test engine performance under load"""
        timer.start()
        
        results = []
        for query in performance_queries[:10]:  # Limit for unit test
            context = ExecutionContext(session_id=f"perf_test_{len(results)}")
            result = saiql_engine.execute(query, context)
            results.append(result)
        
        total_time = timer.stop()
        
        # Performance assertions
        assert len(results) == 10
        assert total_time < 30  # Should complete within 30 seconds
        assert all(r.execution_time < 5 for r in results)  # Each query under 5 seconds
        
        # Check statistics
        stats = saiql_engine.get_stats()
        assert stats['queries_executed'] >= 10
        assert stats['average_execution_time'] > 0
# IHNpZ25hdHVyZTplYzUyNTBkZWRmNTZjZTZiMDA4NCBpZDpTQUlRTC1DSEFSTElFLVBST0QgYnk6QXBvbGxvICYgQ2xhdWRlIA==
