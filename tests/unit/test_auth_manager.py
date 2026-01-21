#!/usr/bin/env python3
"""
SAIQL Authentication Manager Unit Tests - LoreToken Ultra Format

LORETOKEN_TEST_AUTH_SCOPE: comprehensive_authentication_system_validation
LORETOKEN_TEST_COVERAGE: user_management jwt_tokens api_keys rbac rate_limiting security_events
LORETOKEN_TEST_STATUS: production_ready_saiql_bravo

Author: Apollo & Claude
Version: 1.0.0 
Status: LORETOKEN_COMPRESSED_TESTING_FRAMEWORK
"""

import pytest
import time
import json
import tempfile
from datetime import datetime, timedelta
from security.auth_manager import AuthManager, User, UserRole, APIKey, AuthResult, RateLimiter

@pytest.mark.auth
class TestAuthManager:
    """LORETOKEN_AUTH_CORE: manager_initialization token_lifecycle user_ops api_key_ops rbac_validation"""
    
    def test_manager_initialization(self, auth_manager):
        """LORETOKEN_INIT_TEST: basic_setup default_admin_creation config_loading"""
        assert auth_manager is not None
        assert len(auth_manager.users) >= 1  # Default admin
        assert auth_manager.secret_key is not None
        assert len(auth_manager.secret_key) > 10
    
    def test_user_creation_and_management(self, auth_manager):
        """LORETOKEN_USER_CRUD: create_modify_delete user_validation role_assignment"""
        # Create user
        user = auth_manager.create_user(
            username="testuser",
            email="test@example.com",
            roles=[UserRole.READ_WRITE],
            user_id="test_123"
        )
        
        assert user.user_id == "test_123"
        assert user.username == "testuser"
        assert UserRole.READ_WRITE in user.roles
        assert user.has_permission("read")
        assert user.has_permission("write")
        assert not user.has_permission("admin")
    
    def test_jwt_token_lifecycle(self, auth_manager, test_user):
        """LORETOKEN_JWT_CYCLE: create_verify_expire token_claims user_validation"""
        # Create token
        token = auth_manager.create_token(test_user.user_id)
        assert token is not None
        assert len(token) > 50
        
        # Verify token
        auth_result = auth_manager.verify_token(token)
        assert auth_result.success is True
        assert auth_result.user_id == test_user.user_id
        assert auth_result.user.username == test_user.username
    
    def test_api_key_management(self, auth_manager, test_user):
        """LORETOKEN_API_KEY_OPS: create_verify_usage_tracking expiry_validation"""
        # Create API key
        api_key, key_secret = auth_manager.create_api_key(
            user_id=test_user.user_id,
            name="Test Key",
            roles=[UserRole.READ_ONLY],
            expires_days=30
        )
        
        assert api_key.key_id.startswith("sk_")
        assert len(key_secret) > 20
        assert api_key.usage_count == 0
        
        # Verify API key
        auth_result = auth_manager.verify_api_key(key_secret)
        assert auth_result.success is True
        assert auth_result.user_id == test_user.user_id
        
        # Check usage tracking
        updated_key = auth_manager.api_keys[api_key.key_id]
        assert updated_key.usage_count == 1
        assert updated_key.last_used is not None
    
    def test_token_refresh(self, auth_manager, test_user):
        """Ensure refresh_token issues a new JWT"""
        token = auth_manager.create_token(test_user.user_id)
        result = auth_manager.refresh_token(token)
        
        assert result.success is True
        assert result.metadata.get("token") != token
        assert result.user_id == test_user.user_id
    
    def test_api_key_rotation(self, auth_manager, test_user):
        """Verify API key rotation deactivates old key and returns new secret"""
        api_key, key_secret = auth_manager.create_api_key(
            user_id=test_user.user_id,
            name="Initial Key",
            roles=[UserRole.READ_ONLY],
            expires_days=7
        )
        
        new_key, new_secret = auth_manager.rotate_api_key(api_key.key_id)
        
        assert new_key.key_id != api_key.key_id
        assert new_secret != key_secret
        assert auth_manager.api_keys[api_key.key_id].is_active is False
    
    def test_rbac_permissions(self, auth_manager):
        """LORETOKEN_RBAC_TEST: role_permissions permission_matrix user_access_validation"""
        # Create users with different roles
        admin = auth_manager.create_user("admin", "admin@test.com", [UserRole.ADMIN])
        writer = auth_manager.create_user("writer", "writer@test.com", [UserRole.READ_WRITE])
        reader = auth_manager.create_user("reader", "reader@test.com", [UserRole.READ_ONLY])
        
        # Test admin permissions
        assert admin.has_permission("admin")
        assert admin.has_permission("write")
        assert admin.has_permission("read")
        
        # Test writer permissions
        assert not writer.has_permission("admin")
        assert writer.has_permission("write")
        assert writer.has_permission("read")
        
        # Test reader permissions
        assert not reader.has_permission("admin")
        assert not reader.has_permission("write")
        assert reader.has_permission("read")

@pytest.mark.auth
class TestRateLimiter:
    """LORETOKEN_RATE_LIMIT: request_throttling burst_protection time_window_validation"""
    
    def test_rate_limit_enforcement(self):
        """LORETOKEN_RATE_TEST: max_requests time_window eviction_policy"""
        limiter = RateLimiter(max_requests=3, time_window=1)
        
        # First 3 requests should pass
        for i in range(3):
            allowed, remaining = limiter.is_allowed("test_user")
            assert allowed is True
            assert remaining >= 0
        
        # 4th request should be blocked
        allowed, remaining = limiter.is_allowed("test_user")
        assert allowed is False
        assert remaining == 0
    
    def test_rate_limit_time_window(self):
        """LORETOKEN_TIME_WINDOW: request_expiry window_sliding reset_behavior"""
        limiter = RateLimiter(max_requests=2, time_window=1)
        
        # Use up quota
        limiter.is_allowed("test_user")
        limiter.is_allowed("test_user")
        
        # Should be blocked
        allowed, _ = limiter.is_allowed("test_user")
        assert allowed is False
        
        # Wait for window to pass
        time.sleep(1.1)
        
        # Should be allowed again
        allowed, _ = limiter.is_allowed("test_user")
        assert allowed is True

@pytest.mark.auth
class TestSecurityFeatures:
    """LORETOKEN_SECURITY: token_expiry api_key_expiry failed_attempts security_logging"""
    
    def test_token_expiry_validation(self, auth_manager, test_user):
        """LORETOKEN_TOKEN_EXPIRY: expired_token_rejection invalid_signature_handling"""
        # Create token with very short expiry
        with patch.object(auth_manager.config['jwt'], 'expiry_hours', 0.001):  # ~3.6 seconds
            token = auth_manager.create_token(test_user.user_id)
            
            # Should work immediately
            result = auth_manager.verify_token(token)
            assert result.success is True
            
            # Wait for expiry
            time.sleep(0.1)
            
            # Should fail after expiry
            result = auth_manager.verify_token(token)
            assert result.success is False
            assert "expired" in result.error_message.lower()
    
    def test_api_key_expiry(self, auth_manager, test_user):
        """LORETOKEN_KEY_EXPIRY: expired_key_rejection expiry_date_validation"""
        # Create key that expires in past
        api_key, key_secret = auth_manager.create_api_key(
            user_id=test_user.user_id,
            name="Expired Key",
            roles=[UserRole.READ_ONLY],
            expires_days=1
        )
        
        # Manually set expiry to past
        api_key.expires_at = datetime.now() - timedelta(days=1)
        auth_manager.api_keys[api_key.key_id] = api_key
        
        # Should be rejected
        result = auth_manager.verify_api_key(key_secret)
        assert result.success is False
        assert "expired" in result.error_message.lower()
    
    def test_security_event_logging(self, auth_manager, test_user):
        """LORETOKEN_SEC_LOG: event_tracking invalid_attempts audit_trail"""
        # Clear existing events
        auth_manager.security_events.clear()
        
        # Generate security events
        auth_manager.verify_api_key("invalid_key")  # Should log invalid attempt
        auth_manager.create_token(test_user.user_id)  # Should log token creation
        
        # Check events were logged
        events = list(auth_manager.security_events)
        assert len(events) >= 1
        
        # Verify event structure
        for event in events:
            assert 'timestamp' in event
            assert 'event_type' in event
            assert 'details' in event

@pytest.mark.integration
class TestAuthIntegration:
    """LORETOKEN_AUTH_INTEGRATION: full_flow_validation persistence_testing multi_user_scenarios"""
    
    def test_full_authentication_flow(self, auth_manager):
        """LORETOKEN_FULL_FLOW: user_create api_key_create token_create cross_validation"""
        # Create user
        user = auth_manager.create_user("flowtest", "flow@test.com", [UserRole.READ_WRITE])
        
        # Create API key for user
        api_key, key_secret = auth_manager.create_api_key(
            user_id=user.user_id,
            name="Flow Test Key",
            roles=[UserRole.READ_WRITE]
        )
        
        # Create JWT token for user
        jwt_token = auth_manager.create_token(user.user_id)
        
        # Verify both authentication methods work
        api_result = auth_manager.verify_api_key(key_secret)
        jwt_result = auth_manager.verify_token(jwt_token)
        
        assert api_result.success is True
        assert jwt_result.success is True
        assert api_result.user_id == jwt_result.user_id == user.user_id
    
    def test_concurrent_authentication(self, auth_manager, test_user):
        """LORETOKEN_CONCURRENT: thread_safety race_conditions concurrent_access"""
        import threading
        
        results = []
        errors = []
        
        def auth_operations(thread_id):
            try:
                # Create API key
                api_key, key_secret = auth_manager.create_api_key(
                    user_id=test_user.user_id,
                    name=f"Concurrent Key {thread_id}",
                    roles=[UserRole.READ_ONLY]
                )
                
                # Verify multiple times
                for _ in range(5):
                    result = auth_manager.verify_api_key(key_secret)
                    results.append(result.success)
                    
            except Exception as e:
                errors.append(e)
        
        # Run concurrent operations
        threads = []
        for i in range(3):
            thread = threading.Thread(target=auth_operations, args=(i,))
            threads.append(thread)
            thread.start()
        
        for thread in threads:
            thread.join()
        
        # Verify no errors and all successes
        assert len(errors) == 0, f"Concurrent errors: {errors}"
        assert all(results), "Some auth operations failed"
    
    def test_statistics_and_monitoring(self, auth_manager, test_user):
        """LORETOKEN_STATS: usage_metrics security_analytics performance_tracking"""
        # Perform various operations
        auth_manager.create_token(test_user.user_id)
        api_key, key_secret = auth_manager.create_api_key(
            user_id=test_user.user_id,
            name="Stats Test",
            roles=[UserRole.READ_ONLY]
        )
        auth_manager.verify_api_key(key_secret)
        auth_manager.verify_api_key("invalid_key")  # Generate failed attempt
        
        # Get statistics
        stats = auth_manager.get_statistics()
        
        assert 'total_users' in stats
        assert 'total_api_keys' in stats
        assert 'recent_security_events' in stats
        assert stats['total_users'] >= 1
        assert stats['total_api_keys'] >= 1

# LORETOKEN_PERFORMANCE_TESTS: load_testing stress_testing benchmark_validation
@pytest.mark.performance
class TestAuthPerformance:
    """LORETOKEN_PERF_AUTH: scalability_testing response_times memory_usage"""
    
    @pytest.mark.slow
    def test_auth_performance_load(self, auth_manager, timer):
        """LORETOKEN_LOAD_TEST: high_volume_auth token_verification_speed api_key_performance"""
        # Create test user and API key
        user = auth_manager.create_user("perftest", "perf@test.com", [UserRole.READ_WRITE])
        api_key, key_secret = auth_manager.create_api_key(
            user_id=user.user_id,
            name="Performance Test",
            roles=[UserRole.READ_WRITE]
        )
        jwt_token = auth_manager.create_token(user.user_id)
        
        # Performance test - API key verification
        timer.start()
        for _ in range(100):
            result = auth_manager.verify_api_key(key_secret)
            assert result.success is True
        api_time = timer.stop()
        
        # Performance test - JWT verification
        timer.start()
        for _ in range(100):
            result = auth_manager.verify_token(jwt_token)
            assert result.success is True
        jwt_time = timer.stop()
        
        # Performance assertions
        assert api_time < 5.0  # 100 API key verifications under 5 seconds
        assert jwt_time < 2.0  # 100 JWT verifications under 2 seconds
        
        # Average time per operation
        avg_api_time = api_time / 100
        avg_jwt_time = jwt_time / 100
        
        assert avg_api_time < 0.05  # Under 50ms per API key verification
        assert avg_jwt_time < 0.02  # Under 20ms per JWT verification

# LORETOKEN_TEST_SUMMARY: auth_manager_comprehensive_validation rbac_security_performance_confirmed production_ready_status
# IHNpZ25hdHVyZTplYzUyNTBkZWRmNTZjZTZiMDA4NCBpZDpTQUlRTC1DSEFSTElFLVBST0QgYnk6QXBvbGxvICYgQ2xhdWRlIA==
