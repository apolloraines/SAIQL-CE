#!/usr/bin/env python3
"""
Test Coverage Booster - Core Operator Tests
===========================================

Comprehensive tests for the new core operators to boost overall test coverage.

Author: Apollo & Claude  
Version: 1.0.0
"""

import unittest
import sys
import os
from typing import Dict, Any, List
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

class TestCoreOperatorLexer(unittest.TestCase):
    """Test the core operator lexer"""
    
    def setUp(self):
        """Set up test environment"""
        try:
            # Safe import instead of exec
            from core.legacy.experimental.core_lexer import SAIQLLexer, TokenType
            self.lexer_class = SAIQLLexer
            self.token_type_class = TokenType
        except ImportError as e:
            self.skipTest(f"Core lexer not available: {e}")
    
    def test_basic_operators(self):
        """Test basic operator tokenization"""
        lexer = self.lexer_class("* >> :: |")
        tokens = lexer.get_tokens()
        
        expected_types = [
            self.token_type_class.ASTERISK,
            self.token_type_class.OUTPUT_REDIRECT,
            self.token_type_class.COLUMN_SELECTOR,
            self.token_type_class.FILTER,
            self.token_type_class.EOF
        ]
        
        self.assertEqual(len(tokens), len(expected_types))
        for token, expected_type in zip(tokens, expected_types):
            self.assertEqual(token.type, expected_type)
    
    def test_comparison_operators(self):
        """Test comparison operator tokenization"""
        lexer = self.lexer_class("== >= <= = > <")
        tokens = lexer.get_tokens()
        
        # Check that strict equals comes before equals
        comparison_tokens = [t for t in tokens if t.type != self.token_type_class.EOF]
        self.assertEqual(comparison_tokens[0].type, self.token_type_class.STRICT_EQUALS)
        self.assertEqual(comparison_tokens[1].type, self.token_type_class.GREATER_EQUAL)
        self.assertEqual(comparison_tokens[2].type, self.token_type_class.LESS_EQUAL)
    
    def test_function_tokenization(self):
        """Test function keyword tokenization"""
        lexer = self.lexer_class("COUNT SUM AVG SIMILAR RECENT")
        tokens = lexer.get_tokens()
        
        function_types = [
            self.token_type_class.COUNT,
            self.token_type_class.SUM,
            self.token_type_class.AVG,
            self.token_type_class.SIMILAR,
            self.token_type_class.RECENT
        ]
        
        for i, expected_type in enumerate(function_types):
            self.assertEqual(tokens[i].type, expected_type)
    
    def test_complex_query_tokenization(self):
        """Test complex query tokenization"""
        query = "*10[users]::name,email|status='active'>>oQ"
        lexer = self.lexer_class(query)
        tokens = lexer.get_tokens()
        
        # Should have tokens for all components
        self.assertGreater(len(tokens), 10)
        
        # First token should be asterisk
        self.assertEqual(tokens[0].type, self.token_type_class.ASTERISK)
        
        # Should have number token
        number_tokens = [t for t in tokens if t.type == self.token_type_class.NUMBER]
        self.assertEqual(len(number_tokens), 1)
        self.assertEqual(number_tokens[0].value, "10")


class TestCoreOperatorRuntime(unittest.TestCase):
    """Test the core operator runtime"""
    
    def setUp(self):
        """Set up test environment"""
        try:
            # Safe import instead of exec
            from core.legacy.experimental.core_runtime import CoreOperatorRuntime
            self.runtime = CoreOperatorRuntime()
        except ImportError as e:
            self.skipTest(f"Core runtime not available: {e}")
    
    def test_all_operators_registered(self):
        """Test that all expected operators are registered"""
        expected_operators = {
            "*", ">>", "oQ", "::", "|",
            "=", "==", ">", "<", ">=", "<=",
            "&", "!",
            "+", "-",
            "COUNT", "SUM", "AVG",
            "SIMILAR", "RECENT"
        }
        
        self.assertEqual(set(self.runtime.operators.keys()), expected_operators)
    
    def test_select_operator(self):
        """Test SELECT (*) operator"""
        result = self.runtime.execute_operator("*", 5, "products")
        
        self.assertEqual(result["operation"], "SELECT")
        self.assertEqual(result["limit"], 5)
        self.assertEqual(result["table"], "products")
        self.assertIn("SELECT * FROM products LIMIT 5", result["sql_hint"])
    
    def test_filter_operator(self):
        """Test filter (|) operator"""
        result = self.runtime.execute_operator("|", "price > 100")
        
        self.assertEqual(result["operation"], "FILTER")
        self.assertEqual(result["condition"], "price > 100")
        self.assertEqual(result["sql_hint"], "WHERE")
    
    def test_column_selector(self):
        """Test column selector (::) operator"""
        result = self.runtime.execute_operator("::", ["id", "name", "price"])
        
        self.assertEqual(result["operation"], "COLUMN_SELECT")
        self.assertEqual(result["columns"], ["id", "name", "price"])
        self.assertEqual(result["sql_hint"], "SELECT id, name, price")
    
    def test_comparison_operators_execution(self):
        """Test all comparison operators"""
        test_cases = [
            ("=", 5, 5, True),
            ("=", 5, 3, False),
            ("==", "test", "test", False),  # strict equality
            (">", 10, 5, True),
            (">", 3, 8, False),
            ("<", 3, 8, True),
            ("<", 10, 5, False),
            (">=", 5, 5, True),
            (">=", 3, 8, False),
            ("<=", 3, 8, True),
            ("<=", 10, 5, False)
        ]
        
        for operator, left, right, expected in test_cases:
            with self.subTest(operator=operator, left=left, right=right):
                result = self.runtime.execute_operator(operator, left, right)
                self.assertEqual(result["result"], expected)
                self.assertEqual(result["left"], left)
                self.assertEqual(result["right"], right)
    
    def test_logical_operators(self):
        """Test logical operators"""
        # Test AND
        result = self.runtime.execute_operator("&", True, True, False)
        self.assertFalse(result["result"])
        
        result = self.runtime.execute_operator("&", True, True, True)
        self.assertTrue(result["result"])
        
        # Test NOT
        result = self.runtime.execute_operator("!", True)
        self.assertFalse(result["result"])
        
        result = self.runtime.execute_operator("!", False)
        self.assertTrue(result["result"])
    
    def test_arithmetic_operators(self):
        """Test arithmetic operators"""
        # Test addition
        result = self.runtime.execute_operator("+", 15, 25)
        self.assertEqual(result["result"], 40)
        self.assertEqual(result["sql_hint"], "15 + 25")
        
        # Test subtraction
        result = self.runtime.execute_operator("-", 20, 8)
        self.assertEqual(result["result"], 12)
        self.assertEqual(result["sql_hint"], "20 - 8")
    
    def test_aggregate_functions(self):
        """Test aggregate functions"""
        test_data = [10, 20, 30, 40, 50]
        
        # Test COUNT
        result = self.runtime.execute_operator("COUNT", test_data)
        self.assertEqual(result["result"], 5)
        self.assertEqual(result["sql_hint"], "COUNT(*)")
        
        # Test SUM
        result = self.runtime.execute_operator("SUM", test_data)
        self.assertEqual(result["result"], 150)
        self.assertEqual(result["sql_hint"], "SUM(column)")
        
        # Test AVG
        result = self.runtime.execute_operator("AVG", test_data)
        self.assertEqual(result["result"], 30.0)
        self.assertEqual(result["sql_hint"], "AVG(column)")
    
    def test_similarity_operator(self):
        """Test similarity (SIMILAR) operator"""
        # Should match with high similarity
        result = self.runtime.execute_operator("SIMILAR", "machine learning algorithms", "machine learning")
        self.assertTrue(result["result"])
        self.assertGreater(result["similarity"], 0.5)
        
        # Should not match with low similarity
        result = self.runtime.execute_operator("SIMILAR", "completely different text", "machine learning")
        self.assertFalse(result["result"])
        self.assertLess(result["similarity"], 0.5)
        
        # Custom threshold
        result = self.runtime.execute_operator("SIMILAR", "partial match", "match", 0.3)
        self.assertTrue(result["result"])
    
    def test_recent_operator(self):
        """Test recent (RECENT) operator"""
        # Test hours
        result = self.runtime.execute_operator("RECENT", "24h")
        self.assertEqual(result["operation"], "RECENT_FILTER")
        self.assertEqual(result["timeframe"], "24h")
        self.assertIn("created_at >", result["sql_hint"])
        
        # Test days
        result = self.runtime.execute_operator("RECENT", "7d")
        self.assertEqual(result["timeframe"], "7d")
        
        # Test weeks
        result = self.runtime.execute_operator("RECENT", "2w")
        self.assertEqual(result["timeframe"], "2w")
    
    def test_output_operators(self):
        """Test output operators"""
        # Test output redirect
        result = self.runtime.execute_operator(">>", "results.csv")
        self.assertEqual(result["operation"], "OUTPUT_REDIRECT")
        self.assertEqual(result["target"], "results.csv")
        self.assertEqual(result["sql_hint"], "INTO")
        
        # Test output query
        result = self.runtime.execute_operator("oQ")
        self.assertEqual(result["operation"], "OUTPUT_QUERY")
        self.assertEqual(result["sql_hint"], "EXECUTE")
    
    def test_operator_error_handling(self):
        """Test error handling for unknown operators"""
        with self.assertRaises(NotImplementedError):
            self.runtime.execute_operator("UNKNOWN_OP", "test")


class TestCoreOperatorIntegration(unittest.TestCase):
    """Integration tests for core operators"""
    
    def setUp(self):
        """Set up test environment"""
        try:
            # Safe imports instead of exec
            from core.legacy.experimental.core_lexer import SAIQLLexer, TokenType
            from core.legacy.experimental.core_runtime import CoreOperatorRuntime
            self.lexer_class = SAIQLLexer
            self.runtime = CoreOperatorRuntime()
        except Exception as e:
            self.skipTest(f"Core components not available: {e}")
    
    def test_query_tokenization_and_execution(self):
        """Test that tokens can be executed"""
        query = "*5[users]::name,email|status='active'"
        
        # Tokenize
        lexer = self.lexer_class(query)
        tokens = lexer.get_tokens()
        
        # Should have tokens
        self.assertGreater(len(tokens), 5)
        
        # Test individual operators
        result = self.runtime.execute_operator("*", 5, "users")
        self.assertEqual(result["limit"], 5)
        
        result = self.runtime.execute_operator("::", ["name", "email"])
        self.assertEqual(result["columns"], ["name", "email"])
        
        result = self.runtime.execute_operator("|", "status='active'")
        self.assertEqual(result["condition"], "status='active'")


if __name__ == "__main__":
    # Run with high verbosity
    unittest.main(verbosity=2)
# IHNpZ25hdHVyZTplYzUyNTBkZWRmNTZjZTZiMDA4NCBpZDpTQUlRTC1DSEFSTElFLVBST0QgYnk6QXBvbGxvICYgQ2xhdWRlIA==
