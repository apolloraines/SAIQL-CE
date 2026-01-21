#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SAIQL Core Package Initialization
Exports all main components for clean imports

Author: Apollo & Claude
Version: 0.3.0-alpha
"""

# Core Components (prefer absolute imports; fallback to relative if packaged)
try:
    from core.lexer import SAIQLLexer, TokenType, Token
    from core.parser import SAIQLParser, ASTNode, NodeType as ASTNodeType
    from core.compiler import SAIQLCompiler, TargetDialect, OptimizationLevel, CompilationResult
    from core.engine import SAIQLEngine, QueryResult
    from core.runtime import SAIQLRuntime
    from core.saiql_types import (
        DataType,
        OperatorType,
        JoinType,
        AggregationType
    )
    from core.symbolic_engine import SymbolicEngine
    # Import QueryType, QueryComponents, and CoreParser from core to avoid None exports
    from core import QueryType, QueryComponents, CoreParser
except ImportError:  # pragma: no cover - packaging fallback
    from .core.lexer import SAIQLLexer, TokenType, Token
    from .core.parser import SAIQLParser, ASTNode, NodeType as ASTNodeType
    from .core.compiler import SAIQLCompiler, TargetDialect, OptimizationLevel, CompilationResult
    from .core.engine import SAIQLEngine, QueryResult
    from .core.runtime import SAIQLRuntime
    from .core.saiql_types import (
        DataType,
        OperatorType,
        JoinType,
        AggregationType
    )
    from .core.symbolic_engine import SymbolicEngine
    # Import QueryType, QueryComponents, and CoreParser from core to avoid None exports
    from .core import QueryType, QueryComponents, CoreParser

# Public API Façade
_default_engine = None

def get_engine() -> SAIQLEngine:
    """Get or create the default SAIQL engine instance"""
    global _default_engine
    if _default_engine is None:
        _default_engine = SAIQLEngine()
    return _default_engine

def execute(query: str, context: dict = None) -> QueryResult:
    """
    Execute a SAIQL query using the default engine.

    Args:
        query: The SAIQL query string
        context: Optional execution context

    Returns:
        QueryResult object
    """
    engine = get_engine()
    return engine.execute(query, context=context)

def connect(db_path: str = None) -> SAIQLEngine:
    """Connect to a specific database instance"""
    return SAIQLEngine(db_path=db_path)

# Export everything
__all__ = [
    # Public API
    'execute',
    'connect',
    'get_engine',
    'SAIQLEngine',
    'QueryResult',

    # Core Classes (Advanced)
    'SAIQLLexer',
    'SAIQLParser',
    'SAIQLCompiler',
    'SAIQLRuntime',
    'SymbolicEngine',

    # Types and Enums
    'TokenType',
    'Token',
    'ASTNode',
    'ASTNodeType',
    'TargetDialect',
    'OptimizationLevel',
    'CompilationResult',
    'QueryType',
    'DataType',
    'OperatorType',
    'JoinType',
    'AggregationType',

    # Components
    'QueryComponents',

    # Core Parser Alias
    'CoreParser'
]

# Version info
__version__ = '0.3.0-alpha'
__author__ = 'Apollo & Claude'
__description__ = 'SAIQL - Semantic AI Query Language'
