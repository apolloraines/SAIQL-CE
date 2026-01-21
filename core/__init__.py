#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SAIQL Core Package Initialization
Exports all main components for clean imports

Author: Apollo & Claude
Version: 1.0.0
"""

# =============================================================================
# Define QueryType, QueryComponents, ParsedToken FIRST to avoid circular imports
# (symbolic_engine.py imports these from core, so they must exist before we
# import SymbolicEngine)
# =============================================================================

class QueryType:
    """Query type enumeration"""
    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    JOIN = "JOIN"
    AGGREGATE = "AGGREGATE"
    TRANSACTION = "TRANSACTION"


class QueryComponents:
    """Container for parsed query components"""
    def __init__(self):
        self.select_fields = []
        self.from_tables = []
        self.where_conditions = []
        self.joins = []
        self.aggregations = []
        self.query_type = None
        # Additional attributes used by symbolic_engine
        self.raw_query = ""
        self.sql_translation = ""
        self.columns = []
        self.tables = []
        self.tokens = []
        self.human_readable = ""


class ParsedToken:
    """Represents a parsed token with semantic information"""
    def __init__(self, token_type, value, semantic_info=None, sql_hint=None):
        self.token_type = token_type
        self.value = value
        self.semantic_info = semantic_info
        self.sql_hint = sql_hint
        # Alias for compatibility
        self.semantic = semantic_info


# =============================================================================
# Core Components (import after QueryType/QueryComponents are defined)
# =============================================================================

from .lexer import SAIQLLexer, TokenType, Token
from .parser import SAIQLParser, ASTNode, NodeType
from .compiler import SAIQLCompiler, TargetDialect, OptimizationLevel, CompilationResult
from .engine import SAIQLEngine, QueryResult
from .runtime import SAIQLRuntime
# from .saiql_core import SAIQLParser as CoreParser
CoreParser = SAIQLParser

try:
    from .symbolic_engine import SymbolicEngine
except ImportError as e:
    raise ImportError(
        "SymbolicEngine not available. "
        "Ensure symbolic_engine.py is present in core/."
    ) from e

# Import type Enums from saiql_types to ensure consistency across codebase
from .saiql_types import DataType, OperatorType, JoinType, AggregationType

# Export everything
__all__ = [
    # Core Classes
    'SAIQLLexer',
    'SAIQLParser', 
    'SAIQLCompiler',
    'SAIQLEngine',
    'SAIQLRuntime',
    'SymbolicEngine',
    
    # Types and Enums
    'TokenType',
    'Token',
    'ASTNode', 
    'NodeType',
    'TargetDialect',
    'OptimizationLevel',
    'CompilationResult',
    'QueryResult',
    'QueryType',
    'DataType',
    'OperatorType', 
    'JoinType',
    'AggregationType',
    
    # Components
    'QueryComponents',
    'ParsedToken',
    
    # Core Parser Alias
    'CoreParser'
]

# Version info
__version__ = '1.0.0-ce'
__author__ = 'Apollo & Claude'
__description__ = 'SAIQL Community Edition - Semantic AI Query Language'
__edition__ = 'ce'

# CE Edition: Block access to removed modules
_BANNED_MODULES = {'atlas', 'qipi', 'loretokens', 'lore_core', 'lore_chunk',
                   'imagination', 'rag', 'Copilot_Carl', 'vector_engine'}


def __getattr__(name: str):
    """Block access to removed modules in CE."""
    if name in _BANNED_MODULES:
        from .ce_edition import CENotShippedError
        raise CENotShippedError(name)
    raise AttributeError(f"module 'core' has no attribute '{name}'")
