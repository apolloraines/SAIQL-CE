#!/usr/bin/env python3
"""
SAIQL Safety System
Defines policies and validators for safe query execution.
"""

from dataclasses import dataclass, field
from typing import Set

@dataclass
class SafetyPolicy:
    """Configuration for query safety guardrails"""
    
    name: str = "custom"
    
    # Resource Limits
    max_rows_scanned: int = 100000
    max_rows_returned: int = 1000
    max_execution_time_ms: int = 5000
    max_memory_mb: int = 512
    
    # Query Constraints
    require_where_clause: bool = True
    require_limit_clause: bool = False
    max_joins: int = 3
    
    # Forbidden Items
    forbidden_tables: Set[str] = field(default_factory=set)
    forbidden_columns: Set[str] = field(default_factory=set)
    read_only: bool = False
    
    # AI Safety
    allow_introspection: bool = False  # Can AI query its own logs?
    
    @classmethod
    def strict(cls):
        """Strict policy for production/untrusted input"""
        return cls(
            name="strict",
            max_rows_returned=100,
            require_where_clause=True,
            require_limit_clause=True,
            read_only=True
        )
    
    @classmethod
    def development(cls):
        """Relaxed policy for development"""
        return cls(
            name="development",
            max_rows_scanned=1000000,
            max_rows_returned=10000,
            require_where_clause=False,
            read_only=False
        )

    def validate_query(self, ast) -> None:
        """
        Validate AST against safety policy

        Args:
            ast: The parsed AST node

        Raises:
            SafetyViolation: If policy is violated
        """
        # Check for read-only violation
        if self.read_only:
            query_type = getattr(ast, 'query_type', 'UNKNOWN')
            # Allow read-only query types: SELECT, JOIN, AGGREGATE, and UNKNOWN (for safety)
            read_only_types = ('SELECT', 'JOIN', 'AGGREGATE', 'UNKNOWN')
            if query_type not in read_only_types:
                raise SafetyViolation(f"Write operation '{query_type}' forbidden by read-only policy")
        
        # Check for WHERE clause requirement
        if self.require_where_clause:
            # Only applies to UPDATE/DELETE usually, but if strict, maybe SELECT too?
            # Let's assume it applies to UPDATE/DELETE for now, or all queries if very strict.
            # Based on standard DB safety, usually UPDATE/DELETE require WHERE.
            # But if this is "require_where_clause" generic, maybe it means ALL queries?
            # Let's assume it means "unsafe" queries (UPDATE/DELETE) must have WHERE.
            
            query_type = getattr(ast, 'query_type', 'UNKNOWN')
            if query_type in ('UPDATE', 'DELETE'):
                has_where = False
                if hasattr(ast, 'conditions') and ast.conditions:
                    has_where = True
                
                if not has_where:
                    raise SafetyViolation(f"{query_type} requires WHERE clause under current safety policy")
        
        # Check for forbidden tables
        if self.forbidden_tables:
            tables = self._extract_tables(ast)
            for table in tables:
                if table.lower() in {t.lower() for t in self.forbidden_tables}:
                    raise SafetyViolation(f"Access to table '{table}' is forbidden by policy")

        # Check for forbidden columns
        if self.forbidden_columns:
            columns = self._extract_columns(ast)
            for column in columns:
                if column.lower() in {c.lower() for c in self.forbidden_columns}:
                    raise SafetyViolation(f"Access to column '{column}' is forbidden by policy")

    def _extract_tables(self, ast) -> Set[str]:
        """Extract table names from AST by traversing nodes."""
        tables = set()

        # Check direct table references
        if hasattr(ast, 'table_name') and ast.table_name:
            tables.add(ast.table_name)

        # Check target (could be ContainerNode with table contents)
        if hasattr(ast, 'target') and ast.target:
            tables.update(self._extract_tables(ast.target))

        # Check contents (for ContainerNode)
        if hasattr(ast, 'contents'):
            for item in ast.contents:
                tables.update(self._extract_tables(item))

        # Check from_tables attribute
        if hasattr(ast, 'from_tables'):
            for t in ast.from_tables:
                if isinstance(t, str):
                    tables.add(t)
                elif hasattr(t, 'table_name'):
                    tables.add(t.table_name)

        # Check join tables
        if hasattr(ast, 'left_table') and ast.left_table:
            tables.update(self._extract_tables(ast.left_table))
        if hasattr(ast, 'right_table') and ast.right_table:
            tables.update(self._extract_tables(ast.right_table))

        return tables

    def _extract_columns(self, ast) -> Set[str]:
        """Extract column names from AST by traversing all nodes recursively."""
        columns = set()

        if ast is None:
            return columns

        # Check direct column reference
        if hasattr(ast, 'column_name') and ast.column_name:
            columns.add(ast.column_name)

        # Check columns list (ColumnListNode)
        if hasattr(ast, 'columns'):
            col_list = ast.columns
            if isinstance(col_list, list):
                for c in col_list:
                    if isinstance(c, str) and c != '*':
                        columns.add(c)
                    elif hasattr(c, 'column_name'):
                        columns.add(c.column_name)
                    else:
                        columns.update(self._extract_columns(c))
            elif hasattr(col_list, 'columns'):
                columns.update(self._extract_columns(col_list))

        # Check target
        if hasattr(ast, 'target') and ast.target:
            columns.update(self._extract_columns(ast.target))

        # Check contents (ContainerNode, etc.)
        if hasattr(ast, 'contents'):
            for item in ast.contents:
                columns.update(self._extract_columns(item))

        # Check select_fields
        if hasattr(ast, 'select_fields'):
            for f in ast.select_fields:
                if isinstance(f, str) and f != '*':
                    columns.add(f)
                elif hasattr(f, 'column_name'):
                    columns.add(f.column_name)
                else:
                    columns.update(self._extract_columns(f))

        # Check conditions (WHERE clauses)
        if hasattr(ast, 'conditions'):
            conds = ast.conditions
            if isinstance(conds, list):
                for cond in conds:
                    columns.update(self._extract_columns(cond))
            else:
                columns.update(self._extract_columns(conds))

        # Check binary operation nodes (left/right)
        if hasattr(ast, 'left') and ast.left:
            columns.update(self._extract_columns(ast.left))
        if hasattr(ast, 'right') and ast.right:
            columns.update(self._extract_columns(ast.right))

        # Check operation node
        if hasattr(ast, 'operation') and ast.operation:
            columns.update(self._extract_columns(ast.operation))

        # Check join condition
        if hasattr(ast, 'join_condition') and ast.join_condition:
            columns.update(self._extract_columns(ast.join_condition))

        # Check function arguments
        if hasattr(ast, 'arguments'):
            for arg in ast.arguments:
                columns.update(self._extract_columns(arg))

        # Check where_conditions
        if hasattr(ast, 'where_conditions'):
            for cond in ast.where_conditions:
                if isinstance(cond, str):
                    # Simple string condition might contain column name
                    # Extract identifier-like tokens
                    pass
                else:
                    columns.update(self._extract_columns(cond))

        # Check output node
        if hasattr(ast, 'output') and ast.output:
            columns.update(self._extract_columns(ast.output))

        return columns


class SafetyViolation(Exception):
    """Raised when a query violates safety policy"""
    pass
