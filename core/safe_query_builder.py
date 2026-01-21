#!/usr/bin/env python3
"""
Safe Query Builder for SAIQL-Delta
Prevents SQL injection through parameterized queries
"""

import re
from typing import List, Dict, Any, Tuple, Optional


class SafeQueryBuilder:
    """Build safe, parameterized queries.

    Security is provided by:
    1. Parameterized queries (values passed separately, never interpolated)
    2. Identifier validation (table/column names checked against pattern)
    3. Table whitelist (optional, configure ALLOWED_TABLES)

    Supports multiple database backends via param_style:
    - 'qmark': ? placeholders (SQLite, default for CE)
    - 'format': %s placeholders (PostgreSQL, MySQL)
    """

    # Whitelist of allowed table names (configure per deployment)
    ALLOWED_TABLES = {
        'users', 'sessions', 'raw_prices', 'indicators',
        'budgets', 'trades', 'signals', 'metadata'
    }

    # Pattern for valid identifiers (alphanumeric + underscore only)
    IDENTIFIER_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')

    # Supported parameter styles
    PARAM_STYLES = {
        'qmark': '?',      # SQLite
        'format': '%s',    # PostgreSQL, MySQL
    }

    def __init__(self, param_style: str = 'qmark'):
        if param_style not in self.PARAM_STYLES:
            raise ValueError(f"Unknown param_style: {param_style}. Use 'qmark' or 'format'.")
        self.param_style = param_style
        self._placeholder = self.PARAM_STYLES[param_style]
        self.params: List[Any] = []
        self.query: str = ""

    @classmethod
    def validate_identifier(cls, identifier: str) -> bool:
        """Validate identifier against injection attacks"""
        if not identifier:
            return False

        # Check against pattern
        if not cls.IDENTIFIER_PATTERN.match(identifier):
            return False

        # Check against SQL keywords
        sql_keywords = {
            'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP',
            'CREATE', 'ALTER', 'EXEC', 'EXECUTE', 'UNION'
        }
        if identifier.upper() in sql_keywords:
            return False

        return True

    @classmethod
    def validate_table_name(cls, table: str) -> bool:
        """Validate table name is allowed"""
        if not cls.validate_identifier(table):
            return False

        # Check against whitelist if configured
        if cls.ALLOWED_TABLES and table not in cls.ALLOWED_TABLES:
            return False

        return True

    def select(
        self,
        table: str,
        columns: List[str] = None,
        where: Dict[str, Any] = None,
        order_by: str = None,
        limit: int = None
    ) -> Tuple[str, List[Any]]:
        """Build safe SELECT query"""

        # Validate table name
        if not self.validate_table_name(table):
            raise ValueError(f"Invalid table name: {table}")

        # Validate columns
        if columns:
            for col in columns:
                if not self.validate_identifier(col):
                    raise ValueError(f"Invalid column name: {col}")
            column_str = ", ".join(columns)
        else:
            column_str = "*"

        # Start query
        query = f"SELECT {column_str} FROM {table}"
        params = []

        # Add WHERE clause
        if where:
            conditions = []
            for col, value in where.items():
                if not self.validate_identifier(col):
                    raise ValueError(f"Invalid column name in WHERE: {col}")
                conditions.append(f"{col} = {self._placeholder}")
                params.append(value)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

        # Add ORDER BY
        if order_by:
            if not self.validate_identifier(order_by.replace(' DESC', '').replace(' ASC', '')):
                raise ValueError(f"Invalid ORDER BY column: {order_by}")
            query += f" ORDER BY {order_by}"

        # Add LIMIT
        if limit is not None:
            if not isinstance(limit, int) or limit < 0:
                raise ValueError(f"Invalid LIMIT value: {limit}")
            query += f" LIMIT {limit}"

        return query, params

    def insert(
        self,
        table: str,
        data: Dict[str, Any]
    ) -> Tuple[str, List[Any]]:
        """Build safe INSERT query"""

        # Validate table
        if not self.validate_table_name(table):
            raise ValueError(f"Invalid table name: {table}")

        # Validate columns
        columns = []
        values = []
        placeholders = []

        for col, value in data.items():
            if not self.validate_identifier(col):
                raise ValueError(f"Invalid column name: {col}")
            columns.append(col)
            values.append(value)
            placeholders.append(self._placeholder)

        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

        return query, values

    def update(
        self,
        table: str,
        data: Dict[str, Any],
        where: Dict[str, Any]
    ) -> Tuple[str, List[Any]]:
        """Build safe UPDATE query"""

        # Validate table
        if not self.validate_table_name(table):
            raise ValueError(f"Invalid table name: {table}")

        # Build SET clause
        set_clauses = []
        params = []

        for col, value in data.items():
            if not self.validate_identifier(col):
                raise ValueError(f"Invalid column name: {col}")
            set_clauses.append(f"{col} = {self._placeholder}")
            params.append(value)

        query = f"UPDATE {table} SET {', '.join(set_clauses)}"

        # Add WHERE clause
        if where:
            conditions = []
            for col, value in where.items():
                if not self.validate_identifier(col):
                    raise ValueError(f"Invalid column name in WHERE: {col}")
                conditions.append(f"{col} = {self._placeholder}")
                params.append(value)

            query += " WHERE " + " AND ".join(conditions)
        else:
            # Prevent accidental full-table updates
            raise ValueError("UPDATE without WHERE clause not allowed")

        return query, params

    def delete(
        self,
        table: str,
        where: Dict[str, Any]
    ) -> Tuple[str, List[Any]]:
        """Build safe DELETE query"""

        # Validate table
        if not self.validate_table_name(table):
            raise ValueError(f"Invalid table name: {table}")

        if not where:
            # Prevent accidental full-table deletes
            raise ValueError("DELETE without WHERE clause not allowed")

        # Build WHERE clause
        conditions = []
        params = []

        for col, value in where.items():
            if not self.validate_identifier(col):
                raise ValueError(f"Invalid column name in WHERE: {col}")
            conditions.append(f"{col} = {self._placeholder}")
            params.append(value)

        query = f"DELETE FROM {table} WHERE {' AND '.join(conditions)}"

        return query, params

    @staticmethod
    def escape_like_pattern(pattern: str) -> str:
        """Escape special characters in LIKE patterns"""
        # Escape % and _ characters
        pattern = pattern.replace('\\', '\\\\')
        pattern = pattern.replace('%', '\\%')
        pattern = pattern.replace('_', '\\_')
        return pattern

    def sanitize_input(self, value: Any) -> Any:
        """Sanitize input values"""
        if isinstance(value, str):
            # Remove null bytes
            value = value.replace('\x00', '')
            # Trim whitespace
            value = value.strip()

        return value


class SAIQLQueryValidator:
    """Validate SAIQL queries for safety.

    WARNING: This class provides DEFENSE-IN-DEPTH only, NOT a security boundary.

    Regex-based pattern matching can be bypassed by determined attackers.
    Do NOT rely on is_safe() as the sole protection against SQL injection.

    True security must come from:
    1. Parameterized queries (use SafeQueryBuilder)
    2. Parser-based validation (use the SAIQL parser/AST)
    3. Principle of least privilege (database permissions)

    This validator catches obvious attack patterns and is useful for:
    - Logging/alerting on suspicious queries
    - Defense-in-depth (additional layer, not primary)
    - Input sanity checking before parsing
    """

    # Dangerous patterns to check for (defense-in-depth, not security boundary)
    DANGEROUS_PATTERNS = [
        r';\s*DROP\s+TABLE',
        r';\s*DELETE\s+FROM',
        r';\s*TRUNCATE\s+TABLE',
        r';\s*ALTER\s+TABLE',
        r';\s*CREATE\s+TABLE',
        r';\s*EXEC\s*\(',
        r';\s*EXECUTE\s*\(',
        r'--\s*',  # SQL comment
        r'/\*[\s\S]*?\*/',  # Multi-line comment ([\s\S] matches any char including newlines)
        r'\bUNION\b.*\bSELECT\b',  # UNION attack
        r'\bINTO\s+OUTFILE\b',  # File write
        r'\bLOAD_FILE\s*\(',  # File read
    ]

    @classmethod
    def is_safe(cls, query: str) -> bool:
        """Check if query is safe from obvious injection patterns.

        WARNING: This is NOT a security boundary. Regex filters are bypassable.
        Use parameterized queries (SafeQueryBuilder) for actual security.
        This method is defense-in-depth only.
        """
        query_upper = query.upper()

        for pattern in cls.DANGEROUS_PATTERNS:
            if re.search(pattern, query_upper, re.IGNORECASE):
                return False

        # Check for multiple statements: reject if semicolon appears before the end
        # Strip trailing whitespace and optional single trailing semicolon, then check for any remaining semicolons
        stripped = query.strip()
        if stripped.endswith(';'):
            stripped = stripped[:-1]
        if ';' in stripped:
            return False

        return True

    @classmethod
    def validate_saiql_query(cls, query: str) -> Tuple[bool, Optional[str]]:
        """Validate SAIQL query format"""

        # Check for dangerous patterns
        if not cls.is_safe(query):
            return False, "Query contains potentially dangerous patterns"

        # Basic SAIQL syntax validation
        if not any(op in query for op in ['*', '::', '>>', '=', '+', '$']):
            return False, "Not a valid SAIQL query format"

        return True, None


if __name__ == "__main__":
    # Test the safe query builder
    builder = SafeQueryBuilder()

    # Test SELECT
    query, params = builder.select(
        'users',
        columns=['id', 'name', 'email'],
        where={'status': 'active', 'age': 25},
        order_by='name ASC',
        limit=10
    )
    print(f"SELECT Query: {query}")
    print(f"Parameters: {params}")

    # Test INSERT
    query, params = builder.insert(
        'users',
        {'name': 'Apollo', 'email': 'apollo@saiql.com', 'status': 'active'}
    )
    print(f"\nINSERT Query: {query}")
    print(f"Parameters: {params}")

    # Test validation
    validator = SAIQLQueryValidator()
    test_queries = [
        "*3[users]::name,email>>oQ",
        "SELECT * FROM users; DROP TABLE users;--",
        "*COUNT[users]::*>>oQ"
    ]

    print("\nQuery Validation:")
    for test_query in test_queries:
        is_safe, error = validator.validate_saiql_query(test_query)
        print(f"Query: {test_query[:50]}...")
        print(f"Safe: {is_safe}, Error: {error}")