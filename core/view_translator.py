#!/usr/bin/env python3
"""
SAIQL View Translator (L2) - Conservative Subset

Supports minimal safe view patterns:
- Simple SELECT + projection
- SELECT + WHERE (simple predicates)
- Basic INNER JOIN (2 tables max)

Everything else generates stubs or analysis reports.
"""

import logging
import re
from typing import Tuple
from core.translator import RiskLevel

logger = logging.getLogger(__name__)


class ViewPattern:
    """Represents a recognized view pattern."""
    SIMPLE_SELECT = "simple_select"
    SELECT_WHERE = "select_where"
    BASIC_JOIN = "basic_join"
    UNSUPPORTED = "unsupported"


class ViewTranslator:
    """
    Conservative view translator.

    Whitelist approach: Only translate explicitly supported patterns.
    """

    def __init__(self, source_dialect: str = "oracle", target_dialect: str = "postgres"):
        self.source_dialect = source_dialect.lower()
        self.target_dialect = target_dialect.lower()

    def is_supported_pattern(self, view_def: str) -> bool:
        """
        Check if view matches a supported pattern.

        Conservative: Default to False (unsupported).
        """
        pattern = self._identify_pattern(view_def)
        return pattern in [ViewPattern.SIMPLE_SELECT, ViewPattern.SELECT_WHERE, ViewPattern.BASIC_JOIN]

    def translate(self, view_name: str, view_def: str) -> Tuple[str, RiskLevel]:
        """
        Translate view to target dialect.

        Args:
            view_name: View name
            view_def: View definition SQL

        Returns:
            Tuple of (translated_sql, risk_level)
        """
        pattern = self._identify_pattern(view_def)

        if pattern == ViewPattern.SIMPLE_SELECT:
            return self._translate_simple_select(view_name, view_def), RiskLevel.SAFE
        elif pattern == ViewPattern.SELECT_WHERE:
            return self._translate_select_where(view_name, view_def), RiskLevel.LOW
        elif pattern == ViewPattern.BASIC_JOIN:
            return self._translate_basic_join(view_name, view_def), RiskLevel.MEDIUM
        else:
            raise ValueError(f"Unsupported pattern: {pattern}. Should call is_supported_pattern first.")

    def calculate_risk(self, view_def: str) -> RiskLevel:
        """
        Calculate risk level for view translation.

        Args:
            view_def: View definition SQL

        Returns:
            Risk level
        """
        pattern = self._identify_pattern(view_def)

        if pattern == ViewPattern.SIMPLE_SELECT:
            return RiskLevel.SAFE
        elif pattern == ViewPattern.SELECT_WHERE:
            return RiskLevel.LOW
        elif pattern == ViewPattern.BASIC_JOIN:
            return RiskLevel.MEDIUM
        else:
            # Unsupported patterns are high/critical risk
            view_lower = view_def.lower()
            if any(keyword in view_lower for keyword in ['window', 'partition by', 'over(']):
                return RiskLevel.CRITICAL
            elif any(keyword in view_lower for keyword in ['union', 'intersect', 'except', 'cte', 'with']):
                return RiskLevel.CRITICAL
            elif 'subquery' in view_lower or view_lower.count('select') > 1:
                return RiskLevel.HIGH
            else:
                return RiskLevel.HIGH

    def get_unsupported_reason(self, view_def: str) -> str:
        """
        Get reason why view is unsupported.

        Args:
            view_def: View definition SQL

        Returns:
            Human-readable reason
        """
        view_lower = view_def.lower()

        if any(keyword in view_lower for keyword in ['window', 'partition by']) or re.search(r'\bover\s*\(', view_lower):
            return "Contains window functions (not in supported subset)"
        elif 'union' in view_lower:
            return "Contains UNION (not in supported subset)"
        elif 'intersect' in view_lower or 'except' in view_lower:
            return "Contains set operations (not in supported subset)"
        elif 'with' in view_lower and 'recursive' not in view_lower:
            return "Contains CTE/WITH clause (not in supported subset)"
        elif view_lower.count('select') > 1:
            return "Contains subqueries (not in supported subset)"
        elif 'left join' in view_lower or 'right join' in view_lower or 'full join' in view_lower:
            return "Contains outer joins (not in supported subset)"
        elif 'cross join' in view_lower:
            return "Contains cross join (not in supported subset)"
        else:
            return "Complex pattern not in supported subset"

    def _identify_pattern(self, view_def: str) -> str:
        """
        Identify view pattern.

        Conservative pattern matching:
        - Simple SELECT: Single table, column projection only
        - SELECT + WHERE: Single table, simple WHERE clause
        - Basic JOIN: 2 tables, INNER JOIN, simple ON clause
        - Everything else: UNSUPPORTED
        """
        view_lower = view_def.lower()

        # Check for unsupported constructs first
        unsupported_keywords = [
            'union', 'intersect', 'except', 'window', 'partition by',
            'left join', 'right join', 'full join', 'cross join',
            'with', 'materialized', 'distinct on'
        ]
        if any(kw in view_lower for kw in unsupported_keywords):
            return ViewPattern.UNSUPPORTED

        # Check for window functions (OVER with or without space before parenthesis)
        if re.search(r'\bover\s*\(', view_lower):
            return ViewPattern.UNSUPPORTED

        # Check for subqueries (multiple SELECT statements)
        if view_lower.count('select') > 1:
            return ViewPattern.UNSUPPORTED

        # Check for unsupported SQL clauses
        if 'group by' in view_lower:
            return ViewPattern.UNSUPPORTED
        if 'having' in view_lower:
            return ViewPattern.UNSUPPORTED
        if 'order by' in view_lower:
            return ViewPattern.UNSUPPORTED

        # Check for aggregates (reject all for now - conservative)
        if any(kw in view_lower for kw in ['count(', 'sum(', 'avg(', 'max(', 'min(']):
            return ViewPattern.UNSUPPORTED

        # Check for DISTINCT (conservative)
        if 'distinct' in view_lower:
            return ViewPattern.UNSUPPORTED

        # Check for joins
        if 'join' in view_lower:
            # Only support INNER JOIN with 2 tables
            if 'inner join' in view_lower:
                # Count number of FROM + JOIN clauses (should be 2 for 2 tables)
                from_count = view_lower.count('from')
                join_count = view_lower.count('join')
                if from_count == 1 and join_count == 1:
                    # Validate ON clause is equality-only
                    if not self._has_equality_only_on_clause(view_def):
                        return ViewPattern.UNSUPPORTED
                    return ViewPattern.BASIC_JOIN
            return ViewPattern.UNSUPPORTED

        # Check for WHERE clause
        if 'where' in view_lower:
            # Simple WHERE with single table
            if 'from' in view_lower:
                # Make sure no subqueries in WHERE
                where_section = view_lower.split('where')[1] if 'where' in view_lower else ''
                if 'select' not in where_section:
                    return ViewPattern.SELECT_WHERE
            return ViewPattern.UNSUPPORTED

        # Simple SELECT (no WHERE, no JOIN)
        if 'select' in view_lower and 'from' in view_lower:
            # Check for computed columns (expressions in SELECT)
            if self._has_computed_columns(view_def):
                return ViewPattern.UNSUPPORTED
            return ViewPattern.SIMPLE_SELECT

        return ViewPattern.UNSUPPORTED

    def _has_computed_columns(self, view_def: str) -> bool:
        """
        Check if view has computed columns (expressions, not just simple column references).

        Conservative check: Look for operators and functions in SELECT clause.
        """
        view_lower = view_def.lower()

        # Extract SELECT clause (between SELECT and FROM)
        if 'select' not in view_lower or 'from' not in view_lower:
            return False

        select_start = view_lower.find('select') + 6
        from_pos = view_lower.find('from')
        select_clause = view_lower[select_start:from_pos].strip()

        # Check for operators (with or without spaces)
        # Use regex to catch operators even without surrounding spaces

        # First, remove wildcard patterns (*, t.*, schema.t.*) before checking for arithmetic *
        # These are valid projections, not multiplication
        select_no_wildcards = re.sub(r'(?:^|,\s*)(?:\w+\.)*\*(?:\s*,|\s*$)', ' ', select_clause)
        select_no_wildcards = re.sub(r'(?:^|,\s*)\*(?:\s*,|\s*$)', ' ', select_no_wildcards)

        # Arithmetic operators: +, -, *, /
        if re.search(r'[+\-*/]', select_no_wildcards):
            return True

        # String concatenation: ||
        if '||' in select_clause:
            return True

        # Check for CASE expressions (special handling - not followed by paren)
        if re.search(r'\bcase\s+when\b', select_clause):
            return True

        # Functions that indicate computed columns
        computed_functions = [
            'cast', 'concat', 'coalesce', 'nvl', 'ifnull',
            'substr', 'substring', 'trim', 'ltrim', 'rtrim',
            'upper', 'lower', 'initcap',
            'extract', 'date_part', 'to_char', 'to_date',
            'round', 'trunc', 'floor', 'ceil',
            'length', 'char_length',
            'replace', 'translate'
        ]

        for func in computed_functions:
            # Check for function calls (function name followed by opening paren)
            if re.search(rf'\b{func}\s*\(', select_clause):
                return True

        return False

    def _has_equality_only_on_clause(self, view_def: str) -> bool:
        """
        Check if JOIN ON clause contains only equality predicates.

        Conservative: Reject any non-equality operators.
        """
        view_lower = view_def.lower()

        # Extract ON clause (between ON and next keyword or end)
        if ' on ' not in view_lower:
            return False  # No ON clause found

        on_pos = view_lower.find(' on ')
        after_on = view_lower[on_pos + 4:]

        # Find end of ON clause (next WHERE, GROUP, HAVING, ORDER, or end)
        end_keywords = ['where', 'group by', 'having', 'order by', 'union', 'limit']
        end_pos = len(after_on)
        for keyword in end_keywords:
            pos = after_on.find(keyword)
            if pos != -1 and pos < end_pos:
                end_pos = pos

        on_clause = after_on[:end_pos].strip()

        # Check for non-equality operators (simple string matches)
        simple_non_equality = ['>', '<', '>=', '<=', '!=', '<>']
        for op in simple_non_equality:
            if op in on_clause:
                return False

        # Check for keyword-based non-equality operators using word boundaries
        # This catches IN/NOT IN with or without whitespace before parenthesis
        if re.search(r'\bbetween\b', on_clause):
            return False
        if re.search(r'\blike\b', on_clause):
            return False
        if re.search(r'\bnot\s+in\b', on_clause):
            return False
        if re.search(r'\bin\s*\(', on_clause):
            return False

        # Must have at least one = (equality)
        if '=' not in on_clause:
            return False

        return True

    def _translate_simple_select(self, view_name: str, view_def: str) -> str:
        """
        Translate simple SELECT + projection.

        Minimal transformation: mostly syntax adjustments.
        """
        # For now, simple passthrough with CREATE VIEW wrapper
        # In real implementation, would handle dialect-specific syntax
        sql = view_def.strip()

        # Remove Oracle-specific keywords if present
        sql = re.sub(r'\bFORCE\b', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bEDITIONABLE\b', '', sql, flags=re.IGNORECASE)

        # Ensure proper CREATE VIEW syntax
        if not sql.upper().startswith('CREATE'):
            sql = f"CREATE VIEW {view_name} AS\n{sql}"

        return sql.strip()

    def _translate_select_where(self, view_name: str, view_def: str) -> str:
        """
        Translate SELECT + WHERE.

        Handle boolean literals (Oracle 1/0 → Postgres true/false).
        """
        sql = view_def.strip()

        # Remove Oracle-specific keywords
        sql = re.sub(r'\bFORCE\b', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bEDITIONABLE\b', '', sql, flags=re.IGNORECASE)

        # Boolean translation (Oracle → Postgres)
        if self.source_dialect == 'oracle' and self.target_dialect == 'postgres':
            # Replace = 1 with = true, = 0 with = false
            sql = re.sub(r'=\s*1\b', '= true', sql)
            sql = re.sub(r'=\s*0\b', '= false', sql)

        # Ensure proper CREATE VIEW syntax
        if not sql.upper().startswith('CREATE'):
            sql = f"CREATE VIEW {view_name} AS\n{sql}"

        return sql.strip()

    def _translate_basic_join(self, view_name: str, view_def: str) -> str:
        """
        Translate basic INNER JOIN (2 tables).

        Minimal transformation for simple joins.
        """
        sql = view_def.strip()

        # Remove Oracle-specific keywords
        sql = re.sub(r'\bFORCE\b', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\bEDITIONABLE\b', '', sql, flags=re.IGNORECASE)

        # Oracle uses (+) for outer joins - make sure we don't have them
        if '(+)' in sql:
            raise ValueError("Oracle outer join syntax (+) detected - not supported")

        # Ensure proper CREATE VIEW syntax
        if not sql.upper().startswith('CREATE'):
            sql = f"CREATE VIEW {view_name} AS\n{sql}"

        return sql.strip()
