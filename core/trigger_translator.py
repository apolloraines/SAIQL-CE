#!/usr/bin/env python3
"""
SAIQL Trigger Translator (L4) - Conservative Subset

Supports minimal safe trigger patterns:
- BEFORE INSERT: Simple column normalization (UPPER/LOWER/TRIM)
- BEFORE UPDATE: Simple column normalization (UPPER/LOWER/TRIM)

Everything else generates stubs or analysis reports.
"""

import logging
import re
from typing import Tuple
from core.translator import RiskLevel

logger = logging.getLogger(__name__)


class TriggerPattern:
    """Represents a recognized trigger pattern."""
    BEFORE_INSERT_NORMALIZE = "before_insert_normalize"
    BEFORE_UPDATE_NORMALIZE = "before_update_normalize"
    UNSUPPORTED = "unsupported"


class TriggerTranslator:
    """
    Conservative trigger translator.

    Whitelist approach: Only translate explicitly supported patterns.
    """

    def __init__(self, source_dialect: str = "oracle", target_dialect: str = "postgres"):
        self.source_dialect = source_dialect.lower()
        self.target_dialect = target_dialect.lower()

    def is_supported_pattern(self, trigger_def: str) -> bool:
        """
        Check if trigger matches a supported pattern.

        Conservative: Default to False (unsupported).
        """
        pattern = self._identify_pattern(trigger_def)
        return pattern in [
            TriggerPattern.BEFORE_INSERT_NORMALIZE,
            TriggerPattern.BEFORE_UPDATE_NORMALIZE
        ]

    def translate(self, trigger_name: str, trigger_def: str) -> Tuple[str, RiskLevel]:
        """
        Translate trigger to target dialect.

        Args:
            trigger_name: Trigger name
            trigger_def: Trigger definition SQL

        Returns:
            Tuple of (translated_sql, risk_level)
        """
        pattern = self._identify_pattern(trigger_def)

        if pattern == TriggerPattern.BEFORE_INSERT_NORMALIZE:
            return self._translate_before_insert_normalize(trigger_name, trigger_def), RiskLevel.LOW
        elif pattern == TriggerPattern.BEFORE_UPDATE_NORMALIZE:
            return self._translate_before_update_normalize(trigger_name, trigger_def), RiskLevel.LOW
        else:
            raise ValueError(f"Unsupported pattern: {pattern}. Should call is_supported_pattern first.")

    def calculate_risk(self, trigger_def: str) -> RiskLevel:
        """
        Calculate risk level for trigger translation.

        Args:
            trigger_def: Trigger definition SQL

        Returns:
            Risk level
        """
        pattern = self._identify_pattern(trigger_def)

        if pattern in [TriggerPattern.BEFORE_INSERT_NORMALIZE, TriggerPattern.BEFORE_UPDATE_NORMALIZE]:
            return RiskLevel.LOW
        else:
            # Unsupported patterns are high/critical risk
            trigger_lower = trigger_def.lower()

            # Critical risk: Complex logic that affects data integrity
            if any(keyword in trigger_lower for keyword in ['cursor', 'loop', 'while', 'for']):
                return RiskLevel.CRITICAL
            elif any(keyword in trigger_lower for keyword in ['select', 'insert', 'update', 'delete', 'merge']):
                return RiskLevel.CRITICAL
            elif any(keyword in trigger_lower for keyword in ['exception', 'raise', 'rollback']):
                return RiskLevel.CRITICAL
            # High risk: Moderately complex triggers
            elif 'if' in trigger_lower or 'case' in trigger_lower:
                return RiskLevel.HIGH
            # Conservative default: Unsupported patterns are CRITICAL until proven otherwise
            else:
                return RiskLevel.CRITICAL

    def get_unsupported_reason(self, trigger_def: str) -> str:
        """
        Get reason why trigger is unsupported.

        Args:
            trigger_def: Trigger definition SQL

        Returns:
            Human-readable reason
        """
        trigger_lower = trigger_def.lower()

        if any(keyword in trigger_lower for keyword in ['cursor', 'loop', 'while', 'for']):
            return "Contains loops or cursors (not in supported subset)"
        elif 'select' in trigger_lower and 'into' in trigger_lower:
            return "Contains SELECT INTO (not in supported subset)"
        elif any(keyword in trigger_lower for keyword in ['insert', 'update', 'delete', 'merge']):
            return "Contains DML operations (not in supported subset)"
        elif any(keyword in trigger_lower for keyword in ['exception', 'raise', 'rollback']):
            return "Contains exception handling (not in supported subset)"
        elif 'after' in trigger_lower and ('insert' in trigger_lower or 'update' in trigger_lower or 'delete' in trigger_lower):
            return "AFTER triggers not in supported subset (only BEFORE INSERT/UPDATE normalization)"
        elif 'instead of' in trigger_lower:
            return "INSTEAD OF triggers not in supported subset"
        elif 'for each statement' in trigger_lower or 'statement level' in trigger_lower:
            return "Statement-level triggers not in supported subset (only row-level)"
        elif 'if' in trigger_lower or 'case' in trigger_lower:
            return "Contains conditional logic (not in supported subset)"
        else:
            return "Complex trigger pattern not in supported subset"

    def _identify_pattern(self, trigger_def: str) -> str:
        """
        Identify trigger pattern.

        Conservative pattern matching:
        - BEFORE INSERT normalization: Simple column assignments (UPPER/LOWER/TRIM)
        - BEFORE UPDATE normalization: Simple column assignments (UPPER/LOWER/TRIM)
        - Everything else: UNSUPPORTED
        """
        trigger_lower = trigger_def.lower()

        # Check for unsupported timing
        if 'after' in trigger_lower and ('insert' in trigger_lower or 'update' in trigger_lower or 'delete' in trigger_lower):
            return TriggerPattern.UNSUPPORTED
        if 'instead of' in trigger_lower:
            return TriggerPattern.UNSUPPORTED

        # Check for unsupported scope
        if 'for each statement' in trigger_lower or 'statement level' in trigger_lower:
            return TriggerPattern.UNSUPPORTED

        # Check for unsupported constructs (use word boundaries to avoid matching "FOR EACH ROW")
        # Check for loops in trigger body (not in trigger header)
        body = self._extract_trigger_body(trigger_def)
        if body:
            body_lower = body.lower()
            if any(kw in body_lower for kw in ['cursor', 'loop', 'while', 'for ', ' for(']):
                return TriggerPattern.UNSUPPORTED
            if any(kw in body_lower for kw in ['exception', 'raise', 'rollback']):
                return TriggerPattern.UNSUPPORTED
            if any(kw in body_lower for kw in ['select', 'delete', 'merge']):
                return TriggerPattern.UNSUPPORTED

        # Check for conditional logic (not supported)
        # Use word boundaries to catch IF at start of line or after newline/tab
        if re.search(r'\bif\b', trigger_lower) or re.search(r'\bcase\b', trigger_lower):
            return TriggerPattern.UNSUPPORTED

        # Check for INSERT triggers in trigger body (not allowed)
        if 'insert into' in trigger_lower:
            return TriggerPattern.UNSUPPORTED

        # Check for UPDATE triggers in trigger body (not the UPDATE event, but UPDATE statement)
        # Need to distinguish between "BEFORE UPDATE OF" and "UPDATE table_name"
        if re.search(r'update\s+\w+\s+set', trigger_lower):
            return TriggerPattern.UNSUPPORTED

        # BEFORE INSERT normalization pattern
        if 'before insert' in trigger_lower:
            if self._is_simple_normalization(trigger_def):
                return TriggerPattern.BEFORE_INSERT_NORMALIZE
            return TriggerPattern.UNSUPPORTED

        # BEFORE UPDATE normalization pattern
        if 'before update' in trigger_lower:
            if self._is_simple_normalization(trigger_def):
                return TriggerPattern.BEFORE_UPDATE_NORMALIZE
            return TriggerPattern.UNSUPPORTED

        return TriggerPattern.UNSUPPORTED

    def _is_simple_normalization(self, trigger_def: str) -> bool:
        """
        Check if trigger body contains only simple normalization operations.

        Allowed operations:
        - UPPER(:NEW.column)
        - LOWER(:NEW.column)
        - TRIM(:NEW.column)
        - LTRIM(:NEW.column)
        - RTRIM(:NEW.column)

        Not allowed:
        - Multiple statements
        - Conditional logic
        - DML operations
        - Function calls other than UPPER/LOWER/TRIM
        """
        trigger_lower = trigger_def.lower()

        # Extract trigger body (between BEGIN and END or between AS and ;)
        body = self._extract_trigger_body(trigger_def)
        if not body:
            return False

        body_lower = body.lower()

        # Check for multiple statements (semicolon-separated)
        # Split by semicolon and count non-empty statements
        statements = [s.strip() for s in body_lower.split(';') if s.strip() and s.strip() != 'end']
        if len(statements) > 1:
            return False

        # Check for conditional logic (use word boundaries)
        if re.search(r'\bif\b', body_lower) or re.search(r'\bcase\b', body_lower) or re.search(r'\bwhen\b', body_lower):
            return False

        # Check for DML operations (excluding assignment)
        # Be careful not to match UPDATE in "BEFORE UPDATE"
        if 'select' in body_lower or 'insert' in body_lower or 'delete' in body_lower or 'merge' in body_lower:
            return False

        # Check for loops
        if any(kw in body_lower for kw in ['loop', 'while', 'for ']):
            return False

        # Check that body contains assignment to :NEW.column or NEW.column
        if ':new.' not in body_lower and 'new.' not in body_lower:
            return False

        # Check that only allowed functions are used
        allowed_functions = ['upper', 'lower', 'trim', 'ltrim', 'rtrim']

        # Extract all function calls
        function_calls = re.findall(r'\b(\w+)\s*\(', body_lower)

        for func in function_calls:
            if func not in allowed_functions:
                return False

        # Check that assignment is simple (column := function(column))
        # Look for := in Oracle or = in Postgres
        if ':=' not in body and '=' not in body:
            return False

        # Must have at least one allowed function call
        if len(function_calls) == 0:
            return False

        return True

    def _extract_trigger_body(self, trigger_def: str) -> str:
        """
        Extract trigger body from trigger definition.

        Oracle: Between BEGIN and END
        Postgres: Between $$ and $$ or AS and ;
        """
        # Try BEGIN...END block (Oracle/Postgres) - preserve case
        begin_match = re.search(r'\bbegin\b(.+?)\bend\b', trigger_def, re.DOTALL | re.IGNORECASE)
        if begin_match:
            return begin_match.group(1).strip()

        # Try $$ ... $$ block (Postgres)
        dollar_match = re.search(r'\$\$(.+?)\$\$', trigger_def, re.DOTALL)
        if dollar_match:
            return dollar_match.group(1).strip()

        # Try AS ... ; (simple form)
        as_match = re.search(r'\bas\s+(.+?)(?:;|$)', trigger_def, re.DOTALL | re.IGNORECASE)
        if as_match:
            body = as_match.group(1).strip()
            # Remove trailing END if present
            body = re.sub(r'\bend\s*;?\s*$', '', body, flags=re.IGNORECASE)
            return body

        return ""

    def _translate_before_insert_normalize(self, trigger_name: str, trigger_def: str) -> str:
        """
        Translate BEFORE INSERT normalization trigger.

        Conservative approach: Keep structure, adapt syntax for target dialect.
        """
        sql = trigger_def.strip()

        if self.source_dialect == 'oracle' and self.target_dialect == 'postgres':
            # Oracle → Postgres transformations

            # Remove Oracle-specific keywords
            sql = re.sub(r'\bFORCE\b', '', sql, flags=re.IGNORECASE)
            sql = re.sub(r'\bEDITIONABLE\b', '', sql, flags=re.IGNORECASE)

            # Convert :NEW to NEW (remove colon prefix)
            sql = re.sub(r':NEW\.', 'NEW.', sql, flags=re.IGNORECASE)
            sql = re.sub(r':OLD\.', 'OLD.', sql, flags=re.IGNORECASE)

            # Convert assignment operator := to =
            sql = re.sub(r':=', '=', sql)

            # Ensure FOR EACH ROW is present
            if 'for each row' not in sql.lower():
                # Add before the function body
                sql = re.sub(
                    r'(EXECUTE\s+(?:PROCEDURE|FUNCTION))',
                    r'FOR EACH ROW \1',
                    sql,
                    flags=re.IGNORECASE
                )

            # Convert to Postgres trigger syntax if needed
            # Oracle: CREATE TRIGGER ... BEFORE INSERT ON table FOR EACH ROW BEGIN ... END;
            # Postgres: CREATE TRIGGER ... BEFORE INSERT ON table FOR EACH ROW EXECUTE FUNCTION func();

            # Check if we need to convert to function-based trigger
            if 'begin' in sql.lower() and 'execute function' not in sql.lower():
                sql = self._convert_to_postgres_function_trigger(trigger_name, sql, 'INSERT')

        return sql.strip()

    def _translate_before_update_normalize(self, trigger_name: str, trigger_def: str) -> str:
        """
        Translate BEFORE UPDATE normalization trigger.

        Conservative approach: Keep structure, adapt syntax for target dialect.
        """
        sql = trigger_def.strip()

        if self.source_dialect == 'oracle' and self.target_dialect == 'postgres':
            # Oracle → Postgres transformations (same as INSERT)

            sql = re.sub(r'\bFORCE\b', '', sql, flags=re.IGNORECASE)
            sql = re.sub(r'\bEDITIONABLE\b', '', sql, flags=re.IGNORECASE)

            sql = re.sub(r':NEW\.', 'NEW.', sql, flags=re.IGNORECASE)
            sql = re.sub(r':OLD\.', 'OLD.', sql, flags=re.IGNORECASE)

            sql = re.sub(r':=', '=', sql)

            if 'for each row' not in sql.lower():
                sql = re.sub(
                    r'(EXECUTE\s+(?:PROCEDURE|FUNCTION))',
                    r'FOR EACH ROW \1',
                    sql,
                    flags=re.IGNORECASE
                )

            # Convert to Postgres trigger syntax if needed
            if 'begin' in sql.lower() and 'execute function' not in sql.lower():
                sql = self._convert_to_postgres_function_trigger(trigger_name, sql, 'UPDATE')

        return sql.strip()

    def _convert_to_postgres_function_trigger(self, trigger_name: str, trigger_def: str, event: str) -> str:
        """
        Convert Oracle inline trigger to Postgres function-based trigger.

        Oracle: CREATE TRIGGER trg BEFORE INSERT ON tbl FOR EACH ROW BEGIN ... END;
        Postgres: CREATE FUNCTION trg_func() RETURNS trigger AS $$ BEGIN ... RETURN NEW; END; $$ LANGUAGE plpgsql;
                  CREATE TRIGGER trg BEFORE INSERT ON tbl FOR EACH ROW EXECUTE FUNCTION trg_func();
        """
        # Extract trigger body
        body = self._extract_trigger_body(trigger_def)

        # Convert :NEW to NEW, :OLD to OLD (preserve uppercase per Postgres convention)
        body = re.sub(r':NEW\.', 'NEW.', body, flags=re.IGNORECASE)
        body = re.sub(r':OLD\.', 'OLD.', body, flags=re.IGNORECASE)

        # Convert assignment operator := to =
        body = re.sub(r':=', '=', body)

        # Extract table name
        table_match = re.search(r'\bon\s+(\w+)', trigger_def, re.IGNORECASE)
        table_name = table_match.group(1) if table_match else 'unknown_table'

        # Build Postgres function
        func_name = f"{trigger_name}_func"

        # Strip trailing semicolons from body to avoid ;; in generated code
        body = body.rstrip().rstrip(';').rstrip()

        postgres_sql = f"""-- Trigger function for {trigger_name}
CREATE OR REPLACE FUNCTION {func_name}()
RETURNS trigger AS $$
BEGIN
    {body};
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger
CREATE TRIGGER {trigger_name}
BEFORE {event} ON {table_name}
FOR EACH ROW
EXECUTE FUNCTION {func_name}();
"""

        return postgres_sql
