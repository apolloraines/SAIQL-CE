#!/usr/bin/env python3
"""
SAIQL Operators - Consolidated Operator Implementations
========================================================

This module contains all SAIQL operator implementations, consolidated from
multiple sources to provide a single, canonical set of operators.

Operators are organized by category:
- Query operators: SELECT, output redirection
- Comparison operators: =, ==, >, <, >=, <=, !=
- Logical operators: &, &&, ||, !
- Arithmetic operators: +, -, *, /, %
- String operators: LIKE, CONCAT, UPPER, LOWER, TRIM
- Aggregate functions: COUNT, SUM, AVG, MIN, MAX, DISTINCT
- Date/time functions: NOW, DATE, YEAR, MONTH, DAY
- Advanced operators: SIMILAR, RECENT, IN, BETWEEN

Author: Apollo & Claude
Version: 1.0.0 (Consolidated)

Change Notes (2026-01-20):
- Fixed execute_distinct() to preserve insertion order using dict.fromkeys()
"""

import re
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from core.logging import logger

@dataclass
class ExecutionContext:
    """Enhanced runtime execution context"""
    variables: Dict[str, Any]
    database_connection: Any = None
    query_cache: Dict[str, Any] = None
    performance_stats: Dict[str, float] = None
    
    def __post_init__(self):
        if self.query_cache is None:
            self.query_cache = {}
        if self.performance_stats is None:
            self.performance_stats = {"queries_executed": 0, "total_time": 0.0}


class SAIQLOperators:
    """
    SAIQL Operators - Comprehensive operator implementations
    
    This class provides all SAIQL operators in one place, consolidating
    functionality from core_runtime.py and production_runtime.py.
    """
    
    def __init__(self):
        self.context = ExecutionContext(variables={})
        
        # Register all operators for dynamic dispatch
        self.operators = {
            # Query operators
            '*': self.execute_select,
            'SELECT': self.execute_select,  # Alias for API compatibility
            '>>': self.execute_output_redirect,
            'oQ': self.execute_output_query,
            '::': self.execute_column_selector,
            '|': self.execute_filter,
            
            # Comparison operators
            '=': self.execute_equals,
            '==': self.execute_strict_equals,
            '!=': self.execute_not_equals,
            '>': self.execute_greater_than,
            '<': self.execute_less_than,
            '>=': self.execute_greater_equal,
            '<=': self.execute_less_equal,
            
            # Logical operators
            '&': self.execute_logical_and,
            '&&': self.execute_logical_and,  # Alias
            '||': self.execute_logical_or,
            '!': self.execute_logical_not,
            
            # Arithmetic operators
            '+': self.execute_addition,
            '-': self.execute_subtraction,
            '*_mul': self.execute_multiplication,  # Avoid conflict with SELECT *
            '/': self.execute_division,
            '%': self.execute_modulo,
            
            # String functions
            'LIKE': self.execute_like,
            'CONCAT': self.execute_concat,
            'UPPER': self.execute_upper,
            'LOWER': self.execute_lower,
            'TRIM': self.execute_trim,
            
            # Aggregate functions
            'COUNT': self.execute_count,
            'SUM': self.execute_sum,
            'AVG': self.execute_average,
            'MIN': self.execute_min,
            'MAX': self.execute_max,
            'DISTINCT': self.execute_distinct,
            
            # Date/time functions
            'NOW': self.execute_now,
            'DATE': self.execute_date,
            'YEAR': self.execute_year,
            'MONTH': self.execute_month,
            'DAY': self.execute_day,
            'RECENT': self.execute_recent_filter,
            
            # Advanced operators
            'SIMILAR': self.execute_similarity,
            'IS': self.execute_is,
            'IN': self.execute_in,
            'BETWEEN': self.execute_between,
            
            # CRUD operations
            'INSERT': self.execute_insert,
            'UPDATE': self.execute_update,
            'DELETE': self.execute_delete,
            
            # Ordering/Limiting
            'ORDER': self.execute_order_by,
            'LIMIT': self.execute_limit,
        }
    
    def execute_operator(self, operator: str, *args):
        """Execute an operator with performance tracking"""
        import time
        start_time = time.time()
        
        try:
            if operator not in self.operators:
                raise ValueError(f"Unknown operator: {operator}")
            
            result = self.operators[operator](*args)
            
            # Track performance
            execution_time = time.time() - start_time
            self.context.performance_stats["queries_executed"] += 1
            self.context.performance_stats["total_time"] += execution_time
            
            return result
            
        except Exception as e:
            logger.error(f"Error executing operator {operator}: {e}")
            raise
    
    # ========================================================================
    # QUERY OPERATORS
    # ========================================================================
    
    def execute_select(self, limit: Optional[int] = None, table: str = None):
        """Execute SELECT operation (*)"""
        logger.debug(f"SELECT operation: limit={limit}, table={table}")
        return {
            "operation": "SELECT",
            "limit": limit,
            "table": table,
            "status": "prepared"
        }
    
    def execute_output_redirect(self, target: str):
        """Execute output redirect (>>)"""
        logger.debug(f"Output redirect to: {target}")
        return {
            "operation": "OUTPUT_REDIRECT",
            "target": target,
            "status": "redirected"
        }
    
    def execute_output_query(self):
        """Execute output query (oQ)"""
        logger.debug("Output query format requested")
        return {
            "operation": "OUTPUT_QUERY",
            "format": "query_result",
            "status": "formatted"
        }
    
    def execute_column_selector(self, columns):
        """Execute column selection (::)"""
        logger.debug(f"Column selector: {columns}")
        return {
            "operation": "COLUMN_SELECT",
            "columns": columns if isinstance(columns, list) else [columns],
            "status": "selected"
        }
    
    def execute_filter(self, condition: Any):
        """Execute filter operation (|)"""
        logger.debug(f"Filter condition: {condition}")
        return {
            "operation": "FILTER",
            "condition": condition,
            "status": "filtered"
        }
    
    # ========================================================================
    # COMPARISON OPERATORS
    # ========================================================================
    
    def execute_equals(self, left: Any, right: Any) -> bool:
        """Execute equals comparison (=)

        Comparison semantics:
        - Both numeric (int, float, or numeric strings): compare as floats
        - Both strings (non-numeric): compare case-insensitively
        - Mixed or other types: compare directly with ==
        """
        def is_numeric(val: Any) -> bool:
            if isinstance(val, bool):
                return False  # Treat bools as non-numeric
            if isinstance(val, (int, float)):
                return True
            if isinstance(val, str):
                try:
                    float(val)
                    return True
                except ValueError:
                    return False
            return False

        left_numeric = is_numeric(left)
        right_numeric = is_numeric(right)

        if left_numeric and right_numeric:
            # Both numeric: compare as floats
            return float(left) == float(right)

        if isinstance(left, str) and isinstance(right, str):
            # Both strings: case-insensitive comparison
            return left.lower() == right.lower()

        # Mixed types or other: direct comparison
        return left == right
    
    def execute_strict_equals(self, left: Any, right: Any) -> bool:
        """Execute strict equals comparison (==)"""
        # Type-specific comparison
        return left == right and type(left) == type(right)
    
    def execute_not_equals(self, left: Any, right: Any) -> bool:
        """Execute not equals comparison (!=)"""
        return left != right
    
    def execute_greater_than(self, left: Any, right: Any) -> bool:
        """Execute greater than comparison (>)"""
        try:
            return float(left) > float(right)
        except (ValueError, TypeError):
            return left > right
    
    def execute_less_than(self, left: Any, right: Any) -> bool:
        """Execute less than comparison (<)"""
        try:
            return float(left) < float(right)
        except (ValueError, TypeError):
            return left < right
    
    def execute_greater_equal(self, left: Any, right: Any) -> bool:
        """Execute greater or equal comparison (>=)"""
        try:
            return float(left) >= float(right)
        except (ValueError, TypeError):
            return left >= right
    
    def execute_less_equal(self, left: Any, right: Any) -> bool:
        """Execute less or equal comparison (<=)"""
        try:
            return float(left) <= float(right)
        except (ValueError, TypeError):
            return left <= right
    
    # ========================================================================
    # LOGICAL OPERATORS
    # ========================================================================
    
    def execute_logical_and(self, *operands) -> bool:
        """Execute logical AND (&, &&)"""
        return all(operands)
    
    def execute_logical_or(self, *operands) -> bool:
        """Execute logical OR (||)"""
        return any(operands)
    
    def execute_logical_not(self, operand: Any) -> bool:
        """Execute logical NOT (!)"""
        return not operand
    
    # ========================================================================
    # ARITHMETIC OPERATORS
    # ========================================================================
    
    def execute_addition(self, left: Any, right: Any) -> Union[int, float]:
        """Execute addition (+)"""
        try:
            return float(left) + float(right)
        except (ValueError, TypeError):
            # String concatenation fallback
            return str(left) + str(right)
    
    def execute_subtraction(self, left: Any, right: Any) -> Union[int, float]:
        """Execute subtraction (-)"""
        return float(left) - float(right)
    
    def execute_multiplication(self, left: Union[int, float], right: Union[int, float]) -> Union[int, float]:
        """Execute multiplication (*)"""
        return float(left) * float(right)
    
    def execute_division(self, left: Union[int, float], right: Union[int, float]) -> float:
        """Execute division (/)"""
        if right == 0:
            raise ZeroDivisionError("Division by zero")
        return float(left) / float(right)
    
    def execute_modulo(self, left: Union[int, float], right: Union[int, float]) -> Union[int, float]:
        """Execute modulo (%)"""
        return float(left) % float(right)
    
    # ========================================================================
    # STRING FUNCTIONS
    # ========================================================================
    
    def execute_like(self, text: str, pattern: str) -> bool:
        """Execute LIKE pattern matching (SQL semantics)"""
        # First escape all regex metacharacters, then convert SQL wildcards
        # This ensures patterns like "test.file" match literally, not as regex
        escaped = re.escape(pattern)
        # Convert SQL wildcards: % = 0+ chars, _ = exactly 1 char
        # re.escape turns % into \% and _ into \_, so we replace those
        regex_pattern = escaped.replace(r'\%', '.*').replace(r'\_', '.')
        # Use fullmatch for complete string matching (SQL LIKE matches entire string)
        return bool(re.fullmatch(regex_pattern, str(text), re.IGNORECASE))
    
    def execute_concat(self, *strings) -> str:
        """Execute string concatenation"""
        return ''.join(str(s) for s in strings)
    
    def execute_upper(self, text: str) -> str:
        """Execute UPPER string conversion"""
        return str(text).upper()
    
    def execute_lower(self, text: str) -> str:
        """Execute LOWER string conversion"""
        return str(text).lower()
    
    def execute_trim(self, text: str) -> str:
        """Execute TRIM whitespace removal"""
        return str(text).strip()
    
    # ========================================================================
    # AGGREGATE FUNCTIONS
    # ========================================================================
    
    def execute_count(self, data: List[Any]) -> int:
        """Execute COUNT aggregate"""
        if not isinstance(data, list):
            return 1
        return len(data)
    
    def execute_sum(self, data: List[Any]) -> Union[int, float]:
        """Execute SUM aggregate"""
        if not isinstance(data, list):
            data = [data]
        return sum(float(x) for x in data if x is not None)
    
    def execute_average(self, data: List[Any]) -> float:
        """Execute AVG aggregate"""
        if not isinstance(data, list):
            data = [data]
        numbers = [float(x) for x in data if x is not None]
        return sum(numbers) / len(numbers) if numbers else 0.0
    
    def execute_min(self, data: List[Union[int, float]]) -> Union[int, float]:
        """Execute MIN aggregate"""
        if not data:
            return None
        numbers = [float(x) for x in data if x is not None]
        return min(numbers) if numbers else None
    
    def execute_max(self, data: List[Union[int, float]]) -> Union[int, float]:
        """Execute MAX aggregate"""
        if not data:
            return None
        numbers = [float(x) for x in data if x is not None]
        return max(numbers) if numbers else None
    
    def execute_distinct(self, data: List[Any]) -> List[Any]:
        """Execute DISTINCT (remove duplicates while preserving insertion order)"""
        # Use dict.fromkeys() to preserve insertion order (Python 3.7+)
        return list(dict.fromkeys(data))
    
    # ========================================================================
    # DATE/TIME FUNCTIONS
    # ========================================================================
    
    def execute_now(self) -> datetime:
        """Execute NOW function"""
        return datetime.now()

    def execute_date(self, datetime_obj: Union[datetime, date]) -> date:
        """Execute DATE function - returns date part"""
        if isinstance(datetime_obj, datetime):
            return datetime_obj.date()
        elif isinstance(datetime_obj, date):
            return datetime_obj
        return datetime.now().date()

    def execute_year(self, datetime_obj: Union[datetime, date]) -> int:
        """Execute YEAR function - returns year as scalar"""
        if isinstance(datetime_obj, (datetime, date)):
            return datetime_obj.year
        return datetime.now().year

    def execute_month(self, datetime_obj: Union[datetime, date]) -> int:
        """Execute MONTH function - returns month as scalar"""
        if isinstance(datetime_obj, (datetime, date)):
            return datetime_obj.month
        return datetime.now().month

    def execute_day(self, datetime_obj: Union[datetime, date]) -> int:
        """Execute DAY function - returns day as scalar"""
        if isinstance(datetime_obj, (datetime, date)):
            return datetime_obj.day
        return datetime.now().day
    
    def execute_recent_filter(self, timeframe: str) -> Dict[str, Any]:
        """Execute recent time filter (RECENT)"""
        # Parse timeframe like "7d", "2h", "30m"
        match = re.match(r'(\d+)([dhm])', timeframe.lower())
        
        if not match:
            raise ValueError(f"Invalid timeframe format: {timeframe}")
        
        amount, unit = int(match.group(1)), match.group(2)
        
        if unit == 'd':
            cutoff = datetime.now() - timedelta(days=amount)
        elif unit == 'h':
            cutoff = datetime.now() - timedelta(hours=amount)
        elif unit == 'm':
            cutoff = datetime.now() - timedelta(minutes=amount)
        else:
            raise ValueError(f"Unknown time unit: {unit}")
        
        return {
            "operation": "RECENT_FILTER",
            "timeframe": timeframe,
            "cutoff": cutoff,
            "status": "filtered"
        }
    
    # ========================================================================
    # ADVANCED OPERATORS
    # ========================================================================
    
    def execute_similarity(self, text: str, pattern: str, threshold: float = 0.7) -> bool:
        """Execute similarity search (SIMILAR)"""
        # Simple similarity based on common characters
        text_set = set(text.lower())
        pattern_set = set(pattern.lower())
        
        if not pattern_set:
            return False
        
        intersection = text_set & pattern_set
        similarity = len(intersection) / len(pattern_set)
        
        return similarity >= threshold
    
    def execute_is(self, left: Any, right: Any) -> bool:
        """Execute IS comparison (for NULL checks)"""
        # IS checks identity, especially for NULL
        if right is None or (isinstance(right, str) and right.upper() == 'NULL'):
            return left is None
        return left is right
    
    def execute_in(self, value: Any, collection: List[Any]) -> bool:
        """Execute IN membership test"""
        return value in collection
    
    def execute_between(self, value: Union[int, float], lower: Union[int, float], upper: Union[int, float]) -> bool:
        """Execute BETWEEN range check"""
        # Cast all operands to float for consistent comparison
        return float(lower) <= float(value) <= float(upper)
    
    # ========================================================================
    # CRUD OPERATIONS
    # ========================================================================
    
    def execute_insert(self, table: str, values: Dict[str, Any]) -> Dict[str, Any]:
        """Execute INSERT operation"""
        logger.debug(f"INSERT into {table}: {values}")
        return {
            "operation": "INSERT",
            "table": table,
            "values": values,
            "status": "prepared"
        }
    
    def execute_update(self, table: str, set_clause: Dict[str, Any], where_clause: str = None) -> Dict[str, Any]:
        """Execute UPDATE operation"""
        logger.debug(f"UPDATE {table} SET {set_clause} WHERE {where_clause}")
        return {
            "operation": "UPDATE",
            "table": table,
            "set": set_clause,
            "where": where_clause,
            "status": "prepared"
        }
    
    def execute_delete(self, table: str, where_clause: str = None) -> Dict[str, Any]:
        """Execute DELETE operation"""
        logger.debug(f"DELETE FROM {table} WHERE {where_clause}")
        return {
            "operation": "DELETE",
            "table": table,
            "where": where_clause,
            "status": "prepared"
        }
    
    # ========================================================================
    # ORDERING/LIMITING
    # ========================================================================
    
    def execute_order_by(self, column: str, direction: str = "ASC") -> Dict[str, Any]:
        """Execute ORDER BY"""
        logger.debug(f"ORDER BY {column} {direction}")
        return {
            "operation": "ORDER_BY",
            "column": column,
            "direction": direction.upper(),
            "status": "prepared"
        }
    
    def execute_limit(self, count: int) -> Dict[str, Any]:
        """Execute LIMIT"""
        logger.debug(f"LIMIT {count}")
        return {
            "operation": "LIMIT",
            "count": int(count),
            "status": "prepared"
        }
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def get_performance_stats(self) -> Dict[str, float]:
        """Get runtime performance statistics"""
        stats = self.context.performance_stats.copy()
        
        if stats["queries_executed"] > 0:
            stats["average_time"] = stats["total_time"] / stats["queries_executed"]
        else:
            stats["average_time"] = 0.0
        
        return stats


# Export for easy import
__all__ = ['SAIQLOperators', 'ExecutionContext']


def main():
    """Test SAIQL operators"""
    print("SAIQL Operators Test")
    print("=" * 50)
    
    operators = SAIQLOperators()
    
    # Test cases
    tests = [
        ("Equals", "=", [5, "5"]),
        ("Greater Than", ">", [10, 5]),
        ("Logical AND", "&", [True, True, False]),
        ("Addition", "+", [5, 3]),
        ("LIKE", "LIKE", ["hello", "he%"]),
        ("COUNT", "COUNT", [[1, 2, 3, 4, 5]]),
        ("UPPER", "UPPER", ["hello world"]),
        ("IN", "IN", [3, [1, 2, 3, 4]]),
        ("BETWEEN", "BETWEEN", [5, 1, 10]),
    ]
    
    print("\nRunning operator tests:\n")
    
    for name, operator, args in tests:
        try:
            result = operators.execute_operator(operator, *args)
            print(f"[OK] {name:15} | {operator:10} | Args: {args} | Result: {result}")
        except Exception as e:
            print(f"[FAIL] {name:15} | {operator:10} | ERROR: {e}")
    
    # Show performance stats
    print(f"\nPerformance Statistics:")
    stats = operators.get_performance_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print(f"\nTotal operators available: {len(operators.operators)}")


if __name__ == "__main__":
    main()
# IHNpZ25hdHVyZTplYzUyNTBkZWRmNTZjZTZiMDA4NCBpZDpTQUlRTC1DSEFSTElFLVBST0QgYnk6QXBvbGxvICYgQ2xhdWRlIA==
