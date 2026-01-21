# Utility functions


#!/usr/bin/env python3
"""
SAIQL Utilities and Helper Functions

This module provides common utility functions used throughout the SAIQL ecosystem,
including file operations, validation, formatting, performance measurement,
and SAIQL-specific helper functions.

Author: Apollo & Claude
Version: 1.0.0
Status: Foundation Phase
"""

import json
import os
import time
import re
import hashlib
import logging
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import contextmanager
import sys

# Configure logging
logger = logging.getLogger(__name__)

# Constants
SAIQL_FILE_EXTENSIONS = ['.saiql', '.lore', '.sdb']
MAX_QUERY_LENGTH = 10000
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
DEFAULT_ENCODING = 'utf-8'

class SAIQLError(Exception):
    """Base exception for SAIQL-related errors"""
    pass

class ValidationError(SAIQLError):
    """Exception for validation errors"""
    pass

class FileOperationError(SAIQLError):
    """Exception for file operation errors"""
    pass

# ============================================================================
# FILE I/O UTILITIES  
# ============================================================================

def read_file(file_path: str, encoding: str = DEFAULT_ENCODING) -> str:
    """
    Safely read a file with error handling
    
    Args:
        file_path: Path to file
        encoding: File encoding
        
    Returns:
        File contents as string
        
    Raises:
        FileOperationError: If file cannot be read
    """
    try:
        path = Path(file_path)
        
        # Validate file exists and size
        if not path.exists():
            raise FileOperationError(f"File not found: {file_path}")
        
        if path.stat().st_size > MAX_FILE_SIZE:
            raise FileOperationError(f"File too large: {file_path} (max {MAX_FILE_SIZE} bytes)")
        
        with open(path, 'r', encoding=encoding) as f:
            return f.read()
            
    except UnicodeDecodeError as e:
        raise FileOperationError(f"Encoding error reading {file_path}: {e}")
    except OSError as e:
        raise FileOperationError(f"OS error reading {file_path}: {e}")

def write_file(file_path: str, content: str, encoding: str = DEFAULT_ENCODING, 
               create_dirs: bool = True) -> bool:
    """
    Safely write content to file
    
    Args:
        file_path: Path to file
        content: Content to write
        encoding: File encoding
        create_dirs: Create parent directories if needed
        
    Returns:
        True if successful
        
    Raises:
        FileOperationError: If file cannot be written
    """
    try:
        path = Path(file_path)
        
        # Create parent directories if needed
        if create_dirs:
            path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, 'w', encoding=encoding) as f:
            f.write(content)
        
        logger.info(f"File written successfully: {file_path}")
        return True
        
    except OSError as e:
        raise FileOperationError(f"Error writing {file_path}: {e}")

def read_json_file(file_path: str) -> Dict[str, Any]:
    """
    Read and parse JSON file
    
    Args:
        file_path: Path to JSON file
        
    Returns:
        Parsed JSON data
        
    Raises:
        FileOperationError: If file cannot be read or parsed
    """
    try:
        content = read_file(file_path)
        return json.loads(content)
    except json.JSONDecodeError as e:
        raise FileOperationError(f"Invalid JSON in {file_path}: {e}")

def write_json_file(file_path: str, data: Dict[str, Any], indent: int = 2) -> bool:
    """
    Write data to JSON file
    
    Args:
        file_path: Path to JSON file
        data: Data to write
        indent: JSON indentation
        
    Returns:
        True if successful
    """
    try:
        content = json.dumps(data, indent=indent, ensure_ascii=False)
        return write_file(file_path, content)
    except (TypeError, ValueError) as e:
        raise FileOperationError(f"Error serializing JSON for {file_path}: {e}")

def backup_file(file_path: str, backup_dir: str = None) -> str:
    """
    Create backup of file with timestamp
    
    Args:
        file_path: Original file path
        backup_dir: Directory for backup (default: same as original)
        
    Returns:
        Path to backup file
    """
    path = Path(file_path)
    
    if backup_dir:
        backup_path = Path(backup_dir)
        backup_path.mkdir(parents=True, exist_ok=True)
    else:
        backup_path = path.parent
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{path.stem}_{timestamp}{path.suffix}"
    backup_file_path = backup_path / backup_name
    
    if path.exists():
        content = read_file(file_path)
        write_file(str(backup_file_path), content)
        logger.info(f"Backup created: {backup_file_path}")
    
    return str(backup_file_path)

# ============================================================================
# VALIDATION UTILITIES
# ============================================================================

def validate_saiql_query(query: str) -> Tuple[bool, Optional[str]]:
    """
    Validate SAIQL query syntax and structure
    
    Args:
        query: SAIQL query string
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not query or not isinstance(query, str):
        return False, "Query must be a non-empty string"
    
    if len(query) > MAX_QUERY_LENGTH:
        return False, f"Query too long (max {MAX_QUERY_LENGTH} characters)"
    
    # Check for balanced brackets
    brackets = {'[': ']', '{': '}', '(': ')'}
    stack = []
    
    for char in query:
        if char in brackets:
            stack.append(char)
        elif char in brackets.values():
            if not stack:
                return False, f"Unmatched closing bracket: {char}"
            if brackets[stack.pop()] != char:
                return False, f"Mismatched brackets"
    
    if stack:
        return False, f"Unmatched opening brackets: {stack}"
    
    # Check for required SAIQL operators
    if '::' not in query and '>>' not in query:
        return False, "Query missing required SAIQL operators (:: or >>)"
    
    # Check for invalid characters (basic check)
    invalid_chars = ['\n', '\r', '\t']
    for char in invalid_chars:
        if char in query:
            return False, f"Query contains invalid character: {repr(char)}"
    
    return True, None

def sanitize_query(query: str) -> str:
    """
    Sanitize SAIQL query by removing/fixing common issues
    
    Args:
        query: Raw query string
        
    Returns:
        Sanitized query string
    """
    if not query:
        return ""
    
    # Remove extra whitespace
    query = query.strip()
    query = re.sub(r'\s+', ' ', query)
    
    # Remove potentially problematic characters
    query = query.replace('\n', '').replace('\r', '').replace('\t', '')
    
    return query

def validate_file_path(file_path: str, must_exist: bool = True, 
                      allowed_extensions: List[str] = None) -> Tuple[bool, Optional[str]]:
    """
    Validate file path
    
    Args:
        file_path: Path to validate
        must_exist: Whether file must exist
        allowed_extensions: List of allowed file extensions
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if not file_path:
        return False, "File path cannot be empty"
    
    try:
        path = Path(file_path)
        
        if must_exist and not path.exists():
            return False, f"File does not exist: {file_path}"
        
        if allowed_extensions:
            if path.suffix.lower() not in [ext.lower() for ext in allowed_extensions]:
                return False, f"Invalid file extension. Allowed: {allowed_extensions}"
        
        # Check if path is within reasonable bounds (security check)
        resolved_path = path.resolve()
        if '..' in str(resolved_path):
            return False, "Path traversal not allowed"
        
        return True, None
        
    except (OSError, ValueError) as e:
        return False, f"Invalid path: {e}"

# ============================================================================
# PERFORMANCE UTILITIES
# ============================================================================

@contextmanager
def measure_time(operation_name: str = "operation", log_result: bool = True):
    """
    Context manager to measure execution time
    
    Args:
        operation_name: Name of operation being measured
        log_result: Whether to log the result
        
    Yields:
        Dictionary with timing results
    """
    start_time = time.time()
    result = {'start_time': start_time}
    
    try:
        yield result
    finally:
        end_time = time.time()
        execution_time = end_time - start_time
        result.update({
            'end_time': end_time,
            'execution_time': execution_time,
            'operation': operation_name
        })
        
        if log_result:
            logger.info(f"{operation_name} completed in {execution_time:.4f}s")

class PerformanceTracker:
    """Track performance metrics across operations"""
    
    def __init__(self):
        self.metrics = {}
        self.start_time = time.time()
    
    def record(self, operation: str, execution_time: float, 
               metadata: Dict[str, Any] = None):
        """Record performance metric"""
        if operation not in self.metrics:
            self.metrics[operation] = {
                'count': 0,
                'total_time': 0.0,
                'min_time': float('inf'),
                'max_time': 0.0,
                'avg_time': 0.0
            }
        
        metric = self.metrics[operation]
        metric['count'] += 1
        metric['total_time'] += execution_time
        metric['min_time'] = min(metric['min_time'], execution_time)
        metric['max_time'] = max(metric['max_time'], execution_time)
        metric['avg_time'] = metric['total_time'] / metric['count']
        
        if metadata:
            if 'metadata' not in metric:
                metric['metadata'] = []
            metric['metadata'].append(metadata)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get performance statistics"""
        return {
            'tracker_uptime': time.time() - self.start_time,
            'metrics': self.metrics.copy()
        }
    
    def reset(self):
        """Reset all metrics"""
        self.metrics.clear()
        self.start_time = time.time()

# ============================================================================
# FORMATTING UTILITIES
# ============================================================================

def format_query_result(data: List[Dict[str, Any]], format_type: str = 'table') -> str:
    """
    Format query results for display
    
    Args:
        data: Query result data
        format_type: Format type ('table', 'json', 'csv')
        
    Returns:
        Formatted string
    """
    if not data:
        return "No results"
    
    if format_type == 'json':
        return json.dumps(data, indent=2, default=str)
    
    elif format_type == 'csv':
        if not data:
            return ""
        
        headers = list(data[0].keys())
        csv_lines = [','.join(headers)]
        
        for row in data:
            csv_lines.append(','.join(str(row.get(h, '')) for h in headers))
        
        return '\n'.join(csv_lines)
    
    elif format_type == 'table':
        if not data:
            return "No results"
        
        headers = list(data[0].keys())
        
        # Calculate column widths
        widths = {}
        for header in headers:
            widths[header] = max(len(header), 
                               max(len(str(row.get(header, ''))) for row in data))
        
        # Build table
        lines = []
        
        # Header
        header_line = ' | '.join(h.ljust(widths[h]) for h in headers)
        lines.append(header_line)
        lines.append('-' * len(header_line))
        
        # Data rows
        for row in data:
            data_line = ' | '.join(str(row.get(h, '')).ljust(widths[h]) for h in headers)
            lines.append(data_line)
        
        return '\n'.join(lines)
    
    else:
        return str(data)

def format_execution_time(seconds: float) -> str:
    """
    Format execution time in human-readable format
    
    Args:
        seconds: Time in seconds
        
    Returns:
        Formatted time string
    """
    if seconds < 0.001:
        return f"{seconds * 1000000:.1f}μs"
    elif seconds < 1:
        return f"{seconds * 1000:.1f}ms"
    elif seconds < 60:
        return f"{seconds:.2f}s"
    else:
        minutes = int(seconds // 60)
        secs = seconds % 60
        return f"{minutes}m {secs:.1f}s"

def format_file_size(bytes_size: int) -> str:
    """
    Format file size in human-readable format
    
    Args:
        bytes_size: Size in bytes
        
    Returns:
        Formatted size string
    """
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    size = float(bytes_size)
    unit_index = 0
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    return f"{size:.1f} {units[unit_index]}"

# ============================================================================
# SAIQL-SPECIFIC UTILITIES
# ============================================================================

def extract_symbols_from_query(query: str) -> List[str]:
    """
    Extract all symbols from a SAIQL query
    
    Args:
        query: SAIQL query string
        
    Returns:
        List of unique symbols found
    """
    symbols = []
    
    # Pattern to match SAIQL symbols (letters, numbers, special chars)
    symbol_pattern = r'[*+=#@!$%^&|<>~`\'"?\\/-]+[A-Za-z0-9]*|[A-Za-z0-9]+[*+=#@!$%^&|<>~`\'"?\\/-]+'
    
    matches = re.findall(symbol_pattern, query)
    symbols.extend(matches)
    
    # Also extract container contents
    container_pattern = r'\[([^\]]+)\]'
    container_matches = re.findall(container_pattern, query)
    symbols.extend(container_matches)
    
    return list(set(symbols))  # Remove duplicates

def calculate_compression_ratio(original: str, compressed: str) -> float:
    """
    Calculate compression ratio between original and compressed text
    
    Args:
        original: Original text
        compressed: Compressed text
        
    Returns:
        Compression ratio (original_length / compressed_length)
    """
    if not compressed:
        return 0.0
    
    return len(original) / len(compressed)

def generate_query_hash(query: str) -> str:
    """
    Generate a unique hash for a query (for caching)
    
    Args:
        query: SAIQL query string
        
    Returns:
        SHA-256 hash of the query
    """
    normalized_query = sanitize_query(query).lower()
    return hashlib.sha256(normalized_query.encode()).hexdigest()[:16]

def split_query_components(query: str) -> Dict[str, str]:
    """
    Split SAIQL query into its major components
    
    Args:
        query: SAIQL query string
        
    Returns:
        Dictionary with query components
    """
    components = {
        'full_query': query,
        'operation': '',
        'target': '',
        'output': ''
    }
    
    # Split on major operators
    if '::' in query:
        parts = query.split('::', 1)
        components['operation'] = parts[0].strip()
        
        if len(parts) > 1 and '>>' in parts[1]:
            target_output = parts[1].split('>>', 1)
            components['target'] = target_output[0].strip()
            if len(target_output) > 1:
                components['output'] = target_output[1].strip()
        else:
            components['target'] = parts[1].strip() if len(parts) > 1 else ''
    
    return components

# ============================================================================
# CONFIGURATION UTILITIES
# ============================================================================

def load_config(config_path: str = "config.json") -> Dict[str, Any]:
    """
    Load configuration from JSON file
    
    Args:
        config_path: Path to configuration file
        
    Returns:
        Configuration dictionary
    """
    default_config = {
        'database': {
            'path': 'data/lore_store.db',
            'timeout': 30
        },
        'legend': {
            'path': 'data/legend_map.lore',
            'cache_symbols': True
        },
        'server': {
            'host': '0.0.0.0',
            'port': 5000,
            'debug': False
        },
        'logging': {
            'level': 'INFO',
            'file': 'saiql.log'
        }
    }
    
    if Path(config_path).exists():
        try:
            file_config = read_json_file(config_path)
            # Merge with defaults
            merge_configs(default_config, file_config)
        except Exception as e:
            logger.warning(f"Error loading config file {config_path}: {e}")
    
    return default_config

def merge_configs(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively merge configuration dictionaries
    
    Args:
        base: Base configuration
        override: Override configuration
        
    Returns:
        Merged configuration
    """
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            merge_configs(base[key], value)
        else:
            base[key] = value
    
    return base

# ============================================================================
# ERROR HANDLING UTILITIES
# ============================================================================

def safe_execute(func, *args, default_return=None, log_errors=True, **kwargs):
    """
    Safely execute a function with error handling
    
    Args:
        func: Function to execute
        *args: Function arguments
        default_return: Value to return on error
        log_errors: Whether to log errors
        **kwargs: Function keyword arguments
        
    Returns:
        Function result or default_return on error
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        if log_errors:
            logger.error(f"Error executing {func.__name__}: {e}")
        return default_return

def create_error_response(error_message: str, error_code: str = None, 
                         details: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Create standardized error response
    
    Args:
        error_message: Error message
        error_code: Optional error code
        details: Optional error details
        
    Returns:
        Error response dictionary
    """
    response = {
        'success': False,
        'error': error_message,
        'timestamp': datetime.now().isoformat()
    }
    
    if error_code:
        response['error_code'] = error_code
    
    if details:
        response['details'] = details
    
    return response

# ============================================================================
# MAIN UTILITIES FOR TESTING
# ============================================================================

def main():
    """Test helper functions"""
    print("SAIQL Utilities Test")
    print("=" * 50)
    
    # Test query validation
    test_queries = [
        "*3[users]::name,email>>oQ",
        "invalid query with unmatched [brackets",
        "=J[users+orders]::users.id=orders.user_id>>oQ",
        ""
    ]
    
    print("\nQuery Validation Tests:")
    for query in test_queries:
        is_valid, error = validate_saiql_query(query)
        status = "VALID" if is_valid else f"INVALID ({error})"
        print(f"  {query[:30]:<30} | {status}")
    
    # Test performance tracking
    print("\nPerformance Tracking Test:")
    tracker = PerformanceTracker()
    
    with measure_time("test_operation"):
        time.sleep(0.1)  # Simulate work
    
    tracker.record("test_op", 0.1, {"test": "data"})
    stats = tracker.get_stats()
    print(f"  Tracked metrics: {len(stats['metrics'])} operations")
    
    # Test formatting
    print("\nFormatting Tests:")
    test_data = [
        {"name": "Alice", "age": 30, "city": "New York"},
        {"name": "Bob", "age": 25, "city": "London"}
    ]
    
    print("  Table format:")
    table_result = format_query_result(test_data, 'table')
    for line in table_result.split('\n')[:3]:  # Show first 3 lines
        print(f"    {line}")
    
    print(f"\nTime formatting: {format_execution_time(0.123456)}")
    print(f"Size formatting: {format_file_size(1234567)}")
    
    print("\nHelper functions test completed successfully!")

if __name__ == "__main__":
    main()
