#!/usr/bin/env python3
"""
SAIQL Community Edition - Production Entry Point
This is the canonical way to use SAIQL CE programmatically

Also supports command-line usage:
    python saiql.py "SELECT * FROM users"
    python saiql.py --interactive
    python saiql.py --help

Note: Atlas, QIPI, and LoreToken are not available in Community Edition.
"""

from pathlib import Path
from typing import Optional, Dict, Any, List
import sys
import argparse
import json
import logging

from core.engine import SAIQLEngine
from core.runtime import ExecutionContext
from config.secure_config import get_config

logger = logging.getLogger(__name__)

# CE Edition identifier
SAIQL_EDITION = "ce"
SAIQL_VERSION = "1.0.0-ce"


class SAIQL:
    """
    Blessed API for SAIQL Community Edition

    This is the recommended way to use SAIQL in your applications.

    Example:
        >>> from saiql import SAIQL
        >>>
        >>> # Initialize engine
        >>> db = SAIQL()
        >>>
        >>> # Execute queries
        >>> result = db.execute("SELECT * FROM users WHERE age > 25")
        >>> print(result)
        >>>
        >>> # Use symbolic SAIQL syntax
        >>> result = db.execute("USER.PROFILE:[name>>John>>age>>30,ACTIVE]")
        >>>
        >>> # Close when done
        >>> db.close()
    """

    def __init__(self,
                 db_path: Optional[str] = None,
                 config_path: Optional[str] = None,
                 auto_create: bool = True):
        """
        Initialize SAIQL engine

        Args:
            db_path: Path to database file (default: auto-detected from config)
            config_path: Reserved for future use. CE Edition uses environment
                        variables and .env files for configuration instead.
            auto_create: Create database if it doesn't exist (default: True)
        """
        # Load configuration (CE: uses env vars and .env, config_path reserved)
        if config_path is not None:
            logger.warning("config_path parameter is reserved; CE uses env vars/.env for config")
        self.config = get_config()

        # Use provided db_path or default from config
        if db_path is None:
            db_path = str(self.config.data_dir / "saiql.db")

        # Create data directory if needed
        if auto_create:
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        # Initialize engine
        self.engine = SAIQLEngine(db_path=db_path)
        self.session_id = "default"
        self.edition = SAIQL_EDITION

    def execute(
        self,
        query: str,
        params: Optional[Dict] = None,
    ) -> Any:
        """
        Execute a SAIQL query

        Args:
            query: SAIQL query string (SQL or symbolic syntax)
            params: Optional parameters for parameterized queries
                    (NOTE: CE edition does not currently support parameterized queries)

        Returns:
            Query results

        Example:
            >>> db.execute("SELECT * FROM users")
            >>> db.execute("USER:[name>>Alice,ACTIVE]")
        """
        if params:
            raise NotImplementedError(
                "Parameterized queries are not supported in SAIQL CE. "
                "Use string formatting with proper escaping instead."
            )
        ctx = ExecutionContext(session_id=self.session_id)
        return self.engine.execute(query, ctx)

    def query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """
        Execute a SELECT query and return results as list of dicts

        Args:
            query: SELECT query
            params: Optional parameters
                    (NOTE: CE edition does not currently support parameterized queries)

        Returns:
            List of result rows as dictionaries
        """
        result = self.execute(query, params)
        # QueryResult has a .data attribute which is List[Dict[str, Any]]
        if hasattr(result, 'data') and isinstance(result.data, list):
            return result.data
        if hasattr(result, 'fetchall'):
            return [dict(row) for row in result.fetchall()]
        # Fallback: return empty list rather than a non-list object
        return [] if not isinstance(result, list) else result

    def insert(self, table: str, data: Dict[str, Any]) -> bool:
        """
        Insert a row into a table

        Args:
            table: Table name
            data: Dictionary of column: value pairs

        Returns:
            True if successful

        Example:
            >>> db.insert("users", {"name": "Alice", "age": 30})
        """
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?" for _ in data])
        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self.execute(query, list(data.values()))
        return True

    def update(self, table: str, data: Dict[str, Any], where: str, params: Optional[List] = None) -> int:
        """
        Update rows in a table

        Args:
            table: Table name
            data: Dictionary of column: value pairs to update
            where: WHERE clause (without 'WHERE')
            params: Parameters for WHERE clause

        Returns:
            Number of rows updated

        Example:
            >>> db.update("users", {"age": 31}, "name = ?", ["Alice"])
        """
        set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
        all_params = list(data.values()) + (params or [])
        result = self.execute(query, all_params)
        return result.rowcount if hasattr(result, 'rowcount') else 0

    def delete(self, table: str, where: str, params: Optional[List] = None) -> int:
        """
        Delete rows from a table

        Args:
            table: Table name
            where: WHERE clause (without 'WHERE')
            params: Parameters for WHERE clause

        Returns:
            Number of rows deleted

        Example:
            >>> db.delete("users", "age < ?", [18])
        """
        query = f"DELETE FROM {table} WHERE {where}"
        result = self.execute(query, params)
        return result.rowcount if hasattr(result, 'rowcount') else 0

    def create_table(self, table: str, schema: Dict[str, str]) -> bool:
        """
        Create a table

        Args:
            table: Table name
            schema: Dictionary of column_name: column_type

        Returns:
            True if successful

        Example:
            >>> db.create_table("users", {
            ...     "id": "INTEGER PRIMARY KEY",
            ...     "name": "TEXT NOT NULL",
            ...     "age": "INTEGER"
            ... })
        """
        columns = ", ".join([f"{name} {dtype}" for name, dtype in schema.items()])
        query = f"CREATE TABLE IF NOT EXISTS {table} ({columns})"
        self.execute(query)
        return True

    def get_stats(self) -> Dict[str, Any]:
        """Get engine statistics."""
        stats = self.engine.get_stats()
        stats['edition'] = self.edition
        return stats

    def close(self):
        """Close the database connection"""
        if hasattr(self.engine, 'close'):
            self.engine.close()

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()


# Convenience function for quick usage
def connect(db_path: Optional[str] = None) -> SAIQL:
    """
    Quick connection to SAIQL database

    Args:
        db_path: Path to database file (default: auto-detected)

    Returns:
        SAIQL instance

    Example:
        >>> import saiql
        >>> db = saiql.connect()
        >>> result = db.execute("SELECT * FROM users")
        >>> db.close()
    """
    return SAIQL(db_path=db_path)


# CLI Interface
def main():
    """Command-line interface for SAIQL Community Edition."""
    parser = argparse.ArgumentParser(
        description='SAIQL Community Edition - Semantic AI Query Language',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python saiql.py "SELECT * FROM users"
  python saiql.py -i  # Interactive mode
  python saiql.py --stats

Note: Atlas, QIPI, and LoreToken are not available in Community Edition.
        """
    )

    # Query arguments
    parser.add_argument('query', nargs='?', help='Query to execute')

    # Database options
    parser.add_argument(
        '--db', '-d',
        type=str,
        default=None,
        help='Path to database file'
    )

    # Output options
    parser.add_argument(
        '--json', '-j',
        action='store_true',
        help='Output results as JSON'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Verbose output with metadata'
    )

    # Mode options
    parser.add_argument(
        '--interactive', '-i',
        action='store_true',
        help='Start interactive REPL mode'
    )
    parser.add_argument(
        '--stats',
        action='store_true',
        help='Show engine statistics'
    )
    parser.add_argument(
        '--version',
        action='store_true',
        help='Show version and edition'
    )

    args = parser.parse_args()

    # Handle --version
    if args.version:
        print(f"SAIQL {SAIQL_VERSION}")
        print(f"Edition: Community (CE)")
        print(f"Features: Core Engine, DB Adapters, Migration, Translation")
        print(f"Disabled: Atlas, QIPI, LoreToken")
        return

    # Initialize SAIQL
    try:
        db = SAIQL(db_path=args.db)
    except Exception as e:
        print(f"Error initializing SAIQL: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        # Handle --stats
        if args.stats:
            stats = db.get_stats()
            if args.json:
                print(json.dumps(stats, indent=2, default=str))
            else:
                print(f"SAIQL Community Edition Statistics:")
                print(f"  Edition: {stats.get('edition', 'ce')}")
                print(f"  Uptime: {stats.get('uptime_seconds', 0):.1f}s")
                print(f"  Queries: {stats.get('queries_executed', 0)}")
                print(f"  Success Rate: {stats.get('success_rate', 0)*100:.1f}%")
            return

        # Handle --interactive
        if args.interactive:
            print(f"SAIQL Community Edition Interactive Mode")
            print(f"Version: {SAIQL_VERSION}")
            print("Type 'exit' to quit, 'help' for commands")
            print()

            # Interactive mode state
            json_output = args.json  # Start with command-line setting

            while True:
                try:
                    query = input("saiql> ").strip()
                except (EOFError, KeyboardInterrupt):
                    print("\nGoodbye!")
                    break

                if not query:
                    continue
                if query.lower() in ('exit', 'quit', 'q'):
                    print("Goodbye!")
                    break
                if query.lower() == 'help':
                    print("Commands:")
                    print("  .stats         - Show statistics")
                    print("  .json on/off   - Toggle JSON output")
                    print("  .info          - Show edition info")
                    print("  exit           - Exit interactive mode")
                    continue
                if query.lower() == '.json on':
                    json_output = True
                    print("JSON output enabled")
                    continue
                if query.lower() == '.json off':
                    json_output = False
                    print("JSON output disabled")
                    continue
                if query.lower() == '.stats':
                    stats = db.get_stats()
                    print(json.dumps(stats, indent=2, default=str))
                    continue
                if query.lower() == '.info':
                    print(f"Edition: Community (CE)")
                    print(f"Version: {SAIQL_VERSION}")
                    print(f"Features: Core Engine, DB Adapters")
                    print(f"Disabled: Atlas, QIPI, LoreToken")
                    continue

                # Execute query
                try:
                    result = db.execute(query)
                    if hasattr(result, 'data') and result.data:
                        if json_output:
                            print(json.dumps(result.data, indent=2, default=str))
                        else:
                            for row in result.data[:10]:
                                print(row)
                            if len(result.data) > 10:
                                print(f"... and {len(result.data) - 10} more rows")
                    elif hasattr(result, 'success'):
                        print(f"Success: {result.success}")
                    else:
                        print(result)
                except Exception as e:
                    print(f"Error: {e}")
            return

        # Handle single query
        if args.query:
            # Execute query
            result = db.execute(args.query)

            # Output result
            if args.json:
                output = {
                    'success': result.success if hasattr(result, 'success') else True,
                    'data': result.data if hasattr(result, 'data') else result,
                }
                if args.verbose and hasattr(result, 'metadata'):
                    output['metadata'] = result.metadata
                print(json.dumps(output, indent=2, default=str))
            else:
                if hasattr(result, 'data') and result.data:
                    for row in result.data:
                        print(row)
                elif hasattr(result, 'success'):
                    if not result.success:
                        print(f"Error: {result.error_message}", file=sys.stderr)
                        sys.exit(1)
                    print("Query executed successfully")
                else:
                    print(result)
        else:
            # No query provided
            parser.print_help()

    finally:
        db.close()


# Export public API
__all__ = ['SAIQL', 'connect', 'ExecutionContext', 'get_config', 'main', 'SAIQL_EDITION', 'SAIQL_VERSION']


if __name__ == '__main__':
    main()
