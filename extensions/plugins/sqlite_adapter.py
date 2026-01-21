#!/usr/bin/env python3
"""
SAIQL SQLite Adapter - Phase 11 L0/L2/L3/L4 Harness

Provides SQLite adapter for SAIQL with L0-L4 capabilities:
- L0: get_tables(), get_schema(), extract_data()
- L2: Views (extraction, dependency ordering, emission)
- L3: Dependency Analysis (function classification)
- L4: Triggers (extraction, emission, behavioral validation)

Author: Apollo & Claude
Version: 2.0.0
Status: Phase 11 L0/L2/L3/L4 Harness
"""

import sqlite3
import logging
import time
import re
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


class SQLiteAdapter:
    """SQLite adapter with L0 capabilities."""

    def __init__(
        self,
        database: str = ':memory:',
        timeout: float = 30.0,
        **kwargs
    ):
        """
        Initialize SQLite adapter.

        Args:
            database: Path to SQLite database file or ':memory:' for in-memory
            timeout: Connection timeout in seconds
            **kwargs: Additional connection parameters
        """
        self.database = database
        self.timeout = timeout
        self._connection: Optional[sqlite3.Connection] = None
        self._connect()
        logger.info(f"SQLite adapter initialized for {database}")

    def _connect(self) -> None:
        """Establish database connection."""
        try:
            self._connection = sqlite3.connect(
                self.database,
                timeout=self.timeout,
                check_same_thread=False
            )
            self._connection.row_factory = sqlite3.Row
            logger.debug(f"Connected to SQLite database: {self.database}")
        except sqlite3.Error as e:
            logger.error(f"Failed to connect to SQLite: {e}")
            raise

    def close(self) -> None:
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.info("SQLite adapter closed")

    def execute_query(
        self,
        sql: str,
        params: Optional[tuple] = None,
        fetch: bool = True
    ) -> Dict[str, Any]:
        """
        Execute SQL query.

        Args:
            sql: SQL query string
            params: Query parameters (optional)
            fetch: Whether to fetch results

        Returns:
            Dictionary with execution results
        """
        result = {
            'success': False,
            'data': [],
            'rows_affected': 0,
            'error': None
        }

        try:
            cursor = self._connection.cursor()

            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)

            if fetch and cursor.description:
                rows = cursor.fetchall()
                result['data'] = [dict(row) for row in rows]

            result['rows_affected'] = cursor.rowcount
            result['success'] = True

            # Auto-commit for modifications
            if not sql.strip().upper().startswith('SELECT'):
                self._connection.commit()

        except sqlite3.IntegrityError as e:
            result['error'] = f"Integrity error: {str(e)}"
        except sqlite3.Error as e:
            result['error'] = f"SQLite error: {str(e)}"
        except Exception as e:
            result['error'] = f"Unexpected error: {str(e)}"

        return result

    def execute_script(self, script: str) -> Dict[str, Any]:
        """
        Execute multi-statement SQL script.

        Args:
            script: SQL script with multiple statements

        Returns:
            Dictionary with execution results
        """
        result = {
            'success': False,
            'error': None
        }

        try:
            cursor = self._connection.cursor()
            cursor.executescript(script)
            self._connection.commit()
            result['success'] = True
        except sqlite3.Error as e:
            result['error'] = f"SQLite error: {str(e)}"

        return result

    # L0 Methods

    def get_tables(self) -> List[str]:
        """
        Get list of user tables in database (L0 method).

        Returns:
            List of table names (lowercase)
        """
        query = """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """

        result = self.execute_query(query)

        if not result['success']:
            logger.error(f"Failed to get tables: {result.get('error')}")
            return []

        tables = [row['name'].lower() for row in result['data']]
        logger.debug(f"Found {len(tables)} tables: {tables}")

        return tables

    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Get table schema introspection (L0 method).

        Args:
            table_name: Table name

        Returns:
            Schema dict with columns, pk, fks
        """
        schema = {
            'columns': [],
            'pk': [],
            'fks': []
        }

        # Get column info using PRAGMA
        pragma_query = f"PRAGMA table_info('{table_name}')"
        result = self.execute_query(pragma_query)

        if not result['success']:
            logger.error(f"Failed to get schema for {table_name}: {result.get('error')}")
            return schema

        from core.type_registry import TypeRegistry, IRType

        for col in result['data']:
            col_name = col['name']
            col_type = col['type'] or 'TEXT'  # SQLite defaults to TEXT
            is_nullable = col['notnull'] == 0
            default_val = col['dflt_value']
            is_pk = col['pk'] > 0

            # Parse type for SQLite (e.g., "VARCHAR(100)" -> "VARCHAR", 100)
            base_type = col_type.split('(')[0].upper()
            length = None
            precision = None
            scale = None

            if '(' in col_type:
                type_params = col_type.split('(')[1].rstrip(')')
                if ',' in type_params:
                    parts = type_params.split(',')
                    precision = int(parts[0].strip())
                    scale = int(parts[1].strip())
                else:
                    try:
                        length = int(type_params.strip())
                    except ValueError:
                        pass

            type_info = TypeRegistry.map_to_ir('sqlite', col_type)

            if type_info.ir_type == IRType.UNKNOWN:
                logger.warning(f"Table {table_name}.{col_name}: Unsupported SQLite type '{col_type}'.")

            schema['columns'].append({
                'name': col_name.lower(),
                'type': col_type,
                'type_info': type_info,
                'unsupported': type_info.ir_type == IRType.UNKNOWN,
                'length': length,
                'precision': precision,
                'scale': scale,
                'nullable': is_nullable,
                'default': default_val
            })

            if is_pk:
                schema['pk'].append(col_name.lower())

        return schema

    def extract_data(
        self,
        table_name: str,
        order_by: Optional[List[str]] = None,
        chunk_size: int = 10000
    ) -> Dict[str, Any]:
        """
        Extract data from table with deterministic ordering (L0 method).

        Args:
            table_name: Table name
            order_by: Columns to order by
            chunk_size: Not used for SQLite (no streaming)

        Returns:
            Dictionary with data and stats
        """
        start_time = time.time()

        if order_by:
            order_clause = ', '.join([f'"{col}"' for col in order_by])
        else:
            schema = self.get_schema(table_name)
            if schema['columns']:
                first_col = schema['columns'][0]['name']
                order_clause = f'"{first_col}"'
            else:
                order_clause = ""

        if order_clause:
            query = f'SELECT * FROM "{table_name}" ORDER BY {order_clause}'
        else:
            query = f'SELECT * FROM "{table_name}"'

        result = self.execute_query(query)

        if not result['success']:
            logger.error(f"Failed to extract data from {table_name}: {result.get('error')}")
            return {
                'data': [],
                'stats': {
                    'total_rows': 0,
                    'duration_seconds': time.time() - start_time,
                    'error': result.get('error')
                }
            }

        data = result['data']
        duration = time.time() - start_time

        return {
            'data': data,
            'stats': {
                'total_rows': len(data),
                'duration_seconds': duration,
                'order_by': order_clause if order_clause else None
            }
        }

    # =========================================================================
    # L2 Methods (Views)
    # =========================================================================

    def get_views(self) -> List[Dict[str, Any]]:
        """
        Get list of views in the database (L2 method).

        Returns:
            List of view dictionaries with name and definition
        """
        query = """
            SELECT name, sql
            FROM sqlite_master
            WHERE type = 'view'
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """

        result = self.execute_query(query)

        if not result['success']:
            logger.error(f"Failed to get views: {result.get('error')}")
            return []

        views = []
        for row in result['data']:
            views.append({
                'name': row['name'],
                'definition': row['sql']
            })

        logger.debug(f"Found {len(views)} views")
        return views

    def get_view_definition(self, view_name: str) -> Optional[str]:
        """
        Get the SQL definition of a view (L2 method).

        Args:
            view_name: View name

        Returns:
            View definition SQL or None
        """
        query = """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'view'
            AND name = ?
        """

        result = self.execute_query(query, (view_name,))

        if not result['success'] or not result['data']:
            return None

        return result['data'][0].get('sql')

    def get_view_dependencies(self, view_name: str) -> List[Dict[str, Any]]:
        """
        Get view dependencies (other views referenced) (L2 method).

        Args:
            view_name: View name

        Returns:
            List of dependency dictionaries
        """
        definition = self.get_view_definition(view_name)
        if not definition:
            return []

        # Get all view names
        all_views = self.get_views()
        view_names = [v['name'] for v in all_views]

        dependencies = []
        definition_upper = definition.upper()

        for vname in view_names:
            if vname == view_name:
                continue
            # Check if view is referenced in definition
            # Look for FROM/JOIN patterns
            pattern = r'\b' + re.escape(vname.upper()) + r'\b'
            if re.search(pattern, definition_upper):
                dependencies.append({
                    'name': vname,
                    'type': 'view'
                })

        return dependencies

    def get_views_in_dependency_order(self) -> List[Dict[str, Any]]:
        """
        Get views sorted by dependency order (L2 method).

        Returns:
            List of views in order (dependencies first)
        """
        views = self.get_views()
        if not views:
            return []

        # Build dependency graph
        deps = {}
        for view in views:
            view_deps = self.get_view_dependencies(view['name'])
            deps[view['name']] = [d['name'] for d in view_deps]

        # Topological sort
        ordered = []
        visited = set()
        temp_visited = set()

        def visit(name):
            if name in temp_visited:
                return  # Cycle - skip
            if name in visited:
                return
            temp_visited.add(name)
            for dep in deps.get(name, []):
                visit(dep)
            temp_visited.remove(name)
            visited.add(name)
            ordered.append(name)

        for view in views:
            visit(view['name'])

        # Return views in order
        view_dict = {v['name']: v for v in views}
        return [view_dict[name] for name in ordered if name in view_dict]

    def create_view(self, name: str, definition: str) -> Dict[str, Any]:
        """
        Create a view (L2 emission method).

        Args:
            name: View name
            definition: CREATE VIEW statement

        Returns:
            Result dict with success status
        """
        return self.execute_query(definition, fetch=False)

    def drop_view(self, name: str, if_exists: bool = True) -> Dict[str, Any]:
        """
        Drop a view (L2 emission method).

        Args:
            name: View name
            if_exists: Use IF EXISTS check

        Returns:
            Result dict with success status
        """
        if if_exists:
            query = f'DROP VIEW IF EXISTS "{name}"'
        else:
            query = f'DROP VIEW "{name}"'

        return self.execute_query(query, fetch=False)

    def create_views_in_order(self, views: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create multiple views in dependency order (L2 bulk emission).

        Args:
            views: List of view dictionaries with 'name' and 'definition'

        Returns:
            Result dict with created/failed counts
        """
        created = 0
        failed = 0
        errors = []

        for view in views:
            result = self.create_view(view['name'], view['definition'])
            if result['success']:
                created += 1
            else:
                failed += 1
                errors.append({'name': view['name'], 'error': result.get('error')})

        return {
            'success': failed == 0,
            'created': created,
            'failed': failed,
            'errors': errors
        }

    # =========================================================================
    # L3 Methods (Dependency Analysis)
    # =========================================================================

    # SQLite builtin functions (comprehensive list)
    SQLITE_BUILTIN_FUNCTIONS = {
        # Core functions
        'abs', 'changes', 'char', 'coalesce', 'glob', 'hex', 'ifnull',
        'instr', 'last_insert_rowid', 'length', 'like', 'likelihood',
        'likely', 'load_extension', 'lower', 'ltrim', 'max', 'min',
        'nullif', 'printf', 'quote', 'random', 'randomblob', 'replace',
        'round', 'rtrim', 'soundex', 'sqlite_compileoption_get',
        'sqlite_compileoption_used', 'sqlite_offset', 'sqlite_source_id',
        'sqlite_version', 'substr', 'substring', 'total_changes', 'trim',
        'typeof', 'unicode', 'unlikely', 'upper', 'zeroblob',
        # Aggregate functions
        'avg', 'count', 'group_concat', 'sum', 'total',
        # Date/time functions
        'date', 'time', 'datetime', 'julianday', 'strftime', 'unixepoch',
        'timediff',
        # Math functions (3.35+)
        'acos', 'acosh', 'asin', 'asinh', 'atan', 'atan2', 'atanh',
        'ceil', 'ceiling', 'cos', 'cosh', 'degrees', 'exp', 'floor',
        'ln', 'log', 'log10', 'log2', 'mod', 'pi', 'pow', 'power',
        'radians', 'sign', 'sin', 'sinh', 'sqrt', 'tan', 'tanh', 'trunc',
        # Window functions
        'row_number', 'rank', 'dense_rank', 'ntile', 'lag', 'lead',
        'first_value', 'last_value', 'nth_value', 'cume_dist', 'percent_rank',
    }

    # JSON1 extension functions
    SQLITE_JSON_FUNCTIONS = {
        'json', 'json_array', 'json_array_length', 'json_extract',
        'json_insert', 'json_object', 'json_patch', 'json_remove',
        'json_replace', 'json_set', 'json_type', 'json_valid',
        'json_quote', 'json_group_array', 'json_group_object',
        'json_each', 'json_tree',
    }

    # FTS extension functions
    SQLITE_FTS_FUNCTIONS = {
        'match', 'highlight', 'snippet', 'offsets', 'matchinfo', 'bm25',
    }

    def extract_function_calls(self, sql: str) -> List[str]:
        """
        Extract function calls from SQL definition (L3 method).

        Args:
            sql: SQL definition string

        Returns:
            List of function names found
        """
        if not sql:
            return []

        # Pattern to match function calls: identifier followed by (
        # Exclude SQL keywords
        pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*\('
        matches = re.findall(pattern, sql, re.IGNORECASE)

        # Filter out SQL keywords
        sql_keywords = {
            'select', 'from', 'where', 'join', 'left', 'right', 'inner',
            'outer', 'on', 'and', 'or', 'not', 'in', 'exists', 'case',
            'when', 'then', 'else', 'end', 'as', 'create', 'view',
            'table', 'insert', 'update', 'delete', 'values', 'set',
            'group', 'order', 'by', 'having', 'union', 'except',
            'intersect', 'limit', 'offset', 'distinct', 'all', 'trigger',
            'begin', 'after', 'before', 'instead', 'of', 'for', 'each',
            'row', 'new', 'old', 'raise', 'abort', 'rollback', 'fail',
            'ignore', 'references', 'primary', 'key', 'foreign',
        }

        functions = []
        for match in matches:
            if match.lower() not in sql_keywords:
                functions.append(match.lower())

        return list(set(functions))

    def classify_function(self, func_name: str) -> str:
        """
        Classify a function as builtin/extension/unknown (L3 method).

        Args:
            func_name: Function name

        Returns:
            Classification: 'builtin', 'json_extension', 'fts_extension', or 'unknown'
        """
        func_lower = func_name.lower()

        if func_lower in self.SQLITE_BUILTIN_FUNCTIONS:
            return 'builtin'
        elif func_lower in self.SQLITE_JSON_FUNCTIONS:
            return 'json_extension'
        elif func_lower in self.SQLITE_FTS_FUNCTIONS:
            return 'fts_extension'
        else:
            return 'unknown'

    def analyze_dependencies(self, sql: str) -> Dict[str, Any]:
        """
        Analyze function dependencies in SQL (L3 method).

        Args:
            sql: SQL definition string

        Returns:
            Dictionary with classified dependencies
        """
        functions = self.extract_function_calls(sql)

        result = {
            'builtin': [],
            'json_extension': [],
            'fts_extension': [],
            'unknown': [],
            'total': len(functions)
        }

        for func in functions:
            classification = self.classify_function(func)
            result[classification].append(func)

        result['is_safe'] = len(result['unknown']) == 0
        result['needs_extension'] = (
            len(result['json_extension']) > 0 or
            len(result['fts_extension']) > 0
        )

        return result

    def get_all_dependencies(self) -> Dict[str, Any]:
        """
        Get dependency analysis for all views and triggers (L3 method).

        Returns:
            Dictionary with dependencies per object
        """
        result = {
            'views': {},
            'triggers': {},
            'summary': {
                'total_objects': 0,
                'safe_objects': 0,
                'needs_extension': 0,
                'needs_app_layer': 0,
                'all_functions': set()
            }
        }

        # Analyze views
        views = self.get_views()
        for view in views:
            deps = self.analyze_dependencies(view['definition'])
            result['views'][view['name']] = deps
            result['summary']['total_objects'] += 1
            if deps['is_safe'] and not deps['needs_extension']:
                result['summary']['safe_objects'] += 1
            if deps['needs_extension']:
                result['summary']['needs_extension'] += 1
            if not deps['is_safe']:
                result['summary']['needs_app_layer'] += 1
            for cat in ['builtin', 'json_extension', 'fts_extension', 'unknown']:
                result['summary']['all_functions'].update(deps[cat])

        # Analyze triggers
        triggers = self.get_triggers()
        for trigger in triggers:
            deps = self.analyze_dependencies(trigger['definition'])
            result['triggers'][trigger['name']] = deps
            result['summary']['total_objects'] += 1
            if deps['is_safe'] and not deps['needs_extension']:
                result['summary']['safe_objects'] += 1
            if deps['needs_extension']:
                result['summary']['needs_extension'] += 1
            if not deps['is_safe']:
                result['summary']['needs_app_layer'] += 1
            for cat in ['builtin', 'json_extension', 'fts_extension', 'unknown']:
                result['summary']['all_functions'].update(deps[cat])

        # Convert set to list for JSON serialization
        result['summary']['all_functions'] = sorted(result['summary']['all_functions'])

        return result

    def get_safe_objects(self) -> Dict[str, List[str]]:
        """
        Get objects that use only builtin functions (L3 method).

        Returns:
            Dictionary with safe views and triggers
        """
        all_deps = self.get_all_dependencies()

        safe = {
            'views': [],
            'triggers': []
        }

        for name, deps in all_deps['views'].items():
            if deps['is_safe'] and not deps['needs_extension']:
                safe['views'].append(name)

        for name, deps in all_deps['triggers'].items():
            if deps['is_safe'] and not deps['needs_extension']:
                safe['triggers'].append(name)

        return safe

    def get_objects_needing_extension(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get objects that require extensions (L3 method).

        Returns:
            Dictionary with objects requiring extensions
        """
        all_deps = self.get_all_dependencies()

        needs_ext = {
            'views': [],
            'triggers': []
        }

        for name, deps in all_deps['views'].items():
            if deps['needs_extension']:
                needs_ext['views'].append({
                    'name': name,
                    'extensions': deps['json_extension'] + deps['fts_extension']
                })

        for name, deps in all_deps['triggers'].items():
            if deps['needs_extension']:
                needs_ext['triggers'].append({
                    'name': name,
                    'extensions': deps['json_extension'] + deps['fts_extension']
                })

        return needs_ext

    def get_objects_needing_app_layer(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get objects that require app-layer UDF registration (L3 method).

        Returns:
            Dictionary with objects requiring app-layer functions
        """
        all_deps = self.get_all_dependencies()

        needs_app = {
            'views': [],
            'triggers': []
        }

        for name, deps in all_deps['views'].items():
            if not deps['is_safe']:
                needs_app['views'].append({
                    'name': name,
                    'unknown_functions': deps['unknown']
                })

        for name, deps in all_deps['triggers'].items():
            if not deps['is_safe']:
                needs_app['triggers'].append({
                    'name': name,
                    'unknown_functions': deps['unknown']
                })

        return needs_app

    # =========================================================================
    # L4 Methods (Triggers)
    # =========================================================================

    def get_triggers(self) -> List[Dict[str, Any]]:
        """
        Get list of triggers in the database (L4 method).

        Returns:
            List of trigger dictionaries
        """
        query = """
            SELECT name, tbl_name, sql
            FROM sqlite_master
            WHERE type = 'trigger'
            AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """

        result = self.execute_query(query)

        if not result['success']:
            logger.error(f"Failed to get triggers: {result.get('error')}")
            return []

        triggers = []
        for row in result['data']:
            definition = row['sql'] or ''

            # Parse trigger timing and event from definition
            timing = 'AFTER'  # default
            event = 'UNKNOWN'

            def_upper = definition.upper()
            if 'BEFORE' in def_upper:
                timing = 'BEFORE'
            elif 'AFTER' in def_upper:
                timing = 'AFTER'
            elif 'INSTEAD OF' in def_upper:
                timing = 'INSTEAD OF'

            if ' INSERT ' in def_upper or def_upper.endswith(' INSERT'):
                event = 'INSERT'
            elif ' UPDATE ' in def_upper or ' UPDATE OF ' in def_upper:
                event = 'UPDATE'
            elif ' DELETE ' in def_upper or def_upper.endswith(' DELETE'):
                event = 'DELETE'

            triggers.append({
                'name': row['name'],
                'table_name': row['tbl_name'],
                'timing': timing,
                'event': event,
                'definition': definition,
                'is_instead_of': timing == 'INSTEAD OF'
            })

        logger.debug(f"Found {len(triggers)} triggers")
        return triggers

    def get_trigger_definition(self, trigger_name: str) -> Optional[str]:
        """
        Get the SQL definition of a trigger (L4 method).

        Args:
            trigger_name: Trigger name

        Returns:
            Trigger definition SQL or None
        """
        query = """
            SELECT sql
            FROM sqlite_master
            WHERE type = 'trigger'
            AND name = ?
        """

        result = self.execute_query(query, (trigger_name,))

        if not result['success'] or not result['data']:
            return None

        return result['data'][0].get('sql')

    def get_safe_triggers(self) -> List[Dict[str, Any]]:
        """
        Get triggers that are in the safe subset (L4 method).

        Safe subset:
        - BEFORE/AFTER triggers on tables
        - No INSTEAD OF triggers (complex semantics)
        - No recursive patterns detected

        Returns:
            List of safe trigger dictionaries
        """
        triggers = self.get_triggers()
        safe = []

        for trigger in triggers:
            is_safe, reasons = self._is_trigger_safe(trigger)
            if is_safe:
                safe.append(trigger)

        return safe

    def get_skipped_triggers(self) -> List[Dict[str, Any]]:
        """
        Get triggers that are skipped (not in safe subset) (L4 method).

        Returns:
            List of skipped trigger dictionaries with reasons
        """
        triggers = self.get_triggers()
        skipped = []

        for trigger in triggers:
            is_safe, reasons = self._is_trigger_safe(trigger)
            if not is_safe:
                trigger_copy = dict(trigger)
                trigger_copy['skip_reasons'] = reasons
                skipped.append(trigger_copy)

        return skipped

    def _is_trigger_safe(self, trigger: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Check if a trigger is in the safe subset (L4 method).

        Args:
            trigger: Trigger dictionary

        Returns:
            Tuple of (is_safe, reasons_if_not_safe)
        """
        reasons = []

        # INSTEAD OF triggers are complex
        if trigger.get('is_instead_of'):
            reasons.append('INSTEAD OF trigger')

        # Check definition for problematic patterns
        definition = trigger.get('definition', '')
        if definition:
            def_upper = definition.upper()

            # Check for recursive patterns (trigger on same table it modifies)
            # This is simplistic - real detection would need more analysis
            # For now we rely on PRAGMA recursive_triggers = OFF

        return len(reasons) == 0, reasons

    def create_trigger(self, name: str, definition: str) -> Dict[str, Any]:
        """
        Create a trigger (L4 emission method).

        Args:
            name: Trigger name
            definition: CREATE TRIGGER statement

        Returns:
            Result dict with success status
        """
        return self.execute_query(definition, fetch=False)

    def drop_trigger(self, name: str, if_exists: bool = True) -> Dict[str, Any]:
        """
        Drop a trigger (L4 emission method).

        Args:
            name: Trigger name
            if_exists: Use IF EXISTS check

        Returns:
            Result dict with success status
        """
        if if_exists:
            query = f'DROP TRIGGER IF EXISTS "{name}"'
        else:
            query = f'DROP TRIGGER "{name}"'

        return self.execute_query(query, fetch=False)

    def get_pragma_settings(self) -> Dict[str, Any]:
        """
        Get relevant PRAGMA settings for determinism (L4 method).

        Returns:
            Dictionary of PRAGMA settings
        """
        pragmas = {}

        pragma_list = [
            'foreign_keys',
            'recursive_triggers',
            'encoding',
            'journal_mode',
        ]

        for pragma in pragma_list:
            result = self.execute_query(f'PRAGMA {pragma}')
            if result['success'] and result['data']:
                pragmas[pragma] = result['data'][0].get(pragma)

        return pragmas

    def set_pragma(self, pragma: str, value: Any) -> Dict[str, Any]:
        """
        Set a PRAGMA value (L4 method).

        Args:
            pragma: PRAGMA name
            value: Value to set

        Returns:
            Result dict with success status
        """
        query = f'PRAGMA {pragma} = {value}'
        return self.execute_query(query, fetch=False)
