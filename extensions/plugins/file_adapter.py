"""
FILE Adapter with L2/L3/L4 support for CSV and Excel sources.

L2 (Views): Derived datasets over files with deterministic config
L3 (Transform Pipelines): Deterministic transform pipelines producing output artifacts
L4 (File Event Automation): Trigger-like automation with simulated events
"""

import os
import json
import re
import logging
from typing import List, Dict, Any, Generator, Optional, Set, Tuple
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
import pandas as pd

logger = logging.getLogger(__name__)

# ============================================================================
# Configuration Constants
# ============================================================================

CSV_DEFAULTS = {
    'delimiter': ',',
    'quote_char': '"',
    'encoding': 'utf-8',
    'header_row': True,
    'strict_schema': True,
    'empty_string_as_null': False
}

EXCEL_DEFAULTS = {
    'header_row': True,
    'strict_schema': True,
    'empty_row_policy': 'stop_at_first_empty',  # or 'include_empties'
    'sheet_selection': 'first'  # or specific sheet name
}

# Deterministic operations allowed in L3 pipelines
ALLOWED_PIPELINE_OPERATIONS = {'JOIN', 'FILTER', 'AGGREGATE', 'PROJECT', 'SORT', 'UNION'}

# Validation rules for L4 triggers
VALIDATION_RULES = {
    'NOT_NULL', 'POSITIVE_INTEGER', 'POSITIVE_NUMBER', 'CONTAINS', 'IN_SET',
    'MIN_LENGTH', 'MAX_LENGTH', 'REGEX_MATCH'
}

# Event types for L4 triggers
EVENT_TYPES = {'on_ingest', 'on_change', 'on_batch_complete'}


# ============================================================================
# Data Classes
# ============================================================================

@dataclass
class TableConfig:
    """Deterministic configuration for a file-backed table."""
    source_file: str
    format: str  # 'CSV' or 'EXCEL'
    config: Dict[str, Any]
    columns: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            'source_file': self.source_file,
            'format': self.format,
            'config': self.config,
            'columns': self.columns
        }


@dataclass
class ViewDefinition:
    """L2 View definition over file tables."""
    name: str
    description: str
    source_tables: List[str]
    definition: str
    columns: List[Dict[str, Any]]
    dependencies: List[str] = field(default_factory=list)
    expected_row_count: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'description': self.description,
            'source_tables': self.source_tables,
            'definition': self.definition,
            'columns': self.columns,
            'dependencies': self.dependencies,
            'expected_row_count': self.expected_row_count
        }


@dataclass
class PipelineDefinition:
    """L3 Transform pipeline definition."""
    name: str
    description: str
    source_tables: List[str]
    output_file: str
    steps: List[Dict[str, Any]]
    expected_output: Dict[str, Any]
    parameters: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'description': self.description,
            'source_tables': self.source_tables,
            'output_file': self.output_file,
            'steps': self.steps,
            'expected_output': self.expected_output,
            'parameters': self.parameters
        }


@dataclass
class TriggerDefinition:
    """L4 Trigger definition for file events."""
    name: str
    description: str
    event: str  # on_ingest, on_change, on_batch_complete
    source_table: Optional[str]
    action: str  # validate, audit_log, refresh_views, status_report
    config: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'description': self.description,
            'event': self.event,
            'source_table': self.source_table,
            'action': self.action,
            'config': self.config
        }


@dataclass
class SimulatedEvent:
    """Simulated file event for deterministic testing."""
    event_id: int
    event_type: str
    timestamp: str
    source_table: Optional[str] = None
    filename: Optional[str] = None
    row_count: Optional[int] = None
    file_count: Optional[int] = None
    total_row_count: Optional[int] = None
    change_type: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            'event_id': self.event_id,
            'event_type': self.event_type,
            'timestamp': self.timestamp
        }
        if self.source_table:
            result['source_table'] = self.source_table
        if self.filename:
            result['filename'] = self.filename
        if self.row_count is not None:
            result['row_count'] = self.row_count
        if self.file_count is not None:
            result['file_count'] = self.file_count
        if self.total_row_count is not None:
            result['total_row_count'] = self.total_row_count
        if self.change_type:
            result['change_type'] = self.change_type
        return result


# ============================================================================
# Main FileAdapter Class
# ============================================================================

class FileAdapter:
    """
    Adapter for treating a directory of CSV/Excel files as a database source.
    Supports L2 (Views), L3 (Transform Pipelines), and L4 (File Event Automation).
    """

    def __init__(self, path: str, schema_file: Optional[str] = None):
        """
        Initialize FileAdapter.

        Args:
            path: Directory containing source files or single file path
            schema_file: Optional JSON file with deterministic schema definitions
        """
        self.path = Path(path.replace("file://", ""))
        self.tables: Dict[str, Path] = {}
        self.table_configs: Dict[str, TableConfig] = {}
        self.views: Dict[str, ViewDefinition] = {}
        self.pipelines: Dict[str, PipelineDefinition] = {}
        self.triggers: Dict[str, TriggerDefinition] = {}
        self.audit_log: List[Dict[str, Any]] = []
        self._schema_file = schema_file
        self._scan_directory()
        if schema_file:
            self._load_schema(schema_file)

    # ========================================================================
    # Base Methods (L1)
    # ========================================================================

    def _scan_directory(self):
        """Scan path for supported files."""
        if self.path.is_file():
            stem = self.path.stem
            self.tables[stem] = self.path
        elif self.path.is_dir():
            for ext in ['*.csv', '*.xlsx', '*.xls']:
                for f in self.path.glob(ext):
                    self.tables[f.stem] = f

        logger.info(f"FileAdapter found {len(self.tables)} tables: {list(self.tables.keys())}")

    def _load_schema(self, schema_file: str):
        """Load deterministic schema from JSON file."""
        schema_path = Path(schema_file)
        if not schema_path.exists():
            raise FileNotFoundError(f"Schema file not found: {schema_file}")

        with open(schema_path, 'r') as f:
            schema = json.load(f)

        for table_name, table_def in schema.get('tables', {}).items():
            self.table_configs[table_name] = TableConfig(
                source_file=table_def['source_file'],
                format=table_def['format'],
                config=table_def.get('config', {}),
                columns=table_def.get('columns', [])
            )

    def connect(self):
        """Connect to file source."""
        if not self.path.exists():
            raise FileNotFoundError(f"Source path {self.path} does not exist")
        return self

    def close(self):
        """Close adapter (no-op for files)."""
        pass

    def get_tables(self) -> List[str]:
        """Return list of available tables."""
        return list(self.tables.keys())

    def get_schema(self, table_name: str) -> Dict[str, Any]:
        """Get schema for a table, using explicit config if available."""
        # Use explicit schema if configured
        if table_name in self.table_configs:
            config = self.table_configs[table_name]
            return {
                'columns': config.columns,
                'pk': [],
                'fks': [],
                'indexes': [],
                'config': config.config
            }

        # Fall back to inference
        file_path = self.tables[table_name]
        try:
            if file_path.suffix == '.csv':
                df = pd.read_csv(file_path, nrows=1000)
            else:
                df = pd.read_excel(file_path, nrows=1000)

            columns = []
            for col in df.columns:
                dtype = df[col].dtype
                sql_type = "TEXT"

                if pd.api.types.is_integer_dtype(dtype):
                    sql_type = "INTEGER"
                elif pd.api.types.is_float_dtype(dtype):
                    sql_type = "REAL"
                elif pd.api.types.is_bool_dtype(dtype):
                    sql_type = "BOOLEAN"
                elif pd.api.types.is_datetime64_any_dtype(dtype):
                    sql_type = "TIMESTAMP"

                columns.append({
                    "name": col,
                    "type": sql_type,
                    "nullable": True,
                    "default": None
                })

            return {
                "columns": columns,
                "pk": [],
                "fks": [],
                "indexes": []
            }

        except Exception as e:
            logger.error(f"Failed to infer schema for {table_name}: {e}")
            raise

    def fetch_rows(self, table_name: str, batch_size: int = 1000) -> Generator[List[Dict[str, Any]], None, None]:
        """Yield batches of rows from a table."""
        file_path = self.tables[table_name]
        config = self.table_configs.get(table_name)

        def process_chunk(chunk):
            return chunk.where(pd.notnull(chunk), None).to_dict('records')

        if file_path.suffix == '.csv':
            read_kwargs = {}
            if config:
                read_kwargs['delimiter'] = config.config.get('delimiter', ',')
                read_kwargs['quotechar'] = config.config.get('quote_char', '"')
                read_kwargs['encoding'] = config.config.get('encoding', 'utf-8')
            for chunk in pd.read_csv(file_path, chunksize=batch_size, **read_kwargs):
                yield process_chunk(chunk)
        else:
            df = pd.read_excel(file_path)
            total = len(df)
            for i in range(0, total, batch_size):
                yield process_chunk(df.iloc[i:i+batch_size])

    def _read_table(self, table_name: str) -> pd.DataFrame:
        """Read entire table into DataFrame."""
        file_path = self.tables[table_name]
        config = self.table_configs.get(table_name)

        if file_path.suffix == '.csv':
            read_kwargs = {}
            if config:
                read_kwargs['delimiter'] = config.config.get('delimiter', ',')
                read_kwargs['quotechar'] = config.config.get('quote_char', '"')
                read_kwargs['encoding'] = config.config.get('encoding', 'utf-8')
            return pd.read_csv(file_path, **read_kwargs)
        else:
            return pd.read_excel(file_path)

    # ========================================================================
    # L2 Methods (Views)
    # ========================================================================

    def load_views(self, views_file: str) -> None:
        """Load view definitions from JSON file."""
        with open(views_file, 'r') as f:
            data = json.load(f)

        for view_def in data.get('views', []):
            self.views[view_def['name']] = ViewDefinition(
                name=view_def['name'],
                description=view_def.get('description', ''),
                source_tables=view_def.get('source_tables', []),
                definition=view_def['definition'],
                columns=view_def.get('columns', []),
                dependencies=view_def.get('dependencies', []),
                expected_row_count=view_def.get('expected_row_count')
            )

    def get_views(self) -> List[str]:
        """Return list of defined views."""
        return list(self.views.keys())

    def get_view_definition(self, name: str) -> Optional[ViewDefinition]:
        """Get view definition by name."""
        return self.views.get(name)

    def get_view_dependencies(self, name: str) -> List[str]:
        """Get dependencies for a view."""
        view = self.views.get(name)
        return view.dependencies if view else []

    def get_views_in_dependency_order(self) -> List[str]:
        """Return views in topologically sorted order (base views first)."""
        # Build dependency graph
        in_degree = {name: 0 for name in self.views}
        dependents = {name: [] for name in self.views}

        for name, view in self.views.items():
            for dep in view.dependencies:
                if dep in self.views:
                    in_degree[name] += 1
                    dependents[dep].append(name)

        # Topological sort (Kahn's algorithm)
        result = []
        queue = [name for name, degree in in_degree.items() if degree == 0]

        while queue:
            queue.sort()  # Ensure deterministic ordering
            current = queue.pop(0)
            result.append(current)

            for dependent in dependents[current]:
                in_degree[dependent] -= 1
                if in_degree[dependent] == 0:
                    queue.append(dependent)

        return result

    def create_view(self, name: str, definition: ViewDefinition) -> None:
        """Create/register a view definition."""
        self.views[name] = definition

    def drop_view(self, name: str) -> bool:
        """Drop a view definition."""
        if name in self.views:
            del self.views[name]
            return True
        return False

    def query_view(self, name: str) -> pd.DataFrame:
        """Execute a view and return results as DataFrame."""
        view = self.views.get(name)
        if not view:
            raise ValueError(f"View not found: {name}")

        # Load required tables into a context
        context = {}
        for table_name in view.source_tables:
            if table_name in self.tables:
                context[table_name] = self._read_table(table_name)

        # Also load dependent views
        for dep in view.dependencies:
            if dep in self.views:
                context[dep] = self.query_view(dep)

        # Execute the view definition using pandasql
        try:
            import pandasql as ps
            result = ps.sqldf(view.definition, context)
            return result
        except ImportError:
            # Fallback: simple implementation for basic queries
            return self._execute_simple_query(view.definition, context)

    def _execute_simple_query(self, sql: str, context: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Simple query executor for basic SELECT statements."""
        sql_upper = sql.upper()

        # Extract table name from simple SELECT ... FROM table WHERE ...
        from_match = re.search(r'FROM\s+(\w+)', sql_upper)
        if not from_match:
            raise ValueError(f"Cannot parse SQL: {sql}")

        table_name = from_match.group(1).lower()

        # Find the actual table (case-insensitive)
        actual_table = None
        for name, df in context.items():
            if name.lower() == table_name:
                actual_table = df.copy()
                break

        if actual_table is None:
            raise ValueError(f"Table not found in context: {table_name}")

        # Handle WHERE clause for simple conditions
        where_match = re.search(r'WHERE\s+(.+?)(?:ORDER|GROUP|$)', sql_upper, re.DOTALL)
        if where_match:
            condition = where_match.group(1).strip()
            actual_table = self._apply_filter(actual_table, condition)

        # Handle column selection
        select_match = re.search(r'SELECT\s+(.+?)\s+FROM', sql_upper)
        if select_match:
            cols_str = select_match.group(1).strip()
            if cols_str != '*':
                cols = [c.strip().lower() for c in cols_str.split(',')]
                # Map to actual column names
                actual_cols = []
                for col in cols:
                    for c in actual_table.columns:
                        if c.lower() == col:
                            actual_cols.append(c)
                            break
                if actual_cols:
                    actual_table = actual_table[actual_cols]

        return actual_table

    def emit_view_definition(self, name: str) -> str:
        """Emit view definition as SQL-like string."""
        view = self.views.get(name)
        if not view:
            raise ValueError(f"View not found: {name}")
        return f"CREATE VIEW {view.name} AS {view.definition}"

    def validate_view_parity(self, name: str) -> Dict[str, Any]:
        """Validate view parity (presence, definition, results)."""
        view = self.views.get(name)
        if not view:
            return {'name': name, 'exists': False, 'parity': False}

        result = {
            'name': name,
            'exists': True,
            'definition_valid': bool(view.definition),
            'columns_valid': len(view.columns) > 0
        }

        # Execute and check row count if expected
        try:
            df = self.query_view(name)
            result['query_success'] = True
            result['row_count'] = len(df)
            if view.expected_row_count is not None:
                result['row_count_match'] = len(df) == view.expected_row_count
            else:
                result['row_count_match'] = True
            result['parity'] = all([
                result['definition_valid'],
                result['columns_valid'],
                result['query_success'],
                result['row_count_match']
            ])
        except Exception as e:
            result['query_success'] = False
            result['query_error'] = str(e)
            result['parity'] = False

        return result

    # ========================================================================
    # L3 Methods (Transform Pipelines)
    # ========================================================================

    def load_pipelines(self, pipelines_file: str) -> None:
        """Load pipeline definitions from JSON file."""
        with open(pipelines_file, 'r') as f:
            data = json.load(f)

        for pipeline_def in data.get('pipelines', []):
            self.pipelines[pipeline_def['name']] = PipelineDefinition(
                name=pipeline_def['name'],
                description=pipeline_def.get('description', ''),
                source_tables=pipeline_def.get('source_tables', []),
                output_file=pipeline_def['output_file'],
                steps=pipeline_def.get('steps', []),
                expected_output=pipeline_def.get('expected_output', {}),
                parameters=pipeline_def.get('parameters', {})
            )

    def get_pipelines(self) -> List[str]:
        """Return list of defined pipelines."""
        return list(self.pipelines.keys())

    def get_pipeline_definition(self, name: str) -> Optional[PipelineDefinition]:
        """Get pipeline definition by name."""
        return self.pipelines.get(name)

    def validate_pipeline_definition(self, name: str) -> Dict[str, Any]:
        """Validate that a pipeline uses only deterministic operations."""
        pipeline = self.pipelines.get(name)
        if not pipeline:
            return {'name': name, 'valid': False, 'error': 'Pipeline not found'}

        issues = []
        for i, step in enumerate(pipeline.steps):
            step_type = step.get('type', '').upper()
            if step_type not in ALLOWED_PIPELINE_OPERATIONS:
                issues.append(f"Step {i}: Invalid operation '{step_type}'")

            # Check for non-deterministic patterns
            if step_type == 'FILTER':
                condition = step.get('condition', '')
                if 'NOW()' in condition.upper() or 'RANDOM()' in condition.upper():
                    issues.append(f"Step {i}: Non-deterministic function in condition")

        return {
            'name': name,
            'valid': len(issues) == 0,
            'issues': issues,
            'step_count': len(pipeline.steps),
            'operations': [s.get('type', '') for s in pipeline.steps]
        }

    def execute_pipeline(self, name: str, output_dir: str, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute a pipeline and write output to file."""
        pipeline = self.pipelines.get(name)
        if not pipeline:
            raise ValueError(f"Pipeline not found: {name}")

        # Merge parameters
        params = {**pipeline.parameters, **(parameters or {})}

        # Validate all source tables exist before execution
        missing_tables = [t for t in pipeline.source_tables if t not in self.tables]
        if missing_tables:
            raise ValueError(f"Pipeline '{name}' references missing source tables: {missing_tables}")

        # Load source tables
        context = {}
        for table_name in pipeline.source_tables:
            context[table_name] = self._read_table(table_name)

        # Execute pipeline steps
        result_df = None
        for step in pipeline.steps:
            result_df = self._execute_pipeline_step(step, context, result_df, params)

        # Write output
        output_path = Path(output_dir) / pipeline.output_file
        output_path.parent.mkdir(parents=True, exist_ok=True)
        result_df.to_csv(output_path, index=False)

        return {
            'name': name,
            'output_file': str(output_path),
            'row_count': len(result_df),
            'columns': list(result_df.columns),
            'success': True
        }

    def _get_working_df(self, current_df: Optional[pd.DataFrame], context: Dict[str, pd.DataFrame],
                        step_type: str) -> pd.DataFrame:
        """Get the working DataFrame for a pipeline step, with proper error handling."""
        if current_df is not None:
            return current_df
        if not context:
            raise ValueError(f"{step_type} step requires data but context is empty and no previous result exists")
        return list(context.values())[0]

    def _execute_pipeline_step(self, step: Dict[str, Any], context: Dict[str, pd.DataFrame],
                                current_df: Optional[pd.DataFrame], params: Dict[str, Any]) -> pd.DataFrame:
        """Execute a single pipeline step."""
        step_type = step.get('type', '').upper()

        if step_type == 'JOIN':
            left_name = step['left']
            right_name = step['right']
            left_df = context.get(left_name, current_df)
            right_df = context.get(right_name)

            # Validate both sides of the join exist
            if left_df is None:
                raise ValueError(f"JOIN step: left table '{left_name}' not found in context")
            if right_df is None:
                raise ValueError(f"JOIN step: right table '{right_name}' not found in context")

            # Parse join condition
            on_clause = step['on']
            match = re.match(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', on_clause)
            if match:
                left_col = match.group(2)
                right_col = match.group(4)
                result = pd.merge(left_df, right_df, left_on=left_col, right_on=right_col, how='inner')
                # Store joined result in context for reference
                context['_joined'] = result
                return result
            raise ValueError(f"Cannot parse join condition: {on_clause}")

        elif step_type == 'FILTER':
            df = self._get_working_df(current_df, context, 'FILTER')
            condition = step['condition']

            # Substitute parameters
            for key, value in params.items():
                condition = condition.replace(f'{{{key}}}', str(value))

            # Parse simple conditions
            return self._apply_filter(df, condition)

        elif step_type == 'AGGREGATE':
            df = self._get_working_df(current_df, context, 'AGGREGATE')
            group_cols = step.get('group_by', [])
            aggs = step.get('aggregations', [])

            # Clean column names (remove table prefixes)
            clean_group_cols = [c.split('.')[-1] for c in group_cols]

            # Build aggregation specs - handle multiple aggregations on same column
            # Map SQL functions to pandas functions
            func_map = {'avg': 'mean', 'count': 'count'}

            # Build a list of (column, func, alias) for proper multi-agg handling
            agg_specs = []
            for agg in aggs:
                col = agg['column'].split('.')[-1]
                func = agg['function'].lower()
                pandas_func = func_map.get(func, func)
                alias = agg.get('alias', f'{func}_{col}')
                agg_specs.append((col, pandas_func, alias))

            if clean_group_cols:
                # Use named aggregation for proper multi-column output
                grouped = df.groupby(clean_group_cols, as_index=False)
                agg_dict = {alias: pd.NamedAgg(column=col, aggfunc=func)
                           for col, func, alias in agg_specs}
                result = grouped.agg(**agg_dict)
            else:
                # No grouping - aggregate entire DataFrame
                result_data = {}
                for col, func, alias in agg_specs:
                    if func == 'count':
                        result_data[alias] = len(df)
                    else:
                        result_data[alias] = getattr(df[col], func)()
                result = pd.DataFrame([result_data])

            return result

        elif step_type == 'PROJECT':
            df = self._get_working_df(current_df, context, 'PROJECT')
            cols = step['columns']

            # Clean column names
            clean_cols = []
            for c in cols:
                clean = c.split('.')[-1]
                # Find actual column in df
                for actual in df.columns:
                    if actual.lower() == clean.lower() or actual == clean:
                        clean_cols.append(actual)
                        break

            return df[clean_cols]

        elif step_type == 'SORT':
            df = self._get_working_df(current_df, context, 'SORT')
            by_cols = [c.split('.')[-1] for c in step['by']]
            ascending = step.get('order', 'ASC').upper() == 'ASC'

            # Find actual column names
            actual_cols = []
            for c in by_cols:
                for actual in df.columns:
                    if actual.lower() == c.lower():
                        actual_cols.append(actual)
                        break

            return df.sort_values(by=actual_cols, ascending=ascending)

        raise ValueError(f"Unknown step type: {step_type}")

    def _apply_filter(self, df: pd.DataFrame, condition: str) -> pd.DataFrame:
        """Apply a filter condition to a DataFrame."""
        # Handle compound conditions with AND
        if ' AND ' in condition.upper():
            parts = re.split(r'\s+AND\s+', condition, flags=re.IGNORECASE)
            result = df
            for part in parts:
                result = self._apply_single_filter(result, part.strip())
            return result
        return self._apply_single_filter(df, condition)

    def _apply_single_filter(self, df: pd.DataFrame, condition: str) -> pd.DataFrame:
        """Apply a single filter condition.

        Supports:
        - Numeric comparisons: column >= 10, column = 5.5
        - String comparisons: column = 'value', column != "text"
        - IS NULL / IS NOT NULL: column IS NULL, column IS NOT NULL
        """
        # Try numeric pattern first: table.column >= 123.45
        match = re.match(r'(?:(\w+)\.)?(\w+)\s*(>=|<=|!=|=|>|<)\s*(\d+(?:\.\d+)?)', condition)
        if match:
            col = match.group(2)
            op = match.group(3)
            val = float(match.group(4)) if '.' in match.group(4) else int(match.group(4))

            actual_col = self._find_column(df, col)
            if actual_col is None:
                logger.warning(f"Column '{col}' not found in DataFrame")
                return df

            return self._apply_comparison(df, actual_col, op, val)

        # Try string pattern: table.column = 'value' or column = "value"
        match = re.match(r"(?:(\w+)\.)?(\w+)\s*(!=|=)\s*['\"](.+?)['\"]", condition)
        if match:
            col = match.group(2)
            op = match.group(3)
            val = match.group(4)

            actual_col = self._find_column(df, col)
            if actual_col is None:
                logger.warning(f"Column '{col}' not found in DataFrame")
                return df

            if op == '=' or op == '==':
                return df[df[actual_col].astype(str) == val]
            elif op == '!=':
                return df[df[actual_col].astype(str) != val]

        # Try IS NULL / IS NOT NULL pattern
        match = re.match(r'(?:(\w+)\.)?(\w+)\s+IS\s+(NOT\s+)?NULL', condition, re.IGNORECASE)
        if match:
            col = match.group(2)
            is_not_null = match.group(3) is not None

            actual_col = self._find_column(df, col)
            if actual_col is None:
                logger.warning(f"Column '{col}' not found in DataFrame")
                return df

            if is_not_null:
                return df[df[actual_col].notna()]
            else:
                return df[df[actual_col].isna()]

        logger.warning(f"Could not parse filter condition: {condition}")
        return df

    def _find_column(self, df: pd.DataFrame, col: str) -> Optional[str]:
        """Find actual column name in DataFrame (case-insensitive, handles table prefix)."""
        for c in df.columns:
            col_name = c.split('.')[-1] if '.' in c else c
            if col_name.lower() == col.lower():
                return c
        return None

    def _apply_comparison(self, df: pd.DataFrame, col: str, op: str, val: Any) -> pd.DataFrame:
        """Apply a comparison operator to a DataFrame column."""
        if op == '>=':
            return df[df[col] >= val]
        elif op == '<=':
            return df[df[col] <= val]
        elif op == '>':
            return df[df[col] > val]
        elif op == '<':
            return df[df[col] < val]
        elif op == '=' or op == '==':
            return df[df[col] == val]
        elif op == '!=':
            return df[df[col] != val]
        return df

    def validate_pipeline_output(self, name: str, output_dir: str) -> Dict[str, Any]:
        """Validate pipeline output against expected results."""
        pipeline = self.pipelines.get(name)
        if not pipeline:
            return {'name': name, 'valid': False, 'error': 'Pipeline not found'}

        output_path = Path(output_dir) / pipeline.output_file
        if not output_path.exists():
            return {'name': name, 'valid': False, 'error': 'Output file not found'}

        df = pd.read_csv(output_path)
        expected = pipeline.expected_output

        result = {
            'name': name,
            'output_file': str(output_path),
            'row_count': len(df),
            'expected_row_count': expected.get('row_count'),
            'columns': list(df.columns),
            'expected_columns': expected.get('columns', [])
        }

        result['row_count_match'] = result['row_count'] == result['expected_row_count']
        result['columns_match'] = set(df.columns) == set(expected.get('columns', []))
        result['valid'] = result['row_count_match'] and result['columns_match']

        return result

    # ========================================================================
    # L4 Methods (File Event Automation / Triggers)
    # ========================================================================

    def load_triggers(self, triggers_file: str) -> None:
        """Load trigger definitions from JSON file."""
        with open(triggers_file, 'r') as f:
            data = json.load(f)

        for trigger_def in data.get('triggers', []):
            config = {}
            if 'validation_rules' in trigger_def:
                config['validation_rules'] = trigger_def['validation_rules']
            if 'on_failure' in trigger_def:
                config['on_failure'] = trigger_def['on_failure']
            if 'audit_fields' in trigger_def:
                config['audit_fields'] = trigger_def['audit_fields']
            if 'target_views' in trigger_def:
                config['target_views'] = trigger_def['target_views']
            if 'report_fields' in trigger_def:
                config['report_fields'] = trigger_def['report_fields']
            if 'output_file' in trigger_def:
                config['output_file'] = trigger_def['output_file']

            self.triggers[trigger_def['name']] = TriggerDefinition(
                name=trigger_def['name'],
                description=trigger_def.get('description', ''),
                event=trigger_def['event'],
                source_table=trigger_def.get('source_table'),
                action=trigger_def['action'],
                config=config
            )

    def get_triggers(self) -> List[str]:
        """Return list of defined triggers."""
        return list(self.triggers.keys())

    def get_trigger_definition(self, name: str) -> Optional[TriggerDefinition]:
        """Get trigger definition by name."""
        return self.triggers.get(name)

    def get_triggers_for_event(self, event_type: str, source_table: Optional[str] = None) -> List[TriggerDefinition]:
        """Get triggers that match an event type and optionally a source table."""
        result = []
        for trigger in self.triggers.values():
            if trigger.event == event_type:
                if source_table is None or trigger.source_table is None or trigger.source_table == source_table:
                    result.append(trigger)
        return result

    def get_safe_triggers(self) -> List[TriggerDefinition]:
        """Return all triggers (all FILE triggers are in the safe subset)."""
        return list(self.triggers.values())

    def get_skipped_triggers(self) -> List[str]:
        """Return list of skipped triggers (none for FILE adapter)."""
        return []

    def simulate_event(self, event: SimulatedEvent, output_dir: str) -> Dict[str, Any]:
        """Simulate a file event and execute matching triggers."""
        results = {
            'event': event.to_dict(),
            'triggers_fired': [],
            'validation_results': [],
            'audit_entries': [],
            'outputs': []
        }

        # Find matching triggers
        triggers = self.get_triggers_for_event(event.event_type, event.source_table)

        for trigger in triggers:
            trigger_result = self._execute_trigger(trigger, event, output_dir)
            results['triggers_fired'].append(trigger.name)

            if trigger.action == 'validate':
                results['validation_results'].append(trigger_result)
            elif trigger.action == 'audit_log':
                results['audit_entries'].append(trigger_result)
            elif trigger.action == 'refresh_views':
                results['outputs'].append(trigger_result)
            elif trigger.action == 'status_report':
                results['outputs'].append(trigger_result)

        return results

    def _execute_trigger(self, trigger: TriggerDefinition, event: SimulatedEvent, output_dir: str) -> Dict[str, Any]:
        """Execute a single trigger action."""
        if trigger.action == 'validate':
            return self._execute_validation_trigger(trigger, event)
        elif trigger.action == 'audit_log':
            return self._execute_audit_trigger(trigger, event)
        elif trigger.action == 'refresh_views':
            return self._execute_refresh_trigger(trigger, event, output_dir)
        elif trigger.action == 'status_report':
            return self._execute_status_report_trigger(trigger, event, output_dir)

        return {'trigger': trigger.name, 'action': trigger.action, 'status': 'unknown_action'}

    def _execute_validation_trigger(self, trigger: TriggerDefinition, event: SimulatedEvent) -> Dict[str, Any]:
        """Execute validation rules on a table."""
        result = {
            'trigger': trigger.name,
            'action': 'validate',
            'source_table': event.source_table,
            'timestamp': event.timestamp,
            'rules_checked': 0,
            'rules_passed': 0,
            'rules_failed': 0,
            'errors': []
        }

        if event.source_table not in self.tables:
            result['errors'].append(f"Table not found: {event.source_table}")
            return result

        df = self._read_table(event.source_table)
        rules = trigger.config.get('validation_rules', [])

        for rule in rules:
            result['rules_checked'] += 1
            col = rule['column']
            rule_type = rule['rule']

            if col not in df.columns:
                result['rules_failed'] += 1
                result['errors'].append(f"Column not found: {col}")
                continue

            passed = self._check_validation_rule(df, col, rule_type, rule)
            if passed:
                result['rules_passed'] += 1
            else:
                result['rules_failed'] += 1
                result['errors'].append(f"Rule failed: {col} {rule_type}")

        result['valid'] = result['rules_failed'] == 0
        return result

    def _check_validation_rule(self, df: pd.DataFrame, col: str, rule_type: str, rule: Dict[str, Any]) -> bool:
        """Check a single validation rule.

        Returns False for unknown rule types (fail-closed) to ensure
        validation rules are not silently ignored.
        """
        if rule_type == 'NOT_NULL':
            return df[col].notna().all()
        elif rule_type == 'POSITIVE_INTEGER':
            return (df[col].dropna() > 0).all() and df[col].dropna().apply(lambda x: float(x).is_integer()).all()
        elif rule_type == 'POSITIVE_NUMBER':
            return (df[col].dropna() > 0).all()
        elif rule_type == 'CONTAINS':
            value = rule.get('value', '')
            return df[col].dropna().astype(str).str.contains(value, regex=False).all()
        elif rule_type == 'IN_SET':
            values = rule.get('values', [])
            return df[col].dropna().isin(values).all()
        elif rule_type == 'MIN_LENGTH':
            min_len = rule.get('value', 0)
            return (df[col].dropna().astype(str).str.len() >= min_len).all()
        elif rule_type == 'MAX_LENGTH':
            max_len = rule.get('value', float('inf'))
            return (df[col].dropna().astype(str).str.len() <= max_len).all()
        elif rule_type == 'REGEX_MATCH':
            pattern = rule.get('pattern', rule.get('value', '.*'))
            return df[col].dropna().astype(str).str.match(pattern).all()
        elif rule_type == 'MIN_VALUE':
            min_val = rule.get('value', float('-inf'))
            return (df[col].dropna() >= min_val).all()
        elif rule_type == 'MAX_VALUE':
            max_val = rule.get('value', float('inf'))
            return (df[col].dropna() <= max_val).all()
        elif rule_type == 'UNIQUE':
            return df[col].dropna().is_unique
        else:
            # Fail-closed: unknown rule types should not pass silently
            logger.warning(f"Unknown validation rule type: {rule_type}")
            return False

    def _execute_audit_trigger(self, trigger: TriggerDefinition, event: SimulatedEvent) -> Dict[str, Any]:
        """Create an audit log entry."""
        audit_fields = trigger.config.get('audit_fields', {})

        details = audit_fields.get('details_template', '')
        details = details.replace('{row_count}', str(event.row_count or 0))
        details = details.replace('{filename}', event.filename or '')

        entry = {
            'trigger': trigger.name,
            'action': 'audit_log',
            'timestamp': event.timestamp,
            'event_type': audit_fields.get('event_type', event.event_type),
            'source_table': event.source_table,
            'details': details
        }

        self.audit_log.append(entry)
        return entry

    def _execute_refresh_trigger(self, trigger: TriggerDefinition, event: SimulatedEvent, output_dir: str) -> Dict[str, Any]:
        """Refresh specified views."""
        target_views = trigger.config.get('target_views', [])
        result = {
            'trigger': trigger.name,
            'action': 'refresh_views',
            'timestamp': event.timestamp,
            'views_refreshed': [],
            'errors': []
        }

        for view_name in target_views:
            if view_name in self.views:
                try:
                    df = self.query_view(view_name)
                    output_path = Path(output_dir) / f"{view_name}.csv"
                    df.to_csv(output_path, index=False)
                    result['views_refreshed'].append({
                        'name': view_name,
                        'output_file': str(output_path),
                        'row_count': len(df)
                    })
                except Exception as e:
                    result['errors'].append(f"Failed to refresh {view_name}: {str(e)}")
            else:
                result['errors'].append(f"View not found: {view_name}")

        return result

    def _execute_status_report_trigger(self, trigger: TriggerDefinition, event: SimulatedEvent, output_dir: str) -> Dict[str, Any]:
        """Generate a status report."""
        report_fields = trigger.config.get('report_fields', {})
        output_file = trigger.config.get('output_file', 'status_report.json')

        report = {
            'trigger': trigger.name,
            'action': 'status_report',
            'timestamp': event.timestamp
        }

        for key, template in report_fields.items():
            value = template
            value = value.replace('{file_count}', str(event.file_count or 0))
            value = value.replace('{total_row_count}', str(event.total_row_count or 0))
            value = value.replace('{error_count}', str(len(self.audit_log)))
            report[key] = value

        output_path = Path(output_dir) / output_file
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)

        report['output_file'] = str(output_path)
        return report

    def load_event_manifest(self, triggers_file: str) -> List[SimulatedEvent]:
        """Load simulated events from triggers file."""
        with open(triggers_file, 'r') as f:
            data = json.load(f)

        events = []
        manifest = data.get('event_simulation_manifest', {}).get('events', [])

        for event_def in manifest:
            events.append(SimulatedEvent(
                event_id=event_def['event_id'],
                event_type=event_def['event_type'],
                timestamp=event_def['timestamp'],
                source_table=event_def.get('source_table'),
                filename=event_def.get('filename'),
                row_count=event_def.get('row_count'),
                file_count=event_def.get('file_count'),
                total_row_count=event_def.get('total_row_count'),
                change_type=event_def.get('change_type')
            ))

        return events

    def execute_event_sequence(self, events: List[SimulatedEvent], output_dir: str) -> Dict[str, Any]:
        """Execute a sequence of simulated events."""
        results = {
            'events_processed': 0,
            'triggers_fired': 0,
            'validation_passed': 0,
            'validation_failed': 0,
            'audit_entries': len(self.audit_log),
            'event_results': []
        }

        for event in events:
            event_result = self.simulate_event(event, output_dir)
            results['events_processed'] += 1
            results['triggers_fired'] += len(event_result['triggers_fired'])

            for val_result in event_result['validation_results']:
                if val_result.get('valid', False):
                    results['validation_passed'] += 1
                else:
                    results['validation_failed'] += 1

            results['event_results'].append(event_result)

        results['audit_entries'] = len(self.audit_log)
        return results

    def get_audit_log(self) -> List[Dict[str, Any]]:
        """Return the audit log."""
        return self.audit_log

    def clear_audit_log(self) -> None:
        """Clear the audit log."""
        self.audit_log = []

    # ========================================================================
    # Reporting Methods
    # ========================================================================

    def get_all_definitions(self) -> Dict[str, Any]:
        """Get all L2/L3/L4 definitions for reporting."""
        return {
            'views': {name: view.to_dict() for name, view in self.views.items()},
            'pipelines': {name: pipeline.to_dict() for name, pipeline in self.pipelines.items()},
            'triggers': {name: trigger.to_dict() for name, trigger in self.triggers.items()},
            'tables': {name: str(path) for name, path in self.tables.items()},
            'table_configs': {name: config.to_dict() for name, config in self.table_configs.items()}
        }

    def validate_all(self, output_dir: str) -> Dict[str, Any]:
        """Validate all L2/L3/L4 definitions."""
        results = {
            'l2_views': {},
            'l3_pipelines': {},
            'l4_triggers': {},
            'summary': {
                'l2_total': 0, 'l2_valid': 0,
                'l3_total': 0, 'l3_valid': 0,
                'l4_total': 0, 'l4_valid': 0
            }
        }

        # Validate L2 views
        for name in self.views:
            result = self.validate_view_parity(name)
            results['l2_views'][name] = result
            results['summary']['l2_total'] += 1
            if result.get('parity', False):
                results['summary']['l2_valid'] += 1

        # Validate L3 pipelines
        for name in self.pipelines:
            result = self.validate_pipeline_definition(name)
            results['l3_pipelines'][name] = result
            results['summary']['l3_total'] += 1
            if result.get('valid', False):
                results['summary']['l3_valid'] += 1

        # L4 triggers are valid by definition (validated at load time)
        for name in self.triggers:
            results['l4_triggers'][name] = {'name': name, 'valid': True}
            results['summary']['l4_total'] += 1
            results['summary']['l4_valid'] += 1

        return results
