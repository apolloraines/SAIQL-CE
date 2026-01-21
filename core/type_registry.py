from enum import Enum
from typing import Dict, Tuple, Optional

class IRType(Enum):
    # Numeric
    SMALLINT = "SMALLINT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    DECIMAL = "DECIMAL"  # With precision/scale
    REAL = "REAL"
    DOUBLE = "DOUBLE PRECISION"

    # String
    CHAR = "CHAR"  # Fixed length
    VARCHAR = "VARCHAR"  # Variable length
    TEXT = "TEXT"  # Unlimited

    # Binary
    BYTEA = "BYTEA"

    # Date/Time
    DATE = "DATE"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_TZ = "TIMESTAMP WITH TIME ZONE"

    # Boolean
    BOOLEAN = "BOOLEAN"

    # Special
    UUID = "UUID"
    JSON = "JSON"
    JSONB = "JSONB"

    # Fallback
    UNKNOWN = "UNKNOWN"

class TypeInfo:
    def __init__(self, ir_type: IRType, precision: Optional[int] = None,
                 scale: Optional[int] = None, length: Optional[int] = None):
        self.ir_type = ir_type
        self.precision = precision
        self.scale = scale
        self.length = length
    
    def __repr__(self):
        return f"TypeInfo({self.ir_type.value}, p={self.precision}, s={self.scale}, l={self.length})"

class TypeRegistry:
    # Source type → IR type mappings
    # Format: dialect -> base_type -> (IRType, precision_rule, scale_rule)
    # rule='EXTRACT' means take from source type definition
    SOURCE_TO_IR: Dict[str, Dict[str, Tuple[IRType, Optional[str], Optional[str]]]] = {
        'postgres': {
            'smallint': (IRType.SMALLINT, None, None),
            'integer': (IRType.INTEGER, None, None),
            'bigint': (IRType.BIGINT, None, None),
            'numeric': (IRType.DECIMAL, 'EXTRACT', 'EXTRACT'),
            'real': (IRType.REAL, None, None),
            'double precision': (IRType.DOUBLE, None, None),
            'varchar': (IRType.VARCHAR, 'EXTRACT', None),
            'character varying': (IRType.VARCHAR, 'EXTRACT', None),
            'text': (IRType.TEXT, None, None),
            'bytea': (IRType.BYTEA, None, None),
            'boolean': (IRType.BOOLEAN, None, None),
            'date': (IRType.DATE, None, None),
            'timestamp': (IRType.TIMESTAMP, None, None),
            'timestamp without time zone': (IRType.TIMESTAMP, None, None),
            'timestamp with time zone': (IRType.TIMESTAMP_TZ, None, None),
            'timestamptz': (IRType.TIMESTAMP_TZ, None, None),
            'uuid': (IRType.UUID, None, None),
            'json': (IRType.JSON, None, None),
            'jsonb': (IRType.JSONB, None, None),
        },
        'mysql': {
            'tinyint': (IRType.SMALLINT, None, None),
            'smallint': (IRType.SMALLINT, None, None),
            'int': (IRType.INTEGER, None, None),
            'bigint': (IRType.BIGINT, None, None),
            'decimal': (IRType.DECIMAL, 'EXTRACT', 'EXTRACT'),
            'float': (IRType.REAL, None, None),
            'double': (IRType.DOUBLE, None, None),
            'varchar': (IRType.VARCHAR, 'EXTRACT', None),
            'text': (IRType.TEXT, None, None),
            'longtext': (IRType.TEXT, None, None),
            'blob': (IRType.BYTEA, None, None),
            'longblob': (IRType.BYTEA, None, None),
            'tinyint(1)': (IRType.BOOLEAN, None, None),
            'date': (IRType.DATE, None, None),
            'datetime': (IRType.TIMESTAMP, None, None),
            'timestamp': (IRType.TIMESTAMP_TZ, None, None),
            'json': (IRType.JSON, None, None),
            'binary': (IRType.BYTEA, None, None),
            'varbinary': (IRType.BYTEA, None, None),
        },
        'sqlite': {
            'integer': (IRType.INTEGER, None, None),
            'real': (IRType.DOUBLE, None, None),
            'text': (IRType.TEXT, None, None),
            'blob': (IRType.BYTEA, None, None),
            'boolean': (IRType.BOOLEAN, None, None),
            'date': (IRType.DATE, None, None),
            'datetime': (IRType.TIMESTAMP, None, None),
            'timestamp': (IRType.TIMESTAMP, None, None),
        },
        'oracle': {
            'NUMBER': (IRType.DECIMAL, 'EXTRACT', 'EXTRACT'),
            'FLOAT': (IRType.DOUBLE, None, None),
            'BINARY_FLOAT': (IRType.REAL, None, None),
            'BINARY_DOUBLE': (IRType.DOUBLE, None, None),
            'VARCHAR2': (IRType.VARCHAR, 'EXTRACT', None),
            'NVARCHAR2': (IRType.VARCHAR, 'EXTRACT', None),
            'CHAR': (IRType.CHAR, 'EXTRACT', None),
            'NCHAR': (IRType.CHAR, 'EXTRACT', None),
            'CLOB': (IRType.TEXT, None, None),
            'NCLOB': (IRType.TEXT, None, None),
            'BLOB': (IRType.BYTEA, None, None),
            'RAW': (IRType.BYTEA, None, None),
            'LONG': (IRType.TEXT, None, None),
            'LONG RAW': (IRType.BYTEA, None, None),
            'DATE': (IRType.TIMESTAMP, None, None),
            'TIMESTAMP': (IRType.TIMESTAMP, None, None),
            'TIMESTAMP WITH TIME ZONE': (IRType.TIMESTAMP_TZ, None, None),
            'TIMESTAMP WITH LOCAL TIME ZONE': (IRType.TIMESTAMP_TZ, None, None),
        },
        'duckdb': {
             'integer': (IRType.INTEGER, None, None),
             'bigint': (IRType.BIGINT, None, None),
             'varchar': (IRType.VARCHAR, None, None),
             'double': (IRType.DOUBLE, None, None),
             'boolean': (IRType.BOOLEAN, None, None),
             'timestamp': (IRType.TIMESTAMP, None, None),
        },
        'mssql': {
            'tinyint': (IRType.SMALLINT, None, None),
            'smallint': (IRType.SMALLINT, None, None),
            'int': (IRType.INTEGER, None, None),
            'bigint': (IRType.BIGINT, None, None),
            'bit': (IRType.BOOLEAN, None, None),
            'decimal': (IRType.DECIMAL, 'EXTRACT', 'EXTRACT'),
            'numeric': (IRType.DECIMAL, 'EXTRACT', 'EXTRACT'),
            'money': (IRType.DECIMAL, 19, 4),
            'smallmoney': (IRType.DECIMAL, 10, 4),
            'float': (IRType.DOUBLE, None, None),
            'real': (IRType.REAL, None, None),
            'date': (IRType.DATE, None, None),
            'datetime': (IRType.TIMESTAMP, None, None),
            'datetime2': (IRType.TIMESTAMP, None, None),
            'datetimeoffset': (IRType.TIMESTAMP_TZ, None, None),
            'char': (IRType.CHAR, 'EXTRACT', None),
            'varchar': (IRType.VARCHAR, 'EXTRACT', None),
            'nchar': (IRType.CHAR, 'EXTRACT', None),
            'nvarchar': (IRType.VARCHAR, 'EXTRACT', None),
            'text': (IRType.TEXT, None, None),
            'ntext': (IRType.TEXT, None, None),
            'binary': (IRType.BYTEA, None, None),
            'varbinary': (IRType.BYTEA, None, None),
            'image': (IRType.BYTEA, None, None),
            'uniqueidentifier': (IRType.UUID, None, None),
            'xml': (IRType.TEXT, None, None),
        },
        'hana': {
            # Exact mappings (Phase 07)
            'boolean': (IRType.BOOLEAN, None, None),
            'tinyint': (IRType.SMALLINT, None, None),  # Upcast for safety
            'smallint': (IRType.SMALLINT, None, None),
            'integer': (IRType.INTEGER, None, None),
            'bigint': (IRType.BIGINT, None, None),
            'real': (IRType.REAL, None, None),
            'double': (IRType.DOUBLE, None, None),
            'char': (IRType.CHAR, 'EXTRACT', None),
            'nchar': (IRType.CHAR, 'EXTRACT', None),
            'varchar': (IRType.VARCHAR, 'EXTRACT', None),
            'nvarchar': (IRType.VARCHAR, 'EXTRACT', None),
            'clob': (IRType.TEXT, None, None),
            'nclob': (IRType.TEXT, None, None),
            'date': (IRType.DATE, None, None),
            'time': (IRType.TIME, None, None),
            'timestamp': (IRType.TIMESTAMP, None, None),
            'binary': (IRType.BYTEA, 'EXTRACT', None),
            'varbinary': (IRType.BYTEA, 'EXTRACT', None),
            # Lossy mappings (Phase 07)
            'decimal': (IRType.DECIMAL, 'EXTRACT', 'EXTRACT'),
            'smalldecimal': (IRType.DECIMAL, 16, 0),  # Lossy: max 16 digit precision
            'seconddate': (IRType.TIMESTAMP, None, None),  # Lossy: sub-second truncation
            'blob': (IRType.BYTEA, None, None),  # Lossy: size limit (1GB Postgres vs 2GB HANA)
            # Partial support (Phase 07)
            'shorttext': (IRType.VARCHAR, 'EXTRACT', None),  # Loses fuzzy search capability
            # Unsupported types raise errors at runtime (not mapped here)
            # ST_GEOMETRY, ST_POINT, ALPHANUM, TEXT (HANA full-text), BINTEXT
        }
    }

    # IR type → Target type mappings
    IR_TO_TARGET: Dict[str, Dict[IRType, str]] = {
        'postgres': {
            IRType.SMALLINT: 'SMALLINT',
            IRType.INTEGER: 'INTEGER',
            IRType.BIGINT: 'BIGINT',
            IRType.DECIMAL: 'NUMERIC',
            IRType.REAL: 'REAL',
            IRType.DOUBLE: 'DOUBLE PRECISION',
            IRType.VARCHAR: 'VARCHAR',
            IRType.TEXT: 'TEXT',
            IRType.CHAR: 'CHAR',
            IRType.BYTEA: 'BYTEA',
            IRType.BOOLEAN: 'BOOLEAN',
            IRType.DATE: 'DATE',
            IRType.TIMESTAMP: 'TIMESTAMP',
            IRType.TIMESTAMP_TZ: 'TIMESTAMP WITH TIME ZONE',
            IRType.UUID: 'UUID',
            IRType.JSON: 'JSON',
            IRType.JSONB: 'JSONB',
        },
        'mysql': {
            IRType.SMALLINT: 'SMALLINT',
            IRType.INTEGER: 'INT',
            IRType.BIGINT: 'BIGINT',
            IRType.DECIMAL: 'DECIMAL',
            IRType.REAL: 'FLOAT',
            IRType.DOUBLE: 'DOUBLE',
            IRType.VARCHAR: 'VARCHAR',
            IRType.TEXT: 'TEXT',
            IRType.CHAR: 'CHAR',
            IRType.BYTEA: 'LONGBLOB',
            IRType.BOOLEAN: 'TINYINT(1)',
            IRType.DATE: 'DATE',
            IRType.TIMESTAMP: 'DATETIME',
            IRType.TIMESTAMP_TZ: 'TIMESTAMP',
            IRType.UUID: 'CHAR(36)',
            IRType.JSON: 'JSON',
            IRType.JSONB: 'JSON',
        },
        'sqlite': {
            IRType.SMALLINT: 'INTEGER',
            IRType.INTEGER: 'INTEGER',
            IRType.BIGINT: 'INTEGER',
            IRType.DECIMAL: 'REAL',
            IRType.REAL: 'REAL',
            IRType.DOUBLE: 'REAL',
            IRType.VARCHAR: 'TEXT',
            IRType.TEXT: 'TEXT',
            IRType.CHAR: 'TEXT',
            IRType.BYTEA: 'BLOB',
            IRType.BOOLEAN: 'INTEGER',
            IRType.DATE: 'TEXT',
            IRType.TIMESTAMP: 'TEXT',
            IRType.TIMESTAMP_TZ: 'TEXT',
            IRType.UUID: 'TEXT',
            IRType.JSON: 'TEXT',
            IRType.JSONB: 'TEXT',
        },
        'mssql': {
            IRType.SMALLINT: 'SMALLINT',
            IRType.INTEGER: 'INT',
            IRType.BIGINT: 'BIGINT',
            IRType.DECIMAL: 'DECIMAL',
            IRType.REAL: 'REAL',
            IRType.DOUBLE: 'FLOAT',
            IRType.VARCHAR: 'NVARCHAR',
            IRType.TEXT: 'NVARCHAR(MAX)',
            IRType.CHAR: 'NCHAR',
            IRType.BYTEA: 'VARBINARY(MAX)',
            IRType.BOOLEAN: 'BIT',
            IRType.DATE: 'DATE',
            IRType.TIMESTAMP: 'DATETIME2',
            IRType.TIMESTAMP_TZ: 'DATETIMEOFFSET',
            IRType.UUID: 'UNIQUEIDENTIFIER',
            IRType.JSON: 'NVARCHAR(MAX)',
            IRType.JSONB: 'NVARCHAR(MAX)',
        }
    }

    @staticmethod
    def map_to_ir(source_dialect: str, source_type: str) -> TypeInfo:
        """Map source type to IR type"""
        source_type_lower = source_type.lower().strip()
        dialect_lower = source_dialect.lower()
        if 'postgres' in dialect_lower: dialect_lower = 'postgres'
        if 'mysql' in dialect_lower: dialect_lower = 'mysql'
        if 'oracle' in dialect_lower: dialect_lower = 'oracle'
        if 'mssql' in dialect_lower: dialect_lower = 'mssql'
        if 'hana' in dialect_lower: dialect_lower = 'hana'
        
        # Oracle special case: handle uppercase keys in mapping if needed, but we normalized to lower in dict keys
        # Wait, Oracle dict keys are upper case in SOURCE_TO_IR. Let's fix that normalization.
        # Actually, let's just make everything lower case for lookup.

        base_type, precision, scale, length = TypeRegistry._parse_type_string(source_type_lower)

        if dialect_lower not in TypeRegistry.SOURCE_TO_IR:
            return TypeInfo(IRType.UNKNOWN)

        # 1. Try exact match of FULL string (e.g. "tinyint(1)")
        mapping = TypeRegistry.SOURCE_TO_IR[dialect_lower].get(source_type_lower)

        # 2. If not found, try base type (e.g. "tinyint")
        if not mapping:
             mapping = TypeRegistry.SOURCE_TO_IR[dialect_lower].get(base_type)
        
        # Fallback for Oracle (case sensitive keys in my dict? No, I should make them consistent)
        if not mapping and dialect_lower == 'oracle':
             mapping = TypeRegistry.SOURCE_TO_IR[dialect_lower].get(base_type.upper())

        if not mapping:
             # Try partial match for things like 'tinyint(1)' -> 'tinyint'
             for key, val in TypeRegistry.SOURCE_TO_IR[dialect_lower].items():
                 if base_type.startswith(key.lower()):
                     mapping = val
                     break
        
        if not mapping:
            return TypeInfo(IRType.UNKNOWN)

        ir_type, p_rule, s_rule = mapping

        final_precision = precision if p_rule == 'EXTRACT' else p_rule
        final_scale = scale if s_rule == 'EXTRACT' else s_rule
        
        # Helper: if extracting from rule that is None, stay None
        # if rule is integer (e.g. fixed precision), use it? 
        # The logic in previous draft was: if p_rule is 'EXTRACT', use parsed precision. else use p_rule literal.
        
        return TypeInfo(ir_type, final_precision, final_scale, length)

    @staticmethod
    def map_from_ir(target_dialect: str, type_info: TypeInfo) -> str:
        """Map IR type to target type"""
        dialect_lower = target_dialect.lower()
        if 'postgres' in dialect_lower: dialect_lower = 'postgres'
        elif 'mysql' in dialect_lower: dialect_lower = 'mysql'
        elif 'sqlite' in dialect_lower: dialect_lower = 'sqlite'
        elif 'mssql' in dialect_lower: dialect_lower = 'mssql'

        if dialect_lower not in TypeRegistry.IR_TO_TARGET:
            return 'TEXT'

        base_type = TypeRegistry.IR_TO_TARGET[dialect_lower].get(type_info.ir_type, 'TEXT')

        # Add precision/scale/length
        if type_info.ir_type == IRType.DECIMAL and type_info.precision:
            if type_info.scale:
                return f"{base_type}({type_info.precision},{type_info.scale})"
            else:
                return f"{base_type}({type_info.precision})"
        elif type_info.ir_type in (IRType.VARCHAR, IRType.CHAR) and type_info.length:
            return f"{base_type}({type_info.length})"

        return base_type

    @staticmethod
    def _parse_type_string(type_str: str) -> Tuple[str, Optional[int], Optional[int], Optional[int]]:
        """Parse 'varchar(255)' -> ('varchar', None, None, 255)
        Also handles 'timestamp(6) with time zone' -> ('timestamp with time zone', 6, None, 6)
        """
        import re
        # Capture: base_prefix, optional (precision, scale), and trailing modifiers
        match = re.match(r'([a-zA-Z0-9_]+)\s*(?:\((\d+)(?:,\s*(\d+))?\))?\s*(.*)', type_str)
        if not match:
            return (type_str.strip(), None, None, None)

        base_prefix = match.group(1).strip()
        precision = int(match.group(2)) if match.group(2) else None
        scale = int(match.group(3)) if match.group(3) else None
        trailing = match.group(4).strip() if match.group(4) else ""

        # Combine base prefix with trailing modifiers (e.g., "timestamp" + "with time zone")
        base = (base_prefix + ' ' + trailing).strip() if trailing else base_prefix
        length = precision  # Alias

        return (base, precision, scale, length)

    @staticmethod
    def is_lossy_conversion(source_dialect: str, source_type: str, target_dialect: str) -> Tuple[bool, Optional[str]]:
        """Check if conversion is lossy"""
        # TODO: Implement full matrix
        # 1. Map source to IR
        source_ir = TypeRegistry.map_to_ir(source_dialect, source_type)
        if source_ir.ir_type == IRType.UNKNOWN:
             return (False, None) # Can't judge if unknown

        # 2. Get target string (naive check)
        target_type_str = TypeRegistry.map_from_ir(target_dialect, source_ir)

        # 3. Check specific lossy pairs
        
        # Oracle NUMBER -> SQLite REAL
        if source_ir.ir_type == IRType.DECIMAL and 'sqlite' in target_dialect.lower():
            # SQLite stores decimals as REAL (float) or TEXT. Our map uses REAL.
            return (True, f"Precision loss: {source_dialect} {source_type} -> SQLite REAL (floating point)")

        # TimestampTZ -> MySQL/SQLite (TZ loss)
        if source_ir.ir_type == IRType.TIMESTAMP_TZ:
            if 'mysql' in target_dialect.lower() or 'sqlite' in target_dialect.lower():
                 return (True, f"Timezone loss: {source_dialect} {source_type} -> {target_dialect} {target_type_str} (UTC normalized)")

        # Boolean -> Oracle (No native bool)
        # (Though we support Oracle as Source only, if it were target...)
        
        # UUID -> MySQL (String/Binary)
        if source_ir.ir_type == IRType.UUID and 'mysql' in target_dialect.lower():
             return (False, None) # Acceptable mapping to CHAR(36) or BINARY(16) usually, maybe warning?
             # Let's flag it as 'Type change'
             # return (True, f"Type format change: UUID -> {target_type_str}")

        # Oracle Empty String -> NULL warning
        if 'oracle' in source_dialect.lower() and source_ir.ir_type in (IRType.VARCHAR, IRType.CHAR, IRType.TEXT):
             if 'oracle' not in target_dialect.lower():
                 return (True, f"Semantic change: Oracle {source_type} treats empty string as NULL. Target backend may distinguish them.")

        return (False, None)
