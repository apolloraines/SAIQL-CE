from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from enum import Enum

from core.type_registry import TypeInfo, IRType

@dataclass
class ColumnIR:
    """Column definition in IR"""
    name: str
    type: Any # TypeInfo object
    nullable: bool = True
    primary_key: bool = False
    default_value: Optional[str] = None
    collation: Optional[str] = None # Added support for collation
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class ConstraintIR:
    """Constraint definition"""
    name: str
    type: str  # PRIMARY_KEY, FOREIGN_KEY, UNIQUE, CHECK
    columns: List[str]
    definition: str  # For CHECK constraints or raw SQL
    ref_table: Optional[str] = None
    ref_columns: Optional[List[str]] = None

@dataclass
class TableIR:
    """Table definition in IR"""
    name: str
    columns: List[ColumnIR]
    constraints: List[ConstraintIR] = field(default_factory=list)
    indexes: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class SchemaIR:
    """Full database schema in IR"""
    tables: Dict[str, TableIR] = field(default_factory=dict)
    views: Dict[str, str] = field(default_factory=dict)  # name -> definition
    routines: Dict[str, 'RoutineIR'] = field(default_factory=dict)
    
    def add_table(self, table: TableIR):
        self.tables[table.name] = table

    def get_table(self, name: str) -> Optional[TableIR]:
        return self.tables.get(name)

    def add_routine(self, routine: 'RoutineIR'):
        self.routines[routine.name] = routine

class RoutineCapability(Enum):
    NONE = "none"
    ANALYZE = "analyze"
    STUB = "stub"
    SUBSET_TRANSLATE = "subset_translate"

@dataclass
class RoutineArgumentIR:
    name: str
    data_type: Any # TypeInfo
    mode: str = "IN" # IN, OUT, INOUT
    default_value: Optional[str] = None

@dataclass
class RoutineIR:
    """Routine (Function/Procedure) definition in IR"""
    name: str
    arguments: List[RoutineArgumentIR]
    return_type: Optional[Any] = None # TypeInfo, None for procedures
    body_source: str = "" # Original source code
    language: str = "SQL" # PL/SQL, TSQL, pgSQL
    dependencies: List[str] = field(default_factory=list) # Tables/Views/Procs used
    risk_score: int = 0
    issues: List[str] = field(default_factory=list) # Why logic might fail/warn
    metadata: Dict[str, Any] = field(default_factory=dict)

