from typing import List, Dict, Any
from dataclasses import dataclass
import re


from core.schema_ir import RoutineIR, RoutineCapability, RoutineArgumentIR

@dataclass
class TranslationResult:
    routine_name: str
    original_ir: RoutineIR
    generated_code: str
    outcome: str # TRANSLATED, STUBBED, SKIPPED, ANALYZED_ONLY
    warnings: List[str]
    errors: List[str]

class RoutineTranslator:
    def __init__(self, source_dialect: str, target_dialect: str):
        self.source_dialect = source_dialect.lower()
        self.target_dialect = target_dialect.lower()
        
    def process(self, routine: RoutineIR, mode: RoutineCapability) -> TranslationResult:
        """
        Process a single routine based on the requested capability mode.
        """
        warnings = []
        errors = []
        
        if mode == RoutineCapability.NONE:
            return TranslationResult(routine.name, routine, "", "SKIPPED", [], [])
            
        # 1. Analyze (Always happens)
        risks = self._analyze_risk(routine)
        routine.risk_score = risks['score']
        routine.issues.extend(risks['issues'])
        warnings.extend(risks['issues'])

        if mode == RoutineCapability.ANALYZE:
            return TranslationResult(routine.name, routine, "", "ANALYZED_ONLY", warnings, errors)

        # 2. Translate or Stub
        if mode == RoutineCapability.SUBSET_TRANSLATE:
            # Check if safe to translate
            if self._is_safe_subset(routine, risks):
                try:
                    code = self._translate_subset(routine)
                    return TranslationResult(routine.name, routine, code, "TRANSLATED", warnings, errors)
                except Exception as e:
                    errors.append(f"Translation failed: {str(e)}")
                    # Fallthrough to stub
            else:
                warnings.append("Routine outside safe subset -> Fallback to STUB")

        # Fallback to STUB (for STUB mode or failed SUBSET)
        stub_code = self._generate_stub(routine)
        return TranslationResult(routine.name, routine, stub_code, "STUBBED", warnings, errors)

    def _analyze_risk(self, routine: RoutineIR) -> Dict[str, Any]:
        """Check for complex/unsafe features"""
        issues = []
        score = 0
        src = (routine.body_source or "").upper()
        
        # High Risk Features
        if "EXECUTE IMMEDIATE" in src:
            issues.append("Dynamic SQL (EXECUTE IMMEDIATE)")
            score += 50
        if "CURSOR " in src:
            issues.append("Explicit Cursors")
            score += 30
        if "PRAGMA " in src:
            issues.append("PRAGMA directives")
            score += 20
        if "DBMS_" in src or "UTL_" in src:
            issues.append("System Packages (DBMS_*/UTL_*)")
            score += 40
        if "EXCEPTION" in src:
            issues.append("Complex Exception Handling")
            score += 20
            
        return {'score': min(100, score), 'issues': issues}

    def _is_safe_subset(self, routine: RoutineIR, risks: Dict[str, Any]) -> bool:
        """Decide if we should attempt translation"""
        # If any major issues found, unsafe
        if risks['score'] > 20: # Threshold
            return False
        return True

    def _generate_stub(self, routine: RoutineIR) -> str:
        """Generate a valid, empty PL/pgSQL function/proc"""
        # Only supporting Postgres target for POC
        if 'postgres' not in self.target_dialect:
            return f"-- Stub generation not implemented for target {self.target_dialect}"

        # Signature
        args_str = self._format_args_postgres(routine.arguments)
        
        ret_clause = "void"
        if routine.return_type:
             # Basic mapping
             # In a real impl, resolve via TypeRegistry
             # Here we assume routine.return_type is a string from OracleAdapter for now or normalized
             # TODO: Use TypeRegistry properly
             ret_clause = self._map_type_str(routine.return_type)

        name = routine.name
        
        code = f"""
CREATE OR REPLACE FUNCTION {name}({args_str}) 
RETURNS {ret_clause}
LANGUAGE plpgsql
AS $$
BEGIN
    -- [SAIQL STUB] This routine was stubbed due to complexity or configuration.
    -- Issues detected: {', '.join(routine.issues)}
    -- PLEASE IMPLEMENT LOGIC HERE.
    
    RAISE NOTICE 'Calling stubbed routine {name}';
    RAISE EXCEPTION 'Routine {name} is not implemented';
    
    {'RETURN NULL;' if ret_clause != 'void' else 'RETURN;'}
END;
$$;
"""
        return code.strip()

    def _translate_subset(self, routine: RoutineIR) -> str:
        """
        Attempt a crude translation of the body.
        WARNING: This is valid ONLY for simple logic (IF/ELSE, basic SQL).
        """
        if 'postgres' not in self.target_dialect:
             raise NotImplementedError("Only Postgres target supported")

        # 1. Clean Body
        # Remove CREATE OR REPLACE ... line if present in source (Oracle 'all_source' often lacks header, 
        # but sometimes includes matching IS/AS)
        # We construct the header ourselves. We need the logic between BEGIN and END.
        
        src = routine.body_source or ""

        # Regex to extract body between BEGIN ... END
        # Very naive match.
        match = re.search(r'BEGIN(.*)END\s*;?', src, re.DOTALL | re.IGNORECASE)
        body_content = match.group(1) if match else src
        
        # 2. Apply Replacements
        body_content = self._apply_transforms(body_content)
        
        # 3. Wrap
        args_str = self._format_args_postgres(routine.arguments)
        ret_clause = self._map_type_str(routine.return_type) if routine.return_type else "void"
        
        code = f"""
CREATE OR REPLACE FUNCTION {routine.name}({args_str}) 
RETURNS {ret_clause}
LANGUAGE plpgsql
AS $$
DECLARE
    -- Variable declarations would go here (requires parsing)
BEGIN
{body_content}
END;
$$;
"""
        return code.strip()

    def _apply_transforms(self, sql: str) -> str:
        """Regex transforms for Oracle -> Postgres"""
        # SYSDATE -> CURRENT_TIMESTAMP
        sql = re.sub(r'\bSYSDATE\b', 'CURRENT_TIMESTAMP', sql, flags=re.IGNORECASE)
        # NVL -> COALESCE
        sql = re.sub(r'\bNVL\s*\(', 'COALESCE(', sql, flags=re.IGNORECASE)
        # FROM DUAL -> "" (in selects? Postgres supports distinct selecting without FROM, 
        # but keep it simple: SELECT 1 FROM DUAL -> SELECT 1)
        sql = re.sub(r'\s+FROM\s+DUAL\b', '', sql, flags=re.IGNORECASE)
        
        return sql

    def _format_args_postgres(self, args: List[RoutineArgumentIR]) -> str:
        parts = []
        for arg in args:
            t_str = self._map_type_str(arg.data_type)
            # Normalize mode: handle INOUT, IN/OUT, IN OUT variations (case-insensitive)
            mode_str = (arg.mode or "").upper().replace(" ", "").replace("/", "")
            mode = ""
            if mode_str == 'OUT':
                mode = "OUT "
            elif mode_str == 'INOUT':
                mode = "INOUT "
            # IN or empty -> no prefix (default)

            parts.append(f"{mode}{arg.name} {t_str}")
        return ", ".join(parts)

    # Oracle to PostgreSQL type mappings
    _TYPE_MAP = {
        'VARCHAR2': 'VARCHAR',
        'NVARCHAR2': 'VARCHAR',
        'CHAR': 'CHAR',
        'NCHAR': 'CHAR',
        'CLOB': 'TEXT',
        'NCLOB': 'TEXT',
        'BLOB': 'BYTEA',
        'RAW': 'BYTEA',
        'LONG': 'TEXT',
        'LONG RAW': 'BYTEA',
        'NUMBER': 'NUMERIC',
        'BINARY_FLOAT': 'REAL',
        'BINARY_DOUBLE': 'DOUBLE PRECISION',
        'PLS_INTEGER': 'INTEGER',
        'BINARY_INTEGER': 'INTEGER',
        'BOOLEAN': 'BOOLEAN',
        'DATE': 'TIMESTAMP',
        'TIMESTAMP': 'TIMESTAMP',
        'INTERVAL': 'INTERVAL',
        'ROWID': 'TEXT',
        'UROWID': 'TEXT',
        'XMLTYPE': 'XML',
        'SYS_REFCURSOR': 'REFCURSOR',
    }

    def _map_type_str(self, type_raw: Any) -> str:
        """Map Oracle type to PostgreSQL type.

        Handles:
        - Simple types (VARCHAR2 -> VARCHAR)
        - Types with precision/scale (NUMBER(10,2) -> NUMERIC(10,2))
        - Unknown types fall back to TEXT for safety
        """
        if not isinstance(type_raw, str):
            return "TEXT"

        t = type_raw.strip().upper()

        # Extract base type and any precision/scale suffix
        # e.g., "NUMBER(10,2)" -> base="NUMBER", suffix="(10,2)"
        match = re.match(r'^(\w+(?:\s+\w+)?)\s*(\(.*\))?$', t)
        if match:
            base_type = match.group(1)
            suffix = match.group(2) or ""
        else:
            base_type = t
            suffix = ""

        # Look up mapping
        pg_type = self._TYPE_MAP.get(base_type)

        if pg_type:
            # Preserve precision/scale for types that support it
            if suffix and pg_type in ('VARCHAR', 'CHAR', 'NUMERIC'):
                return f"{pg_type}{suffix}"
            return pg_type

        # Unknown type - return TEXT as safe fallback
        # This ensures valid PostgreSQL syntax rather than passing through
        # potentially invalid Oracle types
        return "TEXT"
