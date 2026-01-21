"""
Package analyzer for Oracle packages (Workstream 06.3).

Strategy: Analysis + optional stubbing ONLY.
NO automatic translation (packages are complex, dialect-specific).
"""

import re
from typing import List, Optional
from dataclasses import dataclass


@dataclass
class PackageMember:
    """A procedure or function within a package."""
    member_type: str  # 'procedure' or 'function'
    name: str
    parameters: List[str]
    return_type: Optional[str]  # Only for functions


@dataclass
class PackageAnalysis:
    """Analysis result for an Oracle package."""
    package_name: str
    has_spec: bool
    has_body: bool
    procedures: List[PackageMember]
    functions: List[PackageMember]
    dependencies: List[str]  # Tables/views/other packages referenced
    complexity_score: int  # 0-100
    warnings: List[str]
    manual_steps: List[str]


class PackageAnalyzer:
    """
    Conservative Oracle package analyzer.

    Capabilities:
    - Parse package spec and body
    - Extract procedures/functions
    - Identify dependencies
    - Generate complexity score
    - Produce analysis reports

    NON-capabilities (by design):
    - Automatic translation (too complex, dialect-specific)
    - Semantic equivalence claims (packages are Oracle-specific)
    """

    def __init__(self, source_dialect: str = "oracle", target_dialect: str = "postgres"):
        self.source_dialect = source_dialect
        self.target_dialect = target_dialect

    def analyze(self, package_def: str, package_name: str) -> PackageAnalysis:
        """
        Analyze Oracle package structure.

        Returns detailed analysis without attempting translation.
        """
        # Detect if we have spec, body, or both
        has_spec = self._has_package_spec(package_def)
        has_body = self._has_package_body(package_def)

        # Extract members (procedures and functions)
        procedures = self._extract_procedures(package_def)
        functions = self._extract_functions(package_def)

        # Identify dependencies
        dependencies = self._extract_dependencies(package_def)

        # Calculate complexity
        complexity = self._calculate_complexity(package_def, procedures, functions)

        # Generate warnings
        warnings = self._generate_warnings(package_def, procedures, functions, dependencies)

        # Generate manual steps checklist
        manual_steps = self._generate_manual_steps(package_name, procedures, functions)

        return PackageAnalysis(
            package_name=package_name,
            has_spec=has_spec,
            has_body=has_body,
            procedures=procedures,
            functions=functions,
            dependencies=dependencies,
            complexity_score=complexity,
            warnings=warnings,
            manual_steps=manual_steps
        )

    def _has_package_spec(self, package_def: str) -> bool:
        """Check if definition contains package specification."""
        pattern = r'CREATE\s+(OR\s+REPLACE\s+)?PACKAGE\s+\w+'
        return bool(re.search(pattern, package_def, re.IGNORECASE))

    def _has_package_body(self, package_def: str) -> bool:
        """Check if definition contains package body."""
        pattern = r'CREATE\s+(OR\s+REPLACE\s+)?PACKAGE\s+BODY\s+\w+'
        return bool(re.search(pattern, package_def, re.IGNORECASE))

    def _extract_procedures(self, package_def: str) -> List[PackageMember]:
        """Extract procedure declarations from package."""
        procedures = []
        seen_names = set()

        # Pattern 1: PROCEDURE name (params) - with parameters
        pattern_with_params = r'PROCEDURE\s+(\w+)\s*\((.*?)\)'
        for match in re.finditer(pattern_with_params, package_def, re.IGNORECASE | re.DOTALL):
            name = match.group(1)
            if name.upper() not in seen_names:
                seen_names.add(name.upper())
                params_str = match.group(2)
                params = self._parse_parameters(params_str)
                procedures.append(PackageMember(
                    member_type='procedure',
                    name=name,
                    parameters=params,
                    return_type=None
                ))

        # Pattern 2: PROCEDURE name; or PROCEDURE name IS/AS - without parameters
        pattern_no_params = r'PROCEDURE\s+(\w+)\s*(?:;|IS|AS)'
        for match in re.finditer(pattern_no_params, package_def, re.IGNORECASE):
            name = match.group(1)
            if name.upper() not in seen_names:
                seen_names.add(name.upper())
                procedures.append(PackageMember(
                    member_type='procedure',
                    name=name,
                    parameters=[],
                    return_type=None
                ))

        return procedures

    def _extract_functions(self, package_def: str) -> List[PackageMember]:
        """Extract function declarations from package."""
        functions = []
        seen_names = set()

        # Return type pattern: handles schema.type, VARCHAR2(10), table.column%TYPE, etc.
        return_type_pattern = r'[\w.]+(?:\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\))?(?:%TYPE|%ROWTYPE)?'

        # Pattern 1: FUNCTION name (params) RETURN type - with parameters
        pattern_with_params = r'FUNCTION\s+(\w+)\s*\((.*?)\)\s+RETURN\s+(' + return_type_pattern + r')'
        for match in re.finditer(pattern_with_params, package_def, re.IGNORECASE | re.DOTALL):
            name = match.group(1)
            if name.upper() not in seen_names:
                seen_names.add(name.upper())
                params_str = match.group(2)
                return_type = match.group(3).strip()
                params = self._parse_parameters(params_str)
                functions.append(PackageMember(
                    member_type='function',
                    name=name,
                    parameters=params,
                    return_type=return_type
                ))

        # Pattern 2: FUNCTION name RETURN type - without parameters
        pattern_no_params = r'FUNCTION\s+(\w+)\s+RETURN\s+(' + return_type_pattern + r')'
        for match in re.finditer(pattern_no_params, package_def, re.IGNORECASE):
            name = match.group(1)
            if name.upper() not in seen_names:
                seen_names.add(name.upper())
                return_type = match.group(2).strip()
                functions.append(PackageMember(
                    member_type='function',
                    name=name,
                    parameters=[],
                    return_type=return_type
                ))

        return functions

    def _parse_parameters(self, params_str: str) -> List[str]:
        """Parse parameter list."""
        if not params_str.strip():
            return []

        # Simple parsing: split by comma (doesn't handle nested types)
        params = [p.strip() for p in params_str.split(',')]
        return [p for p in params if p]

    def _extract_dependencies(self, package_def: str) -> List[str]:
        """Extract table/view/package references."""
        dependencies = set()

        # Look for FROM clauses
        from_pattern = r'FROM\s+(\w+)'
        for match in re.finditer(from_pattern, package_def, re.IGNORECASE):
            dependencies.add(match.group(1))

        # Look for JOIN clauses
        join_pattern = r'JOIN\s+(\w+)'
        for match in re.finditer(join_pattern, package_def, re.IGNORECASE):
            dependencies.add(match.group(1))

        # Look for INSERT INTO
        insert_pattern = r'INSERT\s+INTO\s+(\w+)'
        for match in re.finditer(insert_pattern, package_def, re.IGNORECASE):
            dependencies.add(match.group(1))

        # Look for UPDATE
        update_pattern = r'UPDATE\s+(\w+)'
        for match in re.finditer(update_pattern, package_def, re.IGNORECASE):
            dependencies.add(match.group(1))

        return sorted(list(dependencies))

    def _calculate_complexity(
        self,
        package_def: str,
        procedures: List[PackageMember],
        functions: List[PackageMember]
    ) -> int:
        """
        Calculate complexity score (0-100).

        Factors:
        - Number of members
        - Lines of code
        - Control flow statements
        - DML operations
        """
        score = 0

        # Base: number of members
        member_count = len(procedures) + len(functions)
        score += min(member_count * 5, 20)  # Cap at 20

        # Lines of code
        lines = len([line for line in package_def.split('\n') if line.strip()])
        score += min(lines // 10, 30)  # Cap at 30

        # Control flow (IF/LOOP/CASE)
        control_flow = len(re.findall(r'\b(IF|LOOP|CASE|FOR|WHILE)\b', package_def, re.IGNORECASE))
        score += min(control_flow * 2, 20)  # Cap at 20

        # DML operations (INSERT/UPDATE/DELETE)
        dml_ops = len(re.findall(r'\b(INSERT|UPDATE|DELETE)\b', package_def, re.IGNORECASE))
        score += min(dml_ops * 3, 15)  # Cap at 15

        # Cursors (advanced)
        cursors = len(re.findall(r'\bCURSOR\b', package_def, re.IGNORECASE))
        score += min(cursors * 5, 15)  # Cap at 15

        return min(score, 100)

    def _generate_warnings(
        self,
        package_def: str,
        procedures: List[PackageMember],
        functions: List[PackageMember],
        dependencies: List[str]
    ) -> List[str]:
        """Generate warnings about package complexity and features."""
        warnings = []

        # Warn about package complexity
        if len(procedures) + len(functions) > 10:
            warnings.append(f"High member count: {len(procedures)} procedures, {len(functions)} functions")

        # Warn about cursors
        if 'CURSOR' in package_def.upper():
            warnings.append("Package uses cursors (requires manual rewrite)")

        # Warn about DML
        if any(kw in package_def.upper() for kw in ['INSERT', 'UPDATE', 'DELETE']):
            warnings.append("Package contains DML operations (review for side effects)")

        # Warn about autonomous transactions
        if 'PRAGMA AUTONOMOUS_TRANSACTION' in package_def.upper():
            warnings.append("Package uses autonomous transactions (not portable)")

        # Warn about Oracle-specific features
        if 'ROWNUM' in package_def.upper():
            warnings.append("Package uses ROWNUM (Oracle-specific)")

        if 'CONNECT BY' in package_def.upper():
            warnings.append("Package uses hierarchical queries (CONNECT BY)")

        return warnings

    def _generate_manual_steps(
        self,
        package_name: str,
        procedures: List[PackageMember],
        functions: List[PackageMember]
    ) -> List[str]:
        """Generate manual steps checklist for package migration."""
        steps = []

        steps.append(f"Review {package_name} package specification and body")
        steps.append(f"Identify dependencies on other packages or schemas")

        if procedures:
            steps.append(f"Manually rewrite {len(procedures)} procedures in target dialect")

        if functions:
            steps.append(f"Manually rewrite {len(functions)} functions in target dialect")

        steps.append(f"Consider refactoring package into separate modules for {self.target_dialect}")
        steps.append(f"Create comprehensive test suite for package behavior")
        steps.append(f"Validate business logic equivalence after rewrite")

        return steps
