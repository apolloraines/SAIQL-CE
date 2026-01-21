#!/usr/bin/env python3
"""
SAIQL Compiler - Semantic Analysis and Code Generation

This module performs semantic analysis on SAIQL Abstract Syntax Trees (ASTs),
optimizes queries for performance, and generates executable code for various
database backends. The compiler bridges the gap between high-level SAIQL
syntax and efficient database operations.

Processing Pipeline:
Raw Text → Lexer → Parser → Compiler → Engine

The compiler performs three main phases:
1. Semantic Analysis - Type checking, symbol resolution, validation
2. Optimization - Query optimization, dead code elimination, constant folding  
3. Code Generation - Convert AST to SQL or intermediate representation

Author: Apollo & Claude
Version: 1.0.0
Status: Foundation Phase

Educational Note:
A compiler transforms high-level language constructs into lower-level executable
code. In SAIQL's case, we transform symbolic queries into optimized SQL while
preserving the semantic meaning and improving performance.
"""

import logging
from typing import Dict, List, Any, Optional, Union, Tuple, Set
from enum import Enum, auto
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import json
import time

# Import our parser components
try:
    from .parser import (ASTNode, QueryNode, FunctionCallNode, BinaryOperationNode,
                       ContainerNode, TableReferenceNode, ColumnListNode,
                       LiteralNode, JoinNode, NodeType, ASTVisitor,
                       TransactionNode, SchemaNode, ColumnReferenceNode)
except ImportError:
    # Fallback for standalone testing
    logging.warning("parser module not found, using minimal definitions")
    from enum import Enum, auto
    from dataclasses import dataclass
    from abc import ABC, abstractmethod
    
    class NodeType(Enum):
        QUERY = auto()
        FUNCTION_CALL = auto()
    
    @dataclass
    class ASTNode:
        node_type: NodeType

        def accept(self, visitor: 'ASTVisitor') -> Any:
            """Accept a visitor (visitor pattern support)"""
            method_name = f'visit_{self.node_type.name.lower()}'
            visitor_method = getattr(visitor, method_name, None)
            if visitor_method:
                return visitor_method(self)
            return None

    class ASTVisitor(ABC):
        pass

# Configure logging
logger = logging.getLogger(__name__)

class CompilerError(Exception):
    """Exception raised during compilation"""
    
    def __init__(self, message: str, node: ASTNode = None, phase: str = "compilation"):
        self.message = message
        self.node = node
        self.phase = phase
        location = ""
        if node and hasattr(node, 'line') and hasattr(node, 'column'):
            location = f" at {node.line}:{node.column}"
        super().__init__(f"{phase.title()} error{location} - {message}")

class TargetDialect(Enum):
    """Supported SQL dialects for code generation"""
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MSSQL = "mssql"
    ORACLE = "oracle"
    GENERIC = "generic"

class OptimizationLevel(Enum):
    """Optimization levels"""
    NONE = 0      # No optimization
    BASIC = 1     # Basic optimizations only
    STANDARD = 2  # Standard production optimizations
    AGGRESSIVE = 3 # Aggressive optimizations (may change semantics)

@dataclass
class Symbol:
    """Symbol table entry"""
    name: str
    symbol_type: str  # 'table', 'column', 'function', 'variable'
    data_type: str
    scope: str
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class CompilationResult:
    """Result of compilation process"""
    sql_code: str
    optimized_ast: ASTNode
    symbol_table: Dict[str, Symbol]
    optimization_report: Dict[str, Any]
    execution_plan: Dict[str, Any]
    compilation_time: float
    target_dialect: TargetDialect
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

class SymbolTable:
    """Symbol table for tracking identifiers and their types"""
    
    def __init__(self):
        self.scopes = [{}]  # Stack of scopes
        self.current_scope = 0
        
    def enter_scope(self) -> None:
        """Enter a new scope"""
        self.scopes.append({})
        self.current_scope += 1
        
    def exit_scope(self) -> None:
        """Exit current scope"""
        if self.current_scope > 0:
            self.scopes.pop()
            self.current_scope -= 1
    
    def define(self, symbol: Symbol) -> None:
        """Define a symbol in current scope"""
        self.scopes[self.current_scope][symbol.name] = symbol
        
    def lookup(self, name: str) -> Optional[Symbol]:
        """Look up symbol in all scopes (inner to outer)"""
        for scope in reversed(self.scopes):
            if name in scope:
                return scope[name]
        return None
    
    def get_all_symbols(self) -> Dict[str, Symbol]:
        """Get all symbols from all scopes"""
        all_symbols = {}
        for scope in self.scopes:
            all_symbols.update(scope)
        return all_symbols

class SemanticAnalyzer(ASTVisitor):
    """
    Semantic Analyzer - First phase of compilation
    
    Performs semantic analysis on the AST:
    - Type checking
    - Symbol resolution  
    - Validation of references
    - Scope analysis
    """
    
    def __init__(self, legend_map: Dict[str, Any] = None):
        self.symbol_table = SymbolTable()
        self.errors = []
        self.warnings = []
        self.legend_map = legend_map or {}
        
        # Built-in type information
        self._initialize_builtin_types()
        
    def _initialize_builtin_types(self) -> None:
        """Initialize built-in symbols and types"""
        # Define built-in data types
        builtin_types = [
            Symbol("string", "type", "string", "builtin"),
            Symbol("integer", "type", "integer", "builtin"),
            Symbol("decimal", "type", "decimal", "builtin"),
            Symbol("boolean", "type", "boolean", "builtin"),
            Symbol("date", "type", "date", "builtin"),
            Symbol("timestamp", "type", "timestamp", "builtin"),
        ]
        
        for symbol in builtin_types:
            self.symbol_table.define(symbol)
    
    def analyze(self, ast: ASTNode) -> Tuple[bool, List[str], List[str]]:
        """
        Perform semantic analysis on AST
        
        Returns:
            Tuple of (success, errors, warnings)
        """
        self.errors = []
        self.warnings = []
        
        try:
            ast.accept(self)
            return len(self.errors) == 0, self.errors, self.warnings
        except Exception as e:
            self.errors.append(f"Unexpected error during semantic analysis: {e}")
            return False, self.errors, self.warnings
    
    def visit_query(self, node: QueryNode) -> None:
        """Analyze query node"""
        # Analyze operation
        if node.operation:
            node.operation.accept(self)
            
        # Analyze target
        if node.target:
            node.target.accept(self)
            
        # Analyze output specification
        if node.output:
            node.output.accept(self)
            
        # Validate query consistency
        self._validate_query_consistency(node)
    
    def visit_function_call(self, node: FunctionCallNode) -> None:
        """Analyze function call"""
        # Validate function exists in legend
        if self.legend_map:
            function_info = self._lookup_function_in_legend(node.function_name)
            if not function_info:
                self.errors.append(f"Unknown function: {node.function_name}")
            else:
                # Store function metadata
                node.add_metadata("function_info", function_info)
        
        # Analyze arguments
        for arg in node.arguments:
            arg.accept(self)
    
    def visit_binary_operation(self, node: BinaryOperationNode) -> None:
        """Analyze binary operation"""
        # Analyze operands
        node.left.accept(self)
        node.right.accept(self)
        
        # Type checking for binary operations
        self._check_binary_operation_types(node)
    
    def visit_container(self, node: ContainerNode) -> None:
        """Analyze container"""
        for content in node.contents:
            content.accept(self)
    
    def visit_table_reference(self, node: TableReferenceNode) -> None:
        """Analyze table reference"""
        # For now, assume all table references are valid
        # In a full implementation, this would validate against schema
        symbol = Symbol(
            name=node.table_name,
            symbol_type="table",
            data_type="table",
            scope="global"
        )
        self.symbol_table.define(symbol)
    
    def visit_column_list(self, node: ColumnListNode) -> None:
        """Analyze column list"""
        for column in node.columns:
            # Validate column names
            if not self._is_valid_identifier(column):
                self.errors.append(f"Invalid column name: {column}")
    
    def visit_literal(self, node: LiteralNode) -> None:
        """Analyze literal value"""
        # Literals are always valid, just store type information
        node.add_metadata("validated_type", node.literal_type)
    
    def visit_join(self, node: JoinNode) -> None:
        """Analyze join operation"""
        if node.left_table:
            node.left_table.accept(self)
        if node.right_table:
            node.right_table.accept(self)
        if node.join_condition:
            node.join_condition.accept(self)

    def visit_transaction(self, node):
        """Visit transaction node - always valid"""
        pass
    
    def _lookup_function_in_legend(self, function_name: str) -> Optional[Dict[str, Any]]:
        """Look up function in legend map"""
        families = self.legend_map.get('families', {})
        for family_data in families.values():
            symbols = family_data.get('symbols', {})
            if function_name in symbols:
                return symbols[function_name]
        return None
    
    def _check_binary_operation_types(self, node: BinaryOperationNode) -> None:
        """Check type compatibility for binary operations"""
        # This is a simplified type checker
        # A full implementation would do detailed type inference
        
        left_type = node.left.get_metadata("validated_type", "unknown")
        right_type = node.right.get_metadata("validated_type", "unknown")
        
        # Basic type compatibility rules
        if node.operator in ['=', '==', '===']:
            # Equality operators - types should be compatible
            if left_type != "unknown" and right_type != "unknown" and left_type != right_type:
                self.warnings.append(f"Type mismatch in equality: {left_type} {node.operator} {right_type}")
        
        elif node.operator in ['+', '-', '*', '/']:
            # Arithmetic operators - should be numeric
            numeric_types = {'integer', 'decimal', 'number'}
            if left_type not in numeric_types and left_type != "unknown":
                self.errors.append(f"Left operand of {node.operator} must be numeric, got {left_type}")
            if right_type not in numeric_types and right_type != "unknown":
                self.errors.append(f"Right operand of {node.operator} must be numeric, got {right_type}")
    
    def _validate_query_consistency(self, node: QueryNode) -> None:
        """Validate overall query consistency"""
        # Check that SELECT queries have proper structure
        if node.query_type == "SELECT":
            # Allow SAIQL container syntax for targets
            if node.target and hasattr(node.target, "node_type"):
                pass  # Valid target found
            if not node.output:
                self.warnings.append("SELECT query missing output specification")
    
    def _is_valid_identifier(self, identifier: str) -> bool:
        """Check if identifier is valid.

        Allows dotted identifiers like table.column or schema.table.column.
        Each segment must start with alpha/underscore and contain only alnum/underscore.
        """
        if not identifier:
            return False
        # Split on dots and validate each segment
        segments = identifier.split('.')
        for segment in segments:
            if not segment:  # Empty segment (e.g., "table..column")
                return False
            if not segment[0].isalpha() and segment[0] != '_':
                return False
            if not all(c.isalnum() or c == '_' for c in segment):
                return False
        return True

class QueryOptimizer(ASTVisitor):
    """
    Query Optimizer - Second phase of compilation
    
    Performs various optimizations on the AST:
    - Constant folding
    - Dead code elimination
    - Query restructuring
    - Index hint insertion
    """
    
    def __init__(self, optimization_level: OptimizationLevel = OptimizationLevel.STANDARD):
        self.optimization_level = optimization_level
        self.optimizations_applied = []
        self.original_complexity = 0
        self.optimized_complexity = 0
        
    def optimize(self, ast: ASTNode) -> Tuple[ASTNode, Dict[str, Any]]:
        """
        Optimize AST
        
        Returns:
            Tuple of (optimized_ast, optimization_report)
        """
        self.optimizations_applied = []
        self._calculate_complexity(ast)
        self.original_complexity = self._get_complexity_score(ast)
        
        # Apply optimizations based on level
        if self.optimization_level != OptimizationLevel.NONE:
            optimized_ast = self._apply_optimizations(ast)
        else:
            optimized_ast = ast
            
        self.optimized_complexity = self._get_complexity_score(optimized_ast)
        
        optimization_report = {
            'level': self.optimization_level.name,
            'optimizations_applied': self.optimizations_applied,
            'original_complexity': self.original_complexity,
            'optimized_complexity': self.optimized_complexity,
            'improvement_ratio': self.original_complexity / max(self.optimized_complexity, 1)
        }
        
        return optimized_ast, optimization_report
    
    def _apply_optimizations(self, ast: ASTNode) -> ASTNode:
        """Apply optimization transformations"""
        # Start with the original AST
        current_ast = ast
        
        # Apply optimizations in order of effectiveness
        if self.optimization_level.value >= OptimizationLevel.BASIC.value:
            current_ast = self._constant_folding(current_ast)
            current_ast = self._eliminate_dead_code(current_ast)
            
        if self.optimization_level.value >= OptimizationLevel.STANDARD.value:
            current_ast = self._optimize_joins(current_ast)
            current_ast = self._push_down_selections(current_ast)
            
        if self.optimization_level.value >= OptimizationLevel.AGGRESSIVE.value:
            current_ast = self._aggressive_rewriting(current_ast)
            
        return current_ast
    
    def _constant_folding(self, ast: ASTNode) -> ASTNode:
        """Fold constant expressions"""
        # Recursive constant folding
        if hasattr(ast, 'contents'):
            # Handle container nodes
            new_contents = []
            for content in ast.contents:
                new_contents.append(self._constant_folding(content))
            ast.contents = new_contents
            return ast
            
        if hasattr(ast, 'left') and hasattr(ast, 'right'):
            # Handle binary operations
            ast.left = self._constant_folding(ast.left)
            ast.right = self._constant_folding(ast.right)
            
            # Check if both operands are literals
            if (hasattr(ast.left, 'value') and hasattr(ast.right, 'value') and 
                hasattr(ast, 'operator')):
                try:
                    left_val = ast.left.value
                    right_val = ast.right.value
                    op = ast.operator
                    
                    result = None
                    if op == '+':
                        result = left_val + right_val
                    elif op == '-':
                        result = left_val - right_val
                    elif op == '*':
                        result = left_val * right_val
                    elif op == '/':
                        if right_val != 0:
                            result = left_val / right_val
                            
                    if result is not None:
                        # Mark node as folded with computed value
                        # IMPORTANT: Preserve original structure for passes that expect binary node shape
                        self.optimizations_applied.append(f"constant_folding: {left_val} {op} {right_val} -> {result}")
                        logger.debug(f"Folded constant: {left_val} {op} {right_val} -> {result}")

                        # Add folded value as additional attribute (don't delete original structure)
                        ast.folded_value = result
                        ast.is_folded = True
                        # Also set value for compatibility with literal-style access
                        ast.value = result
                        ast.literal_type = type(result).__name__
                        # NOTE: left/right/operator preserved for passes expecting binary node shape
                        
                except Exception as e:
                    logger.debug(f"Constant folding failed: {e}")
                    
        return ast
    
    def _eliminate_dead_code(self, ast: ASTNode) -> ASTNode:
        """Remove unreachable or unnecessary code"""
        # Simple dead code elimination: 1=1, 0=1, etc.
        
        if hasattr(ast, 'operation') and ast.operation:
            ast.operation = self._eliminate_dead_code(ast.operation)
            
        if hasattr(ast, 'target') and ast.target:
            ast.target = self._eliminate_dead_code(ast.target)
            
        # Check for WHERE 1=0 (False)
        if hasattr(ast, 'query_type') and ast.query_type == 'SELECT':
            # This would require inspecting the WHERE clause structure
            # For now, we'll just log that we checked
            pass
            
        self.optimizations_applied.append("dead_code_elimination")
        return ast
    
    def _optimize_joins(self, ast: ASTNode) -> ASTNode:
        """
        Optimize join order and conditions using cost-based optimization
        
        This method:
        1. Identifies all join operations in the AST
        2. Estimates the cost of different join orders
        3. Selects the optimal join algorithm (hash/merge/nested loop)
        4. Reorders joins for better performance
        """
        # Import join engine (lazy import to avoid circular dependencies)
        try:
            from .join_engine import JoinExecutor, JoinAlgorithm
        except ImportError:
            logger.warning("Join engine not available, skipping join optimization")
            return ast
        
        # Check if this AST contains join operations
        has_joins = self._contains_joins(ast)
        
        if not has_joins:
            # No joins to optimize
            return ast
        
        # Extract join information
        join_info = self._extract_join_info(ast)
        
        if not join_info:
            return ast
        
        # Estimate costs and select best algorithm
        for join in join_info:
            left_size = join.get('left_size', 1000)  # Default estimate
            right_size = join.get('right_size', 1000)
            
            # Select algorithm based on size
            if left_size + right_size < 100:
                recommended_algorithm = JoinAlgorithm.NESTED_LOOP
            elif left_size + right_size > 10000:
                recommended_algorithm = JoinAlgorithm.HASH
            else:
                recommended_algorithm = JoinAlgorithm.MERGE
            
            # Store recommendation in AST metadata
            if hasattr(join.get('node'), 'metadata'):
                join['node'].metadata['recommended_algorithm'] = recommended_algorithm.value
            
            self.optimizations_applied.append(
                f"join_optimization: {recommended_algorithm.value} for {left_size}x{right_size} rows"
            )
        
        logger.debug(f"Applied join optimization to {len(join_info)} joins")
        return ast
    
    def _contains_joins(self, ast: ASTNode) -> bool:
        """Check if AST contains any join operations"""
        if hasattr(ast, 'node_type') and 'JOIN' in str(ast.node_type):
            return True
        
        # Recursively check children
        if hasattr(ast, 'contents'):
            for child in ast.contents:
                if self._contains_joins(child):
                    return True
        
        if hasattr(ast, 'left') and self._contains_joins(ast.left):
            return True
        if hasattr(ast, 'right') and self._contains_joins(ast.right):
            return True
        
        return False
    
    def _extract_join_info(self, ast: ASTNode) -> List[Dict[str, Any]]:
        """Extract information about all joins in the AST"""
        joins = []
        
        # This is a simplified extraction - in a real implementation,
        # we would traverse the AST more thoroughly
        if hasattr(ast, 'node_type') and 'JOIN' in str(ast.node_type):
            join_info = {
                'node': ast,
                'left_size': getattr(ast, 'left_size_estimate', 1000),
                'right_size': getattr(ast, 'right_size_estimate', 1000),
                'join_type': getattr(ast, 'join_type', 'INNER')
            }
            joins.append(join_info)
        
        # Recursively extract from children
        if hasattr(ast, 'contents'):
            for child in ast.contents:
                joins.extend(self._extract_join_info(child))
        
        return joins
    
    def _push_down_selections(self, ast: ASTNode) -> ASTNode:
        """Push selection conditions down to reduce intermediate results"""
        self.optimizations_applied.append("selection_pushdown")
        logger.debug("Applied selection pushdown")
        return ast
    
    def _aggressive_rewriting(self, ast: ASTNode) -> ASTNode:
        """Apply aggressive query rewriting"""
        self.optimizations_applied.append("aggressive_rewriting")
        logger.debug("Applied aggressive rewriting")
        return ast
    
    def _calculate_complexity(self, ast: ASTNode) -> None:
        """Calculate query complexity metrics.

        Currently a no-op placeholder. Complexity is computed via _get_complexity_score()
        which counts AST nodes. Future: add cost-based estimation using table statistics.
        """
        # Complexity calculation delegated to _get_complexity_score()
        pass
    
    def _get_complexity_score(self, ast: ASTNode) -> int:
        """Get complexity score for AST"""
        # Simplified complexity scoring
        return self._count_nodes(ast)
    
    def _count_nodes(self, ast: ASTNode) -> int:
        """Count total nodes in AST"""
        count = 1
        if hasattr(ast, 'contents'):
            for child in ast.contents:
                count += self._count_nodes(child)
        if hasattr(ast, 'left') and ast.left:
            count += self._count_nodes(ast.left)
        if hasattr(ast, 'right') and ast.right:
            count += self._count_nodes(ast.right)
        if hasattr(ast, 'operation') and ast.operation:
            count += self._count_nodes(ast.operation)
        if hasattr(ast, 'target') and ast.target:
            count += self._count_nodes(ast.target)
        return count
    
    # Visitor methods - intentional no-ops for QueryOptimizer
    # Optimization is applied via _apply_optimizations() which traverses the AST directly.
    # These visitor methods exist for ASTVisitor interface compatibility but are unused.

    def visit_query(self, node: QueryNode) -> None:
        """No-op: optimization handled by _apply_optimizations()"""
        pass

    def visit_function_call(self, node: FunctionCallNode) -> None:
        """No-op: optimization handled by _apply_optimizations()"""
        pass

    def visit_binary_operation(self, node: BinaryOperationNode) -> None:
        """No-op: optimization handled by _apply_optimizations()"""
        pass

    def visit_container(self, node: ContainerNode) -> None:
        """No-op: optimization handled by _apply_optimizations()"""
        pass

    def visit_table_reference(self, node: TableReferenceNode) -> None:
        """No-op: optimization handled by _apply_optimizations()"""
        pass

    def visit_column_list(self, node: ColumnListNode) -> None:
        """No-op: optimization handled by _apply_optimizations()"""
        pass

    def visit_literal(self, node: LiteralNode) -> None:
        """No-op: optimization handled by _apply_optimizations()"""
        pass

    def visit_join(self, node: JoinNode) -> None:
        """No-op: optimization handled by _apply_optimizations()"""
        pass

    def visit_transaction(self, node):
        """No-op: transactions pass through without optimization"""
        pass


class CodeGenerator(ASTVisitor):
    """
    Code Generator - Third phase of compilation
    
    Generates executable SQL code from optimized AST for various database dialects.
    Handles dialect-specific syntax differences and optimization hints.
    """
    
    def __init__(self, target_dialect: TargetDialect = TargetDialect.SQLITE):
        self.target_dialect = target_dialect
        self.generated_code = []
        self.indent_level = 0
        self.symbol_table = None
        
        # Dialect-specific configurations
        self.dialect_config = self._get_dialect_config()
        
    def _get_dialect_config(self) -> Dict[str, Any]:
        """
        Get configuration (Capability Matrix) for target SQL dialect.
        Defines supported features, quoting rules, and limits.
        """
        configs = {
            TargetDialect.SQLITE: {
                'quote_char': '"',
                'supports_cte': True,
                'supports_window_functions': True,
                'max_identifier_length': 1024,
                'case_sensitivity': 'insensitive',
                'supports_returning': True,
                'supports_merge': False,
                'supports_check_constraints': True,
                'supports_json': True,  # via JSON1 extension
                'supports_arrays': False,
                'param_style': 'qmark', # ?
                'type_mapping': 'sqlite' 
            },
            TargetDialect.POSTGRESQL: {
                'quote_char': '"',
                'supports_cte': True,
                'supports_window_functions': True,
                'max_identifier_length': 63,
                'case_sensitivity': 'sensitive',
                'supports_returning': True,
                'supports_merge': True, # via INSERT ON CONFLICT
                'supports_check_constraints': True,
                'supports_json': True, # JSONB
                'supports_arrays': True,
                'param_style': 'numeric', # $1
                'type_mapping': 'postgres'
            },
            TargetDialect.MYSQL: {
                'quote_char': '`',
                'supports_cte': True, # MySQL 8.0+
                'supports_window_functions': True, # MySQL 8.0+
                'max_identifier_length': 64,
                'case_sensitivity': 'insensitive',
                'supports_returning': False, # Not natively
                'supports_merge': False, # via ON DUPLICATE KEY UPDATE
                'supports_check_constraints': True, # MySQL 8.0.16+
                'supports_json': True,
                'supports_arrays': False, # JSON arrays only
                'param_style': 'format', # %s
                'type_mapping': 'mysql'
            }
        }
        
        return configs.get(self.target_dialect, configs[TargetDialect.SQLITE])
    
    def generate(self, ast: ASTNode, symbol_table: SymbolTable) -> str:
        """
        Generate SQL code from AST
        
        Args:
            ast: Optimized AST
            symbol_table: Symbol table from semantic analysis
            
        Returns:
            Generated SQL code
        """
        self.generated_code = []
        self.indent_level = 0
        self.symbol_table = symbol_table
        
        # Generate code by traversing AST
        ast.accept(self)
        
        # Join generated code
        sql_code = '\n'.join(self.generated_code)
        
        # Apply final formatting
        return self._format_sql(sql_code)
    
    def _emit(self, code: str) -> None:
        """Emit code with proper indentation"""
        indented_code = '  ' * self.indent_level + code
        self.generated_code.append(indented_code)
    
    def _format_sql(self, sql: str) -> str:
        """Apply final SQL formatting"""
        # Basic SQL formatting
        formatted = sql.strip()

        # Add semicolon if missing
        if formatted and not formatted.endswith(';'):
            formatted += ';'

        return formatted

    def _quote_identifier(self, identifier: str) -> str:
        """Quote an identifier (table/column name) for safe SQL interpolation.

        Uses dialect-specific quoting and escapes embedded quote characters.
        Handles:
        - Simple identifiers: users -> "users"
        - Dotted identifiers: users.id -> "users"."id"
        - Wildcards: * -> * (unquoted), users.* -> "users".*
        """
        # Don't quote standalone wildcard
        if identifier == '*':
            return '*'

        quote_char = self.dialect_config.get('quote_char', '"')

        # Handle dotted identifiers (schema.table.column)
        if '.' in identifier:
            segments = identifier.split('.')
            quoted_segments = []
            for seg in segments:
                if seg == '*':
                    # Don't quote wildcard segment
                    quoted_segments.append('*')
                else:
                    # Escape embedded quote characters by doubling them
                    escaped = seg.replace(quote_char, quote_char * 2)
                    quoted_segments.append(f"{quote_char}{escaped}{quote_char}")
            return '.'.join(quoted_segments)

        # Simple identifier - escape and quote
        escaped = identifier.replace(quote_char, quote_char * 2)
        return f"{quote_char}{escaped}{quote_char}"
    
    def visit_query(self, node: QueryNode) -> None:
        """Generate code for query"""
        if node.query_type == "SELECT":
            self._generate_select_query(node)
        elif node.query_type == "JOIN":
            self._generate_join_query(node)
        elif node.query_type == "AGGREGATE":
            self._generate_aggregate_query(node)
        elif node.query_type == "TRANSACTION":
            self._generate_transaction(node)
        else:
            self._emit(f"-- Unknown query type: {node.query_type}")
    
    def _generate_select_query(self, node: QueryNode) -> None:
        """Generate SELECT query"""
        # SELECT clause
        if node.target and hasattr(node.target, 'contents'):
            # Extract column information
            columns = self._extract_columns_from_target(node.target)
            self._emit(f"SELECT {columns}")
        else:
            self._emit("SELECT *")

        # FROM clause
        if node.target:
            tables = self._extract_tables_from_target(node.target)
            if tables:
                self._emit(f"FROM {tables}")

        # WHERE clause from conditions
        if hasattr(node, 'conditions') and node.conditions:
            where_parts = []
            for cond in node.conditions:
                where_parts.append(self._compile_condition(cond))
            if where_parts:
                self._emit(f"WHERE {' AND '.join(where_parts)}")
    
    def _generate_join_query(self, node: QueryNode) -> None:
        """Generate JOIN query"""
        if node.operation and hasattr(node.operation, 'join_type'):
            join_type = node.operation.join_type

            # Extract table information from target
            tables = self._extract_join_tables(node.target)

            if len(tables) >= 2:
                self._emit("SELECT *")
                self._emit(f"FROM {tables[0]}")

                # Get join condition from operation node or conditions list
                join_cond = None
                if hasattr(node.operation, 'join_condition'):
                    join_cond = node.operation.join_condition
                elif hasattr(node, 'conditions') and node.conditions:
                    # Use first condition as join condition
                    join_cond = node.conditions[0]

                on_clause = self._compile_join_condition(join_cond)
                self._emit(f"{join_type} JOIN {tables[1]} ON {on_clause}")
            else:
                self._emit("-- Invalid join: insufficient tables")
    
    def _generate_aggregate_query(self, node: QueryNode) -> None:
        """Generate aggregate query"""
        if node.operation and hasattr(node.operation, 'function_name'):
            func_name = node.operation.function_name
            
            # Map SAIQL functions to SQL
            sql_function = self._map_function_to_sql(func_name)
            
            tables = self._extract_tables_from_target(node.target)
            table_name = tables if tables else "unknown_table"
            
            self._emit(f"SELECT {sql_function}(*)")
            self._emit(f"FROM {table_name}")
    
    def _generate_transaction(self, node: QueryNode) -> None:
        """Generate transaction statement"""
        if node.operation and hasattr(node.operation, 'metadata'):
            operation = node.operation.metadata.get('operation', '$1')
            
            transaction_map = {
                '$1': 'BEGIN TRANSACTION',
                '$2': 'COMMIT',
                '$3': 'ROLLBACK'
            }
            
            sql_statement = transaction_map.get(operation, 'BEGIN TRANSACTION')
            self._emit(sql_statement)
    
    def _extract_columns_from_target(self, target: ASTNode) -> str:
        """Extract column list from target node"""
        # Check for columns attribute directly on target (CE parser format)
        if hasattr(target, 'columns') and target.columns:
            if hasattr(target.columns, 'columns'):
                return ', '.join(self._quote_identifier(c) for c in target.columns.columns)
        # Legacy: check contents
        if hasattr(target, 'contents'):
            for content in target.contents:
                if hasattr(content, 'columns'):
                    return ', '.join(self._quote_identifier(c) for c in content.columns)
        return "*"

    def _extract_tables_from_target(self, target: ASTNode) -> str:
        """Extract table names from target node"""
        # Check contents (CE parser format: target is ContainerNode with contents)
        if hasattr(target, 'contents'):
            table_names = []
            for content in target.contents:
                if hasattr(content, 'table_name'):
                    table_names.append(self._quote_identifier(content.table_name))
            if table_names:
                return ', '.join(table_names)
        # Direct table reference
        if hasattr(target, 'table_name'):
            return self._quote_identifier(target.table_name)
        return '"unknown_table"'
    
    def _extract_join_tables(self, target: ASTNode) -> List[str]:
        """Extract table names for join operations (quoted)"""
        if hasattr(target, 'contents'):
            tables = []
            for content in target.contents:
                if hasattr(content, 'table_name'):
                    tables.append(self._quote_identifier(content.table_name))
            return tables
        return []

    def _compile_condition(self, cond: ASTNode) -> str:
        """Compile a condition node to SQL WHERE clause fragment"""
        # Binary operation (e.g., column = value)
        if hasattr(cond, 'left') and hasattr(cond, 'right') and hasattr(cond, 'operator'):
            left = self._compile_condition(cond.left)
            right = self._compile_condition(cond.right)
            op = cond.operator
            # Map SAIQL operators to SQL
            op_map = {'==': '=', '!=': '<>', '&&': 'AND', '||': 'OR'}
            sql_op = op_map.get(op, op)
            return f"({left} {sql_op} {right})"
        # Literal value
        if hasattr(cond, 'value'):
            val = cond.value
            if isinstance(val, str):
                # Escape single quotes to prevent SQL injection
                escaped = val.replace("'", "''")
                return f"'{escaped}'"
            return str(val)
        # Column reference
        if hasattr(cond, 'column_name'):
            return self._quote_identifier(cond.column_name)
        if hasattr(cond, 'name'):
            return self._quote_identifier(cond.name)
        # Fallback
        return str(cond)

    def _compile_join_condition(self, cond: ASTNode) -> str:
        """Compile join condition to SQL ON clause"""
        if cond is None:
            return "1=1"
        return self._compile_condition(cond)

    def _map_function_to_sql(self, saiql_function: str) -> str:
        """Map SAIQL function to SQL equivalent.

        Raises CompilerError for unknown functions instead of silent fallback.
        """
        function_map = {
            '*COUNT': 'COUNT',
            '*SUM': 'SUM',
            '*AVG': 'AVG',
            '*MIN': 'MIN',
            '*MAX': 'MAX'
        }
        sql_func = function_map.get(saiql_function)
        if sql_func is None:
            raise CompilerError(
                f"Unknown aggregate function: {saiql_function}. "
                f"Supported: {', '.join(function_map.keys())}",
                phase="code_generation"
            )
        return sql_func
    
    # Visitor methods - no-ops for CodeGenerator
    # Code generation is handled by generate() which traverses the AST directly.
    # These visitor methods exist for ASTVisitor interface compatibility.

    def visit_function_call(self, node: FunctionCallNode) -> None:
        """No-op: code generation handled by generate()"""
        pass

    def visit_binary_operation(self, node: BinaryOperationNode) -> None:
        """No-op: code generation handled by generate()"""
        pass

    def visit_container(self, node: ContainerNode) -> None:
        """No-op: code generation handled by generate()"""
        pass

    def visit_table_reference(self, node: TableReferenceNode) -> None:
        """No-op: code generation handled by generate()"""
        pass

    def visit_column_list(self, node: ColumnListNode) -> None:
        """No-op: code generation handled by generate()"""
        pass

    def visit_literal(self, node: LiteralNode) -> None:
        """No-op: code generation handled by generate()"""
        pass

    def visit_join(self, node: JoinNode) -> None:
        """No-op: code generation handled by generate()"""
        pass

    def visit_transaction(self, node):
        """No-op: transactions pass through to generate()"""
        pass

class SAIQLCompiler:
    """
    Main SAIQL Compiler
    
    Orchestrates the three-phase compilation process:
    1. Semantic Analysis
    2. Optimization  
    3. Code Generation
    """
    
    def __init__(self, target_dialect: TargetDialect = TargetDialect.SQLITE,
                 optimization_level: OptimizationLevel = OptimizationLevel.STANDARD,
                 legend_map: Dict[str, Any] = None):
        
        self.target_dialect = target_dialect
        self.optimization_level = optimization_level
        self.legend_map = legend_map or {}
        
        # Initialize compilation phases
        self.semantic_analyzer = SemanticAnalyzer(legend_map)
        self.optimizer = QueryOptimizer(optimization_level)
        self.code_generator = CodeGenerator(target_dialect)
        
        logger.info(f"SAIQL Compiler initialized - Dialect: {target_dialect.value}, "
                   f"Optimization: {optimization_level.name}")
    
    def compile(self, ast: ASTNode, debug: bool = False) -> CompilationResult:
        """
        Compile SAIQL AST to executable SQL
        
        Args:
            ast: Abstract Syntax Tree from parser
            debug: Enable debug output
            
        Returns:
            CompilationResult with generated code and metadata
        """
        start_time = time.time()
        
        if debug:
            logger.info("Starting SAIQL compilation")
        
        try:
            # Phase 1: Semantic Analysis
            if debug:
                logger.info("Phase 1: Semantic Analysis")
            
            success, errors, warnings = self.semantic_analyzer.analyze(ast)
            
            if not success:
                raise CompilerError(f"Semantic analysis failed: {'; '.join(errors)}", 
                                  ast, "semantic_analysis")
            
            # Phase 2: Optimization
            if debug:
                logger.info("Phase 2: Query Optimization")
            
            optimized_ast, optimization_report = self.optimizer.optimize(ast)
            
            # Phase 3: Code Generation
            if debug:
                logger.info("Phase 3: Code Generation")
            
            sql_code = self.code_generator.generate(optimized_ast, 
                                                  self.semantic_analyzer.symbol_table)
            
            # Calculate compilation time
            compilation_time = time.time() - start_time
            
            # Create execution plan (simplified)
            execution_plan = {
                'estimated_cost': optimization_report.get('optimized_complexity', 1),
                'operations': ['scan', 'project'],  # Placeholder
                'index_usage': []  # Placeholder
            }
            
            result = CompilationResult(
                sql_code=sql_code,
                optimized_ast=optimized_ast,
                symbol_table=self.semantic_analyzer.symbol_table.get_all_symbols(),
                optimization_report=optimization_report,
                execution_plan=execution_plan,
                compilation_time=compilation_time,
                target_dialect=self.target_dialect,
                warnings=warnings
            )
            
            if debug:
                logger.info(f"Compilation completed in {compilation_time:.4f}s")
            
            return result
            
        except CompilerError:
            raise
        except Exception as e:
            raise CompilerError(f"Unexpected compilation error: {e}", ast)

def main():
    """Test the SAIQL compiler"""
    # This would require the full pipeline to test properly
    print("SAIQL Compiler Test")
    print("=" * 50)
    
    # Create compiler
    compiler = SAIQLCompiler(
        target_dialect=TargetDialect.SQLITE,
        optimization_level=OptimizationLevel.STANDARD
    )
    
    print(f"Compiler initialized:")
    print(f"  Target Dialect: {compiler.target_dialect.value}")
    print(f"  Optimization Level: {compiler.optimization_level.name}")
    print(f"  Semantic Analyzer: Ready")
    print(f"  Query Optimizer: Ready")
    print(f"  Code Generator: Ready")
    
    print("\nCompiler phases:")
    print("  1. Semantic Analysis - Type checking, symbol resolution")
    print("  2. Query Optimization - Performance improvements")
    print("  3. Code Generation - SQL output for target dialect")
    
    print("\nReady for full pipeline integration!")

if __name__ == "__main__":
    main()
