#!/usr/bin/env python3
"""
SAIQL Parser - Syntax Analysis

This module performs syntax analysis on SAIQL token streams, building Abstract
Syntax Trees (ASTs) that represent the structure and meaning of queries. The
parser implements SAIQL grammar rules and handles complex nested expressions.

Processing Pipeline:
Raw Text → Lexer → Parser → Compiler → Engine

The parser transforms a flat stream of tokens into a hierarchical tree structure
that captures the semantic relationships between different parts of the query.

Author: Apollo & Claude
Version: 1.0.0  
Status: Foundation Phase

Educational Note:
A parser (syntax analyzer) is the second stage of language processing. It takes
tokens from the lexer and organizes them according to the language's grammar
rules, creating an Abstract Syntax Tree (AST) that represents the program's
structure in a way that's easy for later stages to process.
"""

import logging
from typing import List, Optional, Dict, Any, Union
from enum import Enum, auto
from dataclasses import dataclass, field
from abc import ABC, abstractmethod

# Import our lexer components
try:
    # Try relative import first (for package usage)
    from .lexer import Token, TokenType, SAIQLLexer
except ImportError:
    try:
        # Try absolute import (for standalone usage)
        from lexer import Token, TokenType, SAIQLLexer
    except ImportError:
        # Fallback for standalone testing without lexer
        logging.warning("lexer module not found, using minimal definitions")
        from enum import Enum, auto

        class TokenType(Enum):
            FUNCTION_SYMBOL = auto()
            CONTAINER_OPEN = auto()
            CONTAINER_CLOSE = auto()
            EOF = auto()

        @dataclass
        class Token:
            type: TokenType
            value: str
            position: int = 0
            line: int = 1
            column: int = 1
            length: int = 0

# Configure logging
logger = logging.getLogger(__name__)

class ParseError(Exception):
    """Exception raised during parsing"""
    
    def __init__(self, message: str, token: Token):
        self.message = message
        self.token = token
        super().__init__(f"Parse error at {token.line}:{token.column} - {message}")

class NodeType(Enum):
    """Types of AST nodes"""
    # Query Structure
    QUERY = auto()
    SELECT_QUERY = auto()
    INSERT_QUERY = auto()
    UPDATE_QUERY = auto()
    DELETE_QUERY = auto()
    JOIN_QUERY = auto()
    
    # Expressions
    FUNCTION_CALL = auto()
    BINARY_OPERATION = auto()
    UNARY_OPERATION = auto()
    ASSIGNMENT = auto()
    
    # Data Structures
    TABLE_REFERENCE = auto()
    COLUMN_REFERENCE = auto()
    COLUMN_LIST = auto()
    CONTAINER = auto()
    
    # Literals
    STRING_LITERAL = auto()
    NUMBER_LITERAL = auto()
    BOOLEAN_LITERAL = auto()
    NULL_LITERAL = auto()
    VECTOR_LITERAL = auto()

    # Control Structures
    TRANSACTION_BLOCK = auto()
    CONSTRAINT = auto()

    # AI-Native Operations
    VECTOR_OPERATION = auto()
    SIMILARITY_QUERY = auto()
    IMAGINATION_QUERY = auto()
    SCHEMA_OPERATION = auto()

@dataclass
class ASTNode(ABC):
    """
    Base class for Abstract Syntax Tree nodes
    
    Each node represents a syntactic construct in SAIQL and contains
    all information needed to understand and process that construct.
    """
    node_type: NodeType
    position: int = 0
    line: int = 1
    column: int = 1
    
    # Metadata for debugging and optimization
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @abstractmethod
    def accept(self, visitor):
        """Visitor pattern implementation"""
        pass
    
    def add_metadata(self, key: str, value: Any) -> None:
        """Add metadata to node"""
        self.metadata[key] = value
    
    def get_metadata(self, key: str, default: Any = None) -> Any:
        """Get metadata from node"""
        return self.metadata.get(key, default)

@dataclass
class QueryNode(ASTNode):
    """Root node for SAIQL queries"""
    query_type: str = "SELECT"
    operation: Optional[ASTNode] = None
    target: Optional[ASTNode] = None
    output: Optional[ASTNode] = None
    conditions: List[ASTNode] = field(default_factory=list)
    
    def accept(self, visitor):
        return visitor.visit_query(self)

@dataclass
class FunctionCallNode(ASTNode):
    """Function call node (*3, *COUNT, etc.)"""
    function_name: str = "unknown"
    arguments: List[ASTNode] = field(default_factory=list)
    symbol_family: Optional[str] = None
    semantic_meaning: Optional[str] = None
    
    def accept(self, visitor):
        return visitor.visit_function_call(self)

@dataclass
class BinaryOperationNode(ASTNode):
    """Binary operation node (+, =, etc.)"""
    operator: str = "unknown"           # ✅ Has default
    left: ASTNode = None               # ✅ Has default
    right: ASTNode = None              # ✅ Has default
    operator_type: Optional[str] = None

    def accept(self, visitor):
        return visitor.visit_binary_operation(self)

@dataclass
class ContainerNode(ASTNode):
    """Container node [table], {block}, (params)"""
    container_type: str = "["
    contents: List[ASTNode] = field(default_factory=list)
    
    def accept(self, visitor):
        return visitor.visit_container(self)

@dataclass
class TableReferenceNode(ASTNode):
    """Table reference node"""
    table_name: str = "unknown"
    alias: Optional[str] = None
    schema: Optional[str] = None
    
    def accept(self, visitor):
        return visitor.visit_table_reference(self)

@dataclass
class TransactionNode(ASTNode):
    """Transaction operation node"""
    operation: str = "BEGIN"
    
    def accept(self, visitor):
        return visitor.visit_transaction(self)

@dataclass
class SchemaNode(ASTNode):
    """Schema operation node"""
    operation: str = "CREATE"
    
    def accept(self, visitor):
        return visitor.visit_schema_operation(self)

@dataclass
class ColumnReferenceNode(ASTNode):
    """Column reference node"""
    column_name: str = "unknown"
    table_alias: Optional[str] = None
    
    def accept(self, visitor):
        return visitor.visit_column_reference(self)

@dataclass
class ColumnListNode(ASTNode):
    """Column list node (name,email,phone)"""
    columns: List[str] = field(default_factory=list)
    
    def accept(self, visitor):
        return visitor.visit_column_list(self)

@dataclass
class LiteralNode(ASTNode):
    """Literal value node"""
    value: Union[str, int, float, bool, None] = ""
    literal_type: str = "string"  # string, number, boolean, null
    is_null: bool = False  # Explicit flag for 3-value logic handling

    def accept(self, visitor):
        return visitor.visit_literal(self)

@dataclass
class JoinNode(ASTNode):
    """Join operation node"""
    join_type: str = "INNER"  # INNER, LEFT, RIGHT, etc.
    left_table: ASTNode = None
    right_table: ASTNode = None
    join_condition: Optional[ASTNode] = None
    
    def accept(self, visitor):
        return visitor.visit_join(self)


class SAIQLParser:
    """
    SAIQL Syntax Parser
    
    Implements recursive descent parsing for SAIQL grammar. Transforms
    token streams into Abstract Syntax Trees (ASTs) that represent the
    hierarchical structure of queries.
    
    Grammar Overview (simplified BNF):
    query          := operation '::' target '>>' output
    operation      := function_symbol | schema_op | transaction
    target         := container | expression
    container      := '[' table_list ']' | '{' block '}' | '(' params ')'
    table_list     := table ('+' table)*
    expression     := term (operator term)*
    """
    
    def __init__(self, debug: bool = False):
        """Initialize parser"""
        self.tokens = []
        self.current_token_index = 0
        self.current_token = None
        self.debug = debug
        
        # Grammar precedence rules (higher number = higher precedence)
        self.precedence = {
            '=': 1,
            '==': 1,
            '===': 1,
            '<': 1,
            '>': 1,
            '<=': 1,
            '>=': 1,
            '!=': 1,
            '+': 2,
            '++': 2,
            '::': 10,
            '>>': 5
        }
        
        logger.info("SAIQL Parser initialized")
    
    def parse(self, tokens: List[Token]) -> ASTNode:
        """
        Main parsing method - converts tokens to AST
        
        Args:
            tokens: List of tokens from lexer
            
        Returns:
            Root AST node representing the query
            
        Raises:
            ParseError: If syntax is invalid
        """
        self.tokens = tokens
        self.current_token_index = 0
        self.current_token = tokens[0] if tokens else None
        
        if self.debug:
            logger.info(f"Parsing {len(tokens)} tokens")
            for i, token in enumerate(tokens[:5]):  # Show first 5 tokens
                logger.info(f"  Token {i}: {token}")
        
        try:
            ast = self._parse_query()

            # Check for trailing tokens - query should consume all input
            if not self._match(TokenType.EOF):
                raise ParseError(
                    f"Unexpected trailing token: {self.current_token.value!r}",
                    self.current_token
                )

            if self.debug:
                logger.info(f"Parsing completed successfully, AST type: {ast.node_type}")

            return ast

        except Exception as e:
            if isinstance(e, ParseError):
                raise
            else:
                # Wrap unexpected errors
                raise ParseError(f"Unexpected error during parsing: {e}", 
                               self.current_token or Token(TokenType.EOF, "", 0, 1, 1, 0))
    
    def _advance(self) -> None:
        """Move to next token"""
        if self.current_token_index < len(self.tokens) - 1:
            self.current_token_index += 1
            self.current_token = self.tokens[self.current_token_index]
        else:
            self.current_token = Token(TokenType.EOF, "", 0, 1, 1, 0)
    
    def _peek(self, offset: int = 1) -> Optional[Token]:
        """Look ahead at future token without advancing"""
        peek_index = self.current_token_index + offset
        if peek_index < len(self.tokens):
            return self.tokens[peek_index]
        return None
    
    def _expect(self, expected_type: TokenType) -> Token:
        """
        Consume token of expected type or raise error
        
        Args:
            expected_type: Expected token type
            
        Returns:
            The consumed token
            
        Raises:
            ParseError: If token type doesn't match
        """
        if not self.current_token or self.current_token.type != expected_type:
            expected_name = expected_type.name if expected_type else "EOF"
            actual_name = self.current_token.type.name if self.current_token else "EOF"
            raise ParseError(f"Expected {expected_name}, got {actual_name}", 
                           self.current_token or Token(TokenType.EOF, "", 0, 1, 1, 0))
        
        token = self.current_token
        self._advance()
        return token
    
    def _match(self, *token_types: TokenType) -> bool:
        """Check if current token matches any of the given types"""
        if not self.current_token:
            return TokenType.EOF in token_types
        return self.current_token.type in token_types
    
    def _parse_query(self) -> QueryNode:
        """
        Parse complete SAIQL query
        
        Grammar: query := operation '::' target '>>' output
        """
        query_node = QueryNode(
            node_type=NodeType.QUERY,
            query_type="unknown",
            position=self.current_token.position if self.current_token else 0,
            line=self.current_token.line if self.current_token else 1,
            column=self.current_token.column if self.current_token else 1
        )
        
        # Parse operation (function, schema op, transaction, etc.)
        if self._match(TokenType.FUNCTION_SYMBOL, TokenType.SCHEMA_OP,
                      TokenType.TRANSACTION, TokenType.JOIN_SYMBOL):
            query_node.operation = self._parse_operation()
        else:
            # Enforce strict grammar: Query must start with an operation
            raise ParseError("Expected operation (*, =, @, $)",
                           self.current_token or Token(TokenType.EOF, "", 0, 1, 1, 0))

        # Parse table reference [table] if present (SAIQL format: *3[table]::columns>>output)
        if self._match(TokenType.CONTAINER_OPEN):
            query_node.target = self._parse_container()

        # Parse column list after namespace separator
        columns_node = None
        if self._match(TokenType.NAMESPACE_SEP):
            self._advance()  # consume '::'

            # Parse column list, wildcard, or single column
            if self._match(TokenType.WILDCARD):
                # Handle ::* (select all columns)
                token = self.current_token
                self._advance()  # consume '*'
                columns_node = ColumnListNode(
                    node_type=NodeType.COLUMN_LIST,
                    columns=['*'],
                    position=token.position,
                    line=token.line,
                    column=token.column
                )
            elif self._match(TokenType.COLUMN_LIST):
                columns_node = self._parse_column_list()
            elif self._match(TokenType.TABLE_NAME):
                # Column name(s) - may be comma-separated with spaces
                # e.g., "name, email" could tokenize as TABLE_NAME + COLUMN_LIST(',') + TABLE_NAME
                first_token = self.current_token
                columns = [first_token.value]
                self._advance()  # consume the first column name

                # Continue collecting columns if followed by commas or more columns
                while True:
                    if self._match(TokenType.COLUMN_LIST):
                        # Could be ',' or 'col1,col2' or 'col,'
                        token_val = self.current_token.value
                        self._advance()
                        parts = [p.strip() for p in token_val.split(',') if p.strip()]
                        columns.extend(parts)
                    elif self._match(TokenType.TABLE_NAME):
                        columns.append(self.current_token.value)
                        self._advance()
                    else:
                        break

                columns_node = ColumnListNode(
                    node_type=NodeType.COLUMN_LIST,
                    columns=columns,
                    position=first_token.position,
                    line=first_token.line,
                    column=first_token.column
                )

        # Attach columns to target if we have both
        if columns_node and query_node.target:
            query_node.target.columns = columns_node
        elif columns_node:
            query_node.target = columns_node

        # Expect output operator
        if self._match(TokenType.OUTPUT_OP):
            self._advance()  # consume '>>'
            
            # Parse output specification
            query_node.output = self._parse_output()
        
        # Determine query type based on operation
        if query_node.operation:
            query_node.query_type = self._determine_query_type(query_node.operation)
        
        return query_node
    
    def _parse_operation(self) -> ASTNode:
        """Parse operation part of query"""
        if self._match(TokenType.FUNCTION_SYMBOL):
            return self._parse_function_call()
        elif self._match(TokenType.JOIN_SYMBOL):
            return self._parse_join_operation()
        elif self._match(TokenType.SCHEMA_OP):
            return self._parse_schema_operation()
        elif self._match(TokenType.TRANSACTION):
            return self._parse_transaction()
        else:
            raise ParseError("Expected operation (function, join, schema, or transaction)", 
                           self.current_token)
    
    def _parse_function_call(self) -> FunctionCallNode:
        """Parse function call (*3, *COUNT, etc.)"""
        token = self._expect(TokenType.FUNCTION_SYMBOL)
        
        function_node = FunctionCallNode(
            node_type=NodeType.FUNCTION_CALL,
            function_name=token.value,
            position=token.position,
            line=token.line,
            column=token.column
        )
        
        # Add symbol metadata if available
        if hasattr(token, 'symbol_family') and token.symbol_family:
            function_node.symbol_family = token.symbol_family
            function_node.semantic_meaning = getattr(token, 'semantic_meaning', None)
        
        return function_node
    
    def _parse_join_operation(self) -> JoinNode:
        """Parse join operation (=J, =L, =R, etc.)"""
        token = self._expect(TokenType.JOIN_SYMBOL)
        
        # Map join symbols to SQL join types
        join_type_map = {
            '=J': 'INNER',
            '=L': 'LEFT',
            '=R': 'RIGHT', 
            '=F': 'FULL OUTER',
            '=C': 'CROSS',
            '=S': 'SELF',
            '=N': 'NATURAL',
            '=U': 'UNION'
        }
        
        join_type = join_type_map.get(token.value, 'INNER')
        
        join_node = JoinNode(
            node_type=NodeType.JOIN_QUERY,
            join_type=join_type,
            left_table=None,  # Will be filled by target parsing
            right_table=None,
            position=token.position,
            line=token.line,
            column=token.column
        )
        
        return join_node
    
    def _parse_schema_operation(self) -> ASTNode:
        """Parse schema operation (@1, @2, etc.)"""
        token = self._expect(TokenType.SCHEMA_OP)
        
        # Create schema operation node
        schema_node = SchemaNode(
            node_type=NodeType.SCHEMA_OPERATION,
            operation=token.value,
            position=token.position,
            line=token.line,
            column=token.column
        )
        # Node type set by constructor
        # Position set by constructor
        # Line set by constructor  
        # Column set by constructor
        # Operation set by constructor
        
        # Implement accept method for this instance
        # Accept method handled by SchemaNode class
        
        return schema_node
    
    def _parse_transaction(self) -> ASTNode:
        """Parse transaction operation ($1, $2, etc.)"""
        token = self._expect(TokenType.TRANSACTION)
        
        # Create transaction node
        transaction_node = TransactionNode(
            node_type=NodeType.TRANSACTION_BLOCK,
            operation=token.value,
            position=token.position,
            line=token.line,
            column=token.column
        )
        # Node type set by constructor
        # Position set by constructor
        # Line set by constructor
        # Column set by constructor
        # Operation set by constructor
        
        # Implement accept method for this instance
        # Accept method handled by TransactionNode class
        
        return transaction_node
    
    def _parse_target(self) -> ASTNode:
        """Parse target part of query (what operation acts on)"""
        if self._match(TokenType.CONTAINER_OPEN):
            return self._parse_container()
        elif self._match(TokenType.TABLE_NAME):
            return self._parse_table_reference()
        elif self._match(TokenType.COLUMN_LIST):
            return self._parse_column_list()
        else:
            return self._parse_expression()
    
    def _parse_container(self) -> ContainerNode:
        """Parse container [table] or [table1+table2]"""
        open_token = self._expect(TokenType.CONTAINER_OPEN)
        
        container_node = ContainerNode(
            node_type=NodeType.CONTAINER,
            container_type='[',
            position=open_token.position,
            line=open_token.line,
            column=open_token.column
        )
        
        # Parse contents until closing bracket
        while not self._match(TokenType.CONTAINER_CLOSE, TokenType.EOF):
            if self._match(TokenType.TABLE_NAME):
                table_node = self._parse_table_reference()
                container_node.contents.append(table_node)
            elif self._match(TokenType.ARITHMETIC_OP) and self.current_token.value == '+':
                # Join operator between tables
                self._advance()  # consume '+'
            else:
                # Handle other content types
                content_node = self._parse_expression()
                container_node.contents.append(content_node)
        
        self._expect(TokenType.CONTAINER_CLOSE)  # consume ']'
        
        return container_node
    
    def _parse_table_reference(self) -> TableReferenceNode:
        """Parse table reference"""
        token = self._expect(TokenType.TABLE_NAME)
        
        table_node = TableReferenceNode(
            node_type=NodeType.TABLE_REFERENCE,
            table_name=token.value,
            position=token.position,
            line=token.line,
            column=token.column
        )
        
        return table_node
    
    def _parse_column_list(self) -> ColumnListNode:
        """Parse column list (name,email,phone)

        Handles cases where columns with spaces are tokenized separately:
        - "name,email" -> single COLUMN_LIST token
        - "name, email" -> COLUMN_LIST('name,') + TABLE_NAME('email')
        - "name, email, phone" -> COLUMN_LIST('name,') + TABLE_NAME('email') + COLUMN_LIST(',') + TABLE_NAME('phone')
        """
        first_token = self._expect(TokenType.COLUMN_LIST)

        # Split comma-separated columns, filter empty strings from trailing commas
        columns = [col.strip() for col in first_token.value.split(',') if col.strip()]

        # Continue parsing if there are follow-on TABLE_NAME or COLUMN_LIST tokens
        # This handles "name, email" tokenized as COLUMN_LIST('name,') + TABLE_NAME('email')
        while True:
            if self._match(TokenType.TABLE_NAME):
                # Additional column name after space
                columns.append(self.current_token.value)
                self._advance()
            elif self._match(TokenType.COLUMN_LIST):
                # Could be just ',' or 'col1,col2' or 'col,'
                token_val = self.current_token.value
                self._advance()
                parts = [p.strip() for p in token_val.split(',') if p.strip()]
                columns.extend(parts)
            else:
                break

        column_list_node = ColumnListNode(
            node_type=NodeType.COLUMN_LIST,
            columns=columns,
            position=first_token.position,
            line=first_token.line,
            column=first_token.column
        )

        return column_list_node
    
    def _parse_expression(self) -> ASTNode:
        """Parse general expression with operator precedence"""
        return self._parse_binary_expression(0)
    
    def _parse_binary_expression(self, min_precedence: int) -> ASTNode:
        """Parse binary expression with precedence climbing"""
        left = self._parse_primary()
        
        while (self.current_token and
               self.current_token.type in [TokenType.ARITHMETIC_OP, TokenType.ASSIGNMENT_OP, TokenType.COMPARISON_OP] and
               self.precedence.get(self.current_token.value, 0) >= min_precedence):
            
            operator_token = self.current_token
            operator_precedence = self.precedence.get(operator_token.value, 0)
            self._advance()
            
            right = self._parse_binary_expression(operator_precedence + 1)
            
            left = BinaryOperationNode(
                node_type=NodeType.BINARY_OPERATION,
                operator=operator_token.value,
                left=left,
                right=right,
                position=operator_token.position,
                line=operator_token.line,
                column=operator_token.column
            )
        
        return left
    
    def _parse_primary(self) -> ASTNode:
        """Parse primary expression (literals, identifiers, etc.)"""
        if self._match(TokenType.LITERAL_STRING):
            token = self.current_token
            self._advance()
            return LiteralNode(
                node_type=NodeType.STRING_LITERAL,
                value=token.value[1:-1],  # Remove quotes
                literal_type='string',
                position=token.position,
                line=token.line,
                column=token.column
            )
        
        elif self._match(TokenType.LITERAL_NUMBER):
            token = self.current_token
            self._advance()
            # Convert to appropriate numeric type
            try:
                value = int(token.value) if '.' not in token.value else float(token.value)
            except ValueError:
                value = token.value

            return LiteralNode(
                node_type=NodeType.NUMBER_LITERAL,
                value=value,
                literal_type='number',
                position=token.position,
                line=token.line,
                column=token.column
            )

        elif self._match(TokenType.LITERAL_BOOLEAN):
            token = self.current_token
            self._advance()
            value = token.value.lower() == 'true'
            return LiteralNode(
                node_type=NodeType.BOOLEAN_LITERAL,
                value=value,
                literal_type='boolean',
                position=token.position,
                line=token.line,
                column=token.column
            )

        elif self._match(TokenType.LITERAL_NULL):
            token = self.current_token
            self._advance()
            return LiteralNode(
                node_type=NodeType.NULL_LITERAL,
                value=None,
                literal_type='null',
                is_null=True,
                position=token.position,
                line=token.line,
                column=token.column
            )

        elif self._match(TokenType.TABLE_NAME):
            return self._parse_table_reference()
        
        else:
            raise ParseError(f"Unexpected token in expression: {self.current_token.type.name}", 
                           self.current_token)
    
    def _parse_output(self) -> ASTNode:
        """Parse output specification"""
        if self._match(TokenType.DATA_TYPE):
            token = self.current_token
            self._advance()
            
            return LiteralNode(
                node_type=NodeType.STRING_LITERAL,
                value=token.value,
                literal_type='output_type',
                position=token.position,
                line=token.line,
                column=token.column
            )
        else:
            return self._parse_expression()
    
    def _determine_query_type(self, operation: ASTNode) -> str:
        """Determine query type based on operation"""
        if operation.node_type == NodeType.FUNCTION_CALL:
            func_node = operation
            if func_node.function_name.startswith('*3'):
                return 'SELECT'
            elif func_node.function_name.startswith('*4'):
                return 'UPDATE'
            elif func_node.function_name.startswith('*COUNT'):
                return 'AGGREGATE'
        elif operation.node_type == NodeType.JOIN_QUERY:
            return 'JOIN'
        elif operation.node_type == NodeType.SCHEMA_OPERATION:
            return 'SCHEMA'
        elif operation.node_type == NodeType.TRANSACTION_BLOCK:
            return 'TRANSACTION'
        
        return 'UNKNOWN'

class ASTVisitor(ABC):
    """Base visitor for AST traversal"""
    
    @abstractmethod
    def visit_query(self, node: QueryNode):
        pass
    
    @abstractmethod
    def visit_function_call(self, node: FunctionCallNode):
        pass
    
    @abstractmethod
    def visit_binary_operation(self, node: BinaryOperationNode):
        pass
    
    @abstractmethod
    def visit_container(self, node: ContainerNode):
        pass
    
    @abstractmethod
    def visit_table_reference(self, node: TableReferenceNode):
        pass
    
    @abstractmethod
    def visit_column_list(self, node: ColumnListNode):
        pass
    
    @abstractmethod
    def visit_literal(self, node: LiteralNode):
        pass
    
    @abstractmethod
    def visit_join(self, node: JoinNode):
        pass

class ASTPrinter(ASTVisitor):
    """Visitor that prints AST structure"""
    
    def __init__(self):
        self.indent_level = 0
    
    def _indent(self) -> str:
        return "  " * self.indent_level
    
    def visit_query(self, node: QueryNode) -> str:
        result = f"{self._indent()}Query ({node.query_type})\n"
        self.indent_level += 1
        
        if node.operation:
            result += f"{self._indent()}Operation:\n"
            self.indent_level += 1
            result += node.operation.accept(self)
            self.indent_level -= 1
        
        if node.target:
            result += f"{self._indent()}Target:\n"
            self.indent_level += 1
            result += node.target.accept(self)
            self.indent_level -= 1
        
        if node.output:
            result += f"{self._indent()}Output:\n"
            self.indent_level += 1
            result += node.output.accept(self)
            self.indent_level -= 1
        
        self.indent_level -= 1
        return result
    
    def visit_function_call(self, node: FunctionCallNode) -> str:
        result = f"{self._indent()}Function: {node.function_name}"
        if node.semantic_meaning:
            result += f" ({node.semantic_meaning})"
        result += "\n"
        return result
    
    def visit_binary_operation(self, node: BinaryOperationNode) -> str:
        result = f"{self._indent()}BinaryOp: {node.operator}\n"
        self.indent_level += 1
        result += f"{self._indent()}Left:\n"
        self.indent_level += 1
        result += node.left.accept(self)
        self.indent_level -= 1
        result += f"{self._indent()}Right:\n"  
        self.indent_level += 1
        result += node.right.accept(self)
        self.indent_level -= 2
        return result
    
    def visit_container(self, node: ContainerNode) -> str:
        result = f"{self._indent()}Container [{len(node.contents)} items]\n"
        self.indent_level += 1
        for item in node.contents:
            result += item.accept(self)
        self.indent_level -= 1
        return result
    
    def visit_table_reference(self, node: TableReferenceNode) -> str:
        return f"{self._indent()}Table: {node.table_name}\n"
    
    def visit_column_list(self, node: ColumnListNode) -> str:
        return f"{self._indent()}Columns: {', '.join(node.columns)}\n"
    
    def visit_literal(self, node: LiteralNode) -> str:
        return f"{self._indent()}Literal ({node.literal_type}): {node.value}\n"
    
    def visit_join(self, node: JoinNode) -> str:
        return f"{self._indent()}Join: {node.join_type}\n"

def main():
    """Test the SAIQL parser"""
    # Import lexer for testing
    try:
        from lexer import SAIQLLexer
        lexer = SAIQLLexer()
    except ImportError:
        print("Error: lexer module required for testing")
        return
    
    parser = SAIQLParser(debug=True)
    printer = ASTPrinter()
    
    # Test queries
    test_queries = [
        "*3[users]::name,email>>oQ",
        "=J[users+orders]::>>oQ",
        "*COUNT[sales]::*>>oQ",
        "$1",
    ]
    
    print("SAIQL Parser Test Results")
    print("=" * 60)
    
    for query in test_queries:
        print(f"\nQuery: {query}")
        print("-" * 40)
        
        try:
            # Tokenize
            tokens = lexer.tokenize(query)
            print(f"Tokens: {len(tokens) - 1}")  # -1 for EOF
            
            # Parse
            ast = parser.parse(tokens)
            print("AST Structure:")
            print(printer.visit_query(ast))
            
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
