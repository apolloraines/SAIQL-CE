#!/usr/bin/env python3
"""
SAIQL Lexical Analyzer (Lexer)

This module performs lexical analysis on SAIQL queries, converting raw text into
classified tokens. It handles symbol recognition, validates syntax at the character
level, provides detailed error reporting, and prepares tokens for parsing.

The lexer is the first stage of SAIQL query processing:
Raw Text → Lexer → Parser → Compiler → Engine

Author: Apollo & Claude  
Version: 1.0.0
Status: Foundation Phase

Educational Note:
A lexer (lexical analyzer) is the first component of any programming language
compiler or interpreter. It reads raw source code and converts it into tokens -
the fundamental building blocks that represent symbols, operators, identifiers,
and literals in the language.
"""

import re
import logging
from typing import List, Optional, Dict, Any, Tuple, NamedTuple
from enum import Enum, auto
from dataclasses import dataclass
from pathlib import Path
import json

# Configure logging
logger = logging.getLogger(__name__)

class TokenType(Enum):
    """
    SAIQL Token Types
    
    Each token type represents a different category of symbols or operators
    in the SAIQL language. This classification helps the parser understand
    the role of each token in the query structure.
    """
    # Core Symbols (from families in legend_map)
    FUNCTION_SYMBOL = auto()     # *3, *COUNT, *SUM, etc.
    DATA_TYPE = auto()           # o, oQ, oo, etc.  
    ARITHMETIC_OP = auto()       # +, ++, +1, etc.
    ASSIGNMENT_OP = auto()       # =, ==, ===, etc.
    CONSTRAINT = auto()          # !, !1, !2, etc.
    SCHEMA_OP = auto()           # @, @1, @2, etc.
    INDEX_OP = auto()            # #, ##, ###, etc.
    TRANSACTION = auto()         # $, $1, $2, etc.
    
    # Structural Elements
    NAMESPACE_SEP = auto()       # ::
    OUTPUT_OP = auto()           # >>
    WILDCARD = auto()            # * for select all
    JOIN_SYMBOL = auto()         # =J, =L, =R, etc.
    COMPARISON_OP = auto()       # =, <, >, <=, >=, != for comparisons
    
    # Containers and Delimiters
    CONTAINER_OPEN = auto()      # [
    CONTAINER_CLOSE = auto()     # ]
    BLOCK_OPEN = auto()          # {
    BLOCK_CLOSE = auto()         # }
    PARAM_OPEN = auto()          # (
    PARAM_CLOSE = auto()         # )
    
    # Literals and Identifiers
    TABLE_NAME = auto()          # users, orders, products
    COLUMN_NAME = auto()         # name, email, price
    COLUMN_LIST = auto()         # name,email,phone
    LITERAL_STRING = auto()      # 'hello', "world"
    LITERAL_NUMBER = auto()      # 123, 45.67, -89
    LITERAL_BOOLEAN = auto()     # true, false
    LITERAL_NULL = auto()        # null, NULL

    # AI-Native Features
    VECTOR_TYPE = auto()         # VECTOR data type
    SIMILAR_TO_OP = auto()       # SIMILAR TO operator
    IMAGINATION_TABLE = auto()   # IMAGINATION virtual table
    
    # Special Tokens
    COMMENT = auto()             # // comment or # comment
    WHITESPACE = auto()          # spaces, tabs (usually ignored)
    NEWLINE = auto()             # line breaks
    EOF = auto()                 # end of file/query
    
    # Error Token
    UNKNOWN = auto()             # unrecognized token

@dataclass
class Token:
    """
    Represents a single token in SAIQL
    
    A token contains all the information needed to understand a piece
    of the query: what type it is, its actual text value, and where
    it was found in the source.
    """
    type: TokenType
    value: str
    position: int           # Character position in query
    line: int              # Line number (for multi-line queries)
    column: int            # Column number within line
    length: int            # Length of token in characters
    
    # Additional metadata
    symbol_family: Optional[str] = None  # Which legend family this belongs to
    semantic_meaning: Optional[str] = None  # Human-readable meaning
    sql_hint: Optional[str] = None      # SQL equivalent hint
    
    def __str__(self) -> str:
        return f"Token({self.type.name}, '{self.value}', {self.line}:{self.column})"
    
    def __repr__(self) -> str:
        return self.__str__()

class LexError(Exception):
    """Exception raised during lexical analysis"""
    
    def __init__(self, message: str, position: int, line: int, column: int):
        self.message = message
        self.position = position
        self.line = line
        self.column = column
        super().__init__(f"Lexical error at {line}:{column} - {message}")

class SAIQLLexer:
    """
    SAIQL Lexical Analyzer
    
    The lexer converts raw SAIQL query text into a stream of classified tokens.
    It handles symbol recognition, validates character-level syntax, and provides
    detailed error reporting with precise location information.
    
    Key Responsibilities:
    1. Tokenize raw SAIQL text into classified tokens
    2. Validate symbols against the legend map
    3. Handle string literals, numbers, and identifiers
    4. Track position information for error reporting
    5. Skip whitespace and comments
    6. Detect syntax errors at the character level
    """
    
    def __init__(self, legend_path: str = "data/legend_map.lore"):
        """Initialize the lexer with symbol definitions"""
        self.legend_map = {}
        self.symbol_cache = {}
        self.load_legend(legend_path)
        
        # Current lexing state
        self.text = ""
        self.position = 0
        self.line = 1
        self.column = 1
        self.tokens = []
        
        # Compiled regex patterns for efficiency
        self._compile_patterns()
        
        logger.info(f"SAIQL Lexer initialized with {len(self.symbol_cache)} symbols")
    
    def load_legend(self, legend_path: str) -> None:
        """Load symbol definitions from legend file"""
        try:
            with open(legend_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                self.legend_map = data.get('SAIQL_LEGEND', {})
            
            # Build symbol cache for fast lookup
            self._build_symbol_cache()
            logger.info(f"Loaded legend with {len(self.symbol_cache)} symbols")
            
        except FileNotFoundError:
            logger.error(f"Legend file not found: {legend_path}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in legend file: {e}")
            raise
    
    def _build_symbol_cache(self) -> None:
        """Build flattened symbol cache for fast lookups"""
        families = self.legend_map.get('families', {})
        
        for family_name, family_data in families.items():
            symbols = family_data.get('symbols', {})
            for symbol, data in symbols.items():
                self.symbol_cache[symbol] = {
                    'semantic': data['semantic'],
                    'sql_hint': data['sql_hint'],
                    'type': data['type'],
                    'family': family_name
                }
    
    def _compile_patterns(self) -> None:
        """Compile regex patterns for token recognition"""
        # Multi-character operators (order matters - longer first)
        self.operator_patterns = [
            (r'::', TokenType.NAMESPACE_SEP),
            (r'>>', TokenType.OUTPUT_OP),
            (r'===', TokenType.ASSIGNMENT_OP),
            (r'==', TokenType.ASSIGNMENT_OP),
            (r'=J|=L|=R|=F|=C|=S|=N|=U', TokenType.JOIN_SYMBOL),
            (r'\+\+\+', TokenType.ARITHMETIC_OP),
            (r'\+\+', TokenType.ARITHMETIC_OP),
            (r'!!!', TokenType.CONSTRAINT),
            (r'!!', TokenType.CONSTRAINT),
            (r'###', TokenType.INDEX_OP),
            (r'##', TokenType.INDEX_OP),
            (r'\*\*\*\*\*', TokenType.FUNCTION_SYMBOL),
            (r'\*\*\*\*', TokenType.FUNCTION_SYMBOL),
            (r'\*\*\*', TokenType.FUNCTION_SYMBOL),
            (r'\*\*', TokenType.FUNCTION_SYMBOL),
            (r'oooo', TokenType.DATA_TYPE),
            (r'ooo', TokenType.DATA_TYPE),
            (r'oo', TokenType.DATA_TYPE),
        ]
        
        # Single character patterns
        self.single_char_patterns = {
            '[': TokenType.CONTAINER_OPEN,
            ']': TokenType.CONTAINER_CLOSE,
            '{': TokenType.BLOCK_OPEN,
            '}': TokenType.BLOCK_CLOSE,
            '(': TokenType.PARAM_OPEN,
            ')': TokenType.PARAM_CLOSE,
            ',': TokenType.COLUMN_LIST,
            '=': TokenType.COMPARISON_OP,  # Single = for equality comparisons
            '<': TokenType.COMPARISON_OP,
            '>': TokenType.COMPARISON_OP,
        }
        
        # Compiled regex patterns
        self.patterns_compiled = [(re.compile(pattern), token_type) 
                                for pattern, token_type in self.operator_patterns]
        
        # Number pattern
        self.number_pattern = re.compile(r'-?\d+\.?\d*')
        
        # String patterns
        self.string_patterns = [
            re.compile(r"'([^'\\]|\\.)*'"),  # Single quotes
            re.compile(r'"([^"\\]|\\.)*"'),  # Double quotes
        ]
        
        # Identifier pattern (table names, column names, etc.)
        # Supports: simple names, comma-separated lists, qualified names (table.column)
        self.identifier_pattern = re.compile(r'[a-zA-Z_][a-zA-Z0-9_.,]*')
        
        # Whitespace and comment patterns
        self.whitespace_pattern = re.compile(r'\s+')
        # NOTE: Only // comments supported. # is reserved for index operators.
        self.comment_patterns = [
            re.compile(r'//.*$', re.MULTILINE),  # // comments only
        ]
    def _match_wildcard_asterisk(self) -> bool:
        """Match standalone asterisk as wildcard (not part of function symbol)"""
        char = self._current_char()
        if char == "*":
            # Check if this is NOT a function symbol (not followed by alphanumeric)
            next_char = self._peek_char()
            if next_char is None or not next_char.isalnum():
                # This is a standalone wildcard asterisk
                token = Token(
                    type=TokenType.WILDCARD,
                    value=char,
                    position=self.position,
                    line=self.line,
                    column=self.column,
                    length=1
                )
                self.tokens.append(token)
                self._advance()
                return True
        return False


    def tokenize(self, text: str, skip_whitespace: bool = True,
                 skip_comments: bool = True) -> List[Token]:
        """
        Main tokenization method

        Args:
            text: SAIQL query text to tokenize
            skip_whitespace: Whether to skip whitespace tokens
            skip_comments: Whether to skip comment tokens

        Returns:
            List of tokens

        Raises:
            LexError: If invalid syntax is encountered
        """
        self.text = text
        self.position = 0
        self.line = 1
        self.column = 1
        self.tokens = []

        while self.position < len(self.text):
            # Handle whitespace
            if self._match_whitespace(emit_token=not skip_whitespace):
                continue

            # Handle comments
            if self._match_comment(emit_token=not skip_comments):
                continue
            
            # Try to match different token types
            if (self._match_multi_char_operator() or
                self._match_single_char_operator() or
                self._match_symbol() or
                self._match_string_literal() or
                self._match_number_literal() or
                self._match_identifier()):
                continue
            
            # Handle standalone asterisk as wildcard
            if self._match_wildcard_asterisk():
                continue

            # If nothing matched, we have an unknown character
            char = self.text[self.position]
            raise LexError(f"Unexpected character '{char}'", 
                         self.position, self.line, self.column)
        
        # Add EOF token
        self.tokens.append(Token(
            type=TokenType.EOF,
            value="",
            position=self.position,
            line=self.line,
            column=self.column,
            length=0
        ))
        
        return self.tokens
    
    def _current_char(self) -> Optional[str]:
        """Get current character or None if at end"""
        if self.position >= len(self.text):
            return None
        return self.text[self.position]
    
    def _peek_char(self, offset: int = 1) -> Optional[str]:
        """Peek ahead at character without advancing position"""
        peek_pos = self.position + offset
        if peek_pos >= len(self.text):
            return None
        return self.text[peek_pos]
    
    def _advance(self, count: int = 1) -> None:
        """Advance position and update line/column tracking"""
        for _ in range(count):
            if self.position < len(self.text):
                if self.text[self.position] == '\n':
                    self.line += 1
                    self.column = 1
                else:
                    self.column += 1
                self.position += 1
    
    def _match_whitespace(self, emit_token: bool = False) -> bool:
        """Match whitespace, optionally emitting a token"""
        match = self.whitespace_pattern.match(self.text, self.position)
        if match:
            if emit_token:
                token = Token(
                    type=TokenType.WHITESPACE,
                    value=match.group(),
                    position=self.position,
                    line=self.line,
                    column=self.column,
                    length=len(match.group())
                )
                self.tokens.append(token)
            self._advance(len(match.group()))
            return True
        return False

    def _match_comment(self, emit_token: bool = False) -> bool:
        """Match comments, optionally emitting a token"""
        for pattern in self.comment_patterns:
            match = pattern.match(self.text, self.position)
            if match:
                if emit_token:
                    token = Token(
                        type=TokenType.COMMENT,
                        value=match.group(),
                        position=self.position,
                        line=self.line,
                        column=self.column,
                        length=len(match.group())
                    )
                    self.tokens.append(token)
                self._advance(len(match.group()))
                return True
        return False
    
    def _match_multi_char_operator(self) -> bool:
        """Match multi-character operators"""
        for pattern, token_type in self.patterns_compiled:
            match = pattern.match(self.text, self.position)
            if match:
                value = match.group()
                token = Token(
                    type=token_type,
                    value=value,
                    position=self.position,
                    line=self.line,
                    column=self.column,
                    length=len(value)
                )
                self.tokens.append(token)
                self._advance(len(value))
                return True
        return False
    
    def _match_single_char_operator(self) -> bool:
        """Match single character operators"""
        char = self._current_char()
        if char and char in self.single_char_patterns:
            token = Token(
                type=self.single_char_patterns[char],
                value=char,
                position=self.position,
                line=self.line,
                column=self.column,
                length=1
            )
            self.tokens.append(token)
            self._advance()
            return True
        return False
    
    def _match_symbol(self) -> bool:
        """Match SAIQL symbols from legend"""
        # Try to match symbols of different lengths, starting with longest
        max_symbol_length = max(len(s) for s in self.symbol_cache.keys()) if self.symbol_cache else 10
        
        for length in range(min(max_symbol_length, len(self.text) - self.position), 0, -1):
            candidate = self.text[self.position:self.position + length]
            
            if candidate in self.symbol_cache:
                symbol_data = self.symbol_cache[candidate]
                
                # Determine token type based on symbol family
                token_type = self._get_token_type_for_symbol(symbol_data['family'])
                
                token = Token(
                    type=token_type,
                    value=candidate,
                    position=self.position,
                    line=self.line,
                    column=self.column,
                    length=length,
                    symbol_family=symbol_data['family'],
                    semantic_meaning=symbol_data['semantic'],
                    sql_hint=symbol_data['sql_hint']
                )
                self.tokens.append(token)
                self._advance(length)
                return True
        
        return False
    
    def _get_token_type_for_symbol(self, family: str) -> TokenType:
        """Determine token type based on symbol family"""
        family_mapping = {
            'ASTERISK_FAMILY': TokenType.FUNCTION_SYMBOL,
            'CIRCLE_FAMILY': TokenType.DATA_TYPE,
            'PLUS_FAMILY': TokenType.ARITHMETIC_OP,
            'EQUALS_FAMILY': TokenType.ASSIGNMENT_OP,
            'EXCLAMATION_FAMILY': TokenType.CONSTRAINT,
            'AT_FAMILY': TokenType.SCHEMA_OP,
            'HASH_FAMILY': TokenType.INDEX_OP,
            'DOLLAR_FAMILY': TokenType.TRANSACTION,
            'DOLLAR_FAMILY': TokenType.TRANSACTION,
            'AGGREGATES': TokenType.FUNCTION_SYMBOL,
            'AGGREGATES': TokenType.FUNCTION_SYMBOL,
            'operations': TokenType.FUNCTION_SYMBOL,
            'operators': TokenType.ARITHMETIC_OP,
        }
        return family_mapping.get(family, TokenType.UNKNOWN)
    
    def _match_string_literal(self) -> bool:
        """Match string literals"""
        for pattern in self.string_patterns:
            match = pattern.match(self.text, self.position)
            if match:
                value = match.group()
                token = Token(
                    type=TokenType.LITERAL_STRING,
                    value=value,
                    position=self.position,
                    line=self.line,
                    column=self.column,
                    length=len(value)
                )
                self.tokens.append(token)
                self._advance(len(value))
                return True
        return False
    
    def _match_number_literal(self) -> bool:
        """Match numeric literals"""
        match = self.number_pattern.match(self.text, self.position)
        if match:
            value = match.group()
            token = Token(
                type=TokenType.LITERAL_NUMBER,
                value=value,
                position=self.position,
                line=self.line,
                column=self.column,
                length=len(value)
            )
            self.tokens.append(token)
            self._advance(len(value))
            return True
        return False
    
    def _match_identifier(self) -> bool:
        """Match identifiers (table names, column names, etc.)"""
        match = self.identifier_pattern.match(self.text, self.position)
        if match:
            value = match.group()
            
            # Determine if this is likely a table name, column name, etc.
            # This is context-sensitive and will be refined by the parser
            token_type = self._classify_identifier(value)
            
            token = Token(
                type=token_type,
                value=value,
                position=self.position,
                line=self.line,
                column=self.column,
                length=len(value)
            )
            self.tokens.append(token)
            self._advance(len(value))
            return True
        return False
    
    def _classify_identifier(self, identifier: str) -> TokenType:
        """
        Classify an identifier as table name, column name, etc.
        
        This is a basic classification - the parser will do more sophisticated
        context-sensitive classification.
        """
        # Check if it contains commas (likely a column list)
        if ',' in identifier:
            return TokenType.COLUMN_LIST
        
        # Boolean literals
        if identifier.lower() in ('true', 'false'):
            return TokenType.LITERAL_BOOLEAN

        # NULL literal
        if identifier.lower() == 'null':
            return TokenType.LITERAL_NULL

        # Default to table name - parser will reclassify based on context
        return TokenType.TABLE_NAME
    
    def get_token_info(self, token: Token) -> Dict[str, Any]:
        """Get detailed information about a token"""
        info = {
            'type': token.type.name,
            'value': token.value,
            'position': {
                'character': token.position,
                'line': token.line,
                'column': token.column,
                'length': token.length
            }
        }
        
        if token.symbol_family:
            info['symbol_info'] = {
                'family': token.symbol_family,
                'semantic': token.semantic_meaning,
                'sql_hint': token.sql_hint
            }
        
        return info
    
    def format_tokens(self, tokens: List[Token], include_position: bool = False) -> str:
        """Format tokens for display"""
        lines = []
        for token in tokens:
            if token.type == TokenType.EOF:
                continue
                
            line = f"{token.type.name:<20} '{token.value}'"
            
            if include_position:
                line += f" at {token.line}:{token.column}"
            
            if token.semantic_meaning:
                line += f" ({token.semantic_meaning})"
                
            lines.append(line)
        
        return '\n'.join(lines)

def main():
    """Test the SAIQL lexer"""
    lexer = SAIQLLexer()
    
    # Test queries
    test_queries = [
        "*3[users]::name,email>>oQ",
        "=J[users+orders]::users.id=orders.user_id>>oQ", 
        "*COUNT[sales]::*>>oQ",
        "$1 // Begin transaction",
        "@1[customers] :: name,email,phone >> oQ",
        "invalid_symbol_test_xyz"
    ]
    
    print("SAIQL Lexer Test Results")
    print("=" * 60)
    
    for query in test_queries:
        print(f"\nQuery: {query}")
        print("-" * 40)
        
        try:
            tokens = lexer.tokenize(query)
            print(f"Tokens ({len(tokens) - 1}):") # -1 for EOF token
            print(lexer.format_tokens(tokens, include_position=True))
            
        except LexError as e:
            print(f"Lexical Error: {e}")
        except Exception as e:
            print(f"Unexpected Error: {e}")

if __name__ == "__main__":
    main()
