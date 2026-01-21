#!/usr/bin/env python3
"""
SAIQL Error Hierarchy
Canonical exception classes for the SAIQL engine.
"""

from enum import Enum

class ErrorCode(Enum):
    UNKNOWN = "UNKNOWN_ERROR"
    SYNTAX_ERROR = "SYNTAX_ERROR"
    COMPILATION_ERROR = "COMPILATION_ERROR"
    RUNTIME_ERROR = "RUNTIME_ERROR"
    STORAGE_ERROR = "STORAGE_ERROR"
    SECURITY_ERROR = "SECURITY_ERROR"
    TIMEOUT = "TIMEOUT"
    NOT_FOUND = "NOT_FOUND"

class SAIQLError(Exception):
    """Base class for all SAIQL exceptions"""
    def __init__(self, message: str, code: ErrorCode = ErrorCode.UNKNOWN, details: dict = None):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}

class ParseError(SAIQLError):
    """Raised when query parsing fails"""
    def __init__(self, message: str, line: int = None, column: int = None):
        details = {'line': line, 'column': column}
        super().__init__(message, ErrorCode.SYNTAX_ERROR, details)

class CompileError(SAIQLError):
    """Raised when query compilation fails"""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, ErrorCode.COMPILATION_ERROR, details)

class RuntimeError(SAIQLError):
    """Raised during query execution"""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, ErrorCode.RUNTIME_ERROR, details)

class StorageError(SAIQLError):
    """Raised when storage backend fails"""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, ErrorCode.STORAGE_ERROR, details)

class SecurityError(SAIQLError):
    """Raised when a security policy is violated"""
    def __init__(self, message: str, details: dict = None):
        super().__init__(message, ErrorCode.SECURITY_ERROR, details)
