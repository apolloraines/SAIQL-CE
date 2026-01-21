from enum import Enum

class DataType(Enum):
    """SAIQL Data Types"""
    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    LIST = "list"
    DICT = "dict"
    NULL = "null"
    UNKNOWN = "unknown"

class OperatorType(Enum):
    """SAIQL Operator Types"""
    ARITHMETIC = "arithmetic"
    COMPARISON = "comparison"
    LOGICAL = "logical"
    ASSIGNMENT = "assignment"
    BITWISE = "bitwise"
    MEMBERSHIP = "membership"
    IDENTITY = "identity"

class JoinType(Enum):
    """SAIQL Join Types"""
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"
    FULL = "full"
    CROSS = "cross"
    SELF = "self"
    NATURAL = "natural"
    UNION = "union"

class AggregationType(Enum):
    """SAIQL Aggregation Types"""
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    GROUP_CONCAT = "group_concat"
