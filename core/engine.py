#!/usr/bin/env python3
"""
SAIQL Engine - High-Level Orchestration System

This module provides the main API for SAIQL query execution, orchestrating
the entire pipeline from raw queries to database results. It manages all
components (lexer, parser, compiler, executor) and provides a simple,
unified interface for users.

The engine handles:
- Full pipeline orchestration (lexer → parser → compiler → executor)
- Session management and caching
- Performance monitoring and optimization
- Error recovery and detailed reporting
- Configuration management
- Connection pooling and resource management

Author: Apollo & Claude
Version: 1.0.0
Status: Foundation Phase

Usage:
    engine = SAIQLEngine()
    result = engine.execute("*3[users]::name,email>>oQ")
"""

import logging
import time
import threading
from typing import Dict, List, Any, Optional, Union, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto
from contextlib import contextmanager
from pathlib import Path
import json
import hashlib
from collections import defaultdict, OrderedDict

# Optional SymbolicEngine import
try:
    from .symbolic_engine import SymbolicEngine
except ImportError:
    SymbolicEngine = None


# Configure logging
logger = logging.getLogger(__name__)

class ExecutionMode(Enum):
    """Execution modes for different use cases"""
    SYNC = "synchronous"           # Standard synchronous execution
    ASYNC = "asynchronous"         # Asynchronous execution
    BATCH = "batch"                # Batch processing of multiple queries
    STREAMING = "streaming"        # Streaming results for large datasets
    CACHED = "cached"              # Cached execution with result reuse

class SessionState(Enum):
    """Session states"""
    CREATED = auto()
    ACTIVE = auto()
    PAUSED = auto()
    COMPLETED = auto()
    ERROR = auto()

@dataclass
class ExecutionContext:
    """Context for query execution"""
    session_id: str
    user_id: Optional[str] = None
    database_url: Optional[str] = None
    execution_mode: ExecutionMode = ExecutionMode.SYNC
    timeout_seconds: int = 300
    max_memory_mb: int = 1024
    debug: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class QueryResult:
    """Unified result object for SAIQL queries"""
    success: bool
    data: List[Dict[str, Any]]
    execution_time: float
    query: str
    sql_generated: str
    rows_affected: int
    session_id: str
    
    # Detailed execution information
    compilation_time: float = 0.0
    parsing_time: float = 0.0
    lexing_time: float = 0.0
    database_time: float = 0.0
    
    # Optimization and analysis
    optimizations_applied: List[str] = field(default_factory=list)
    complexity_score: int = 0
    cache_hit: bool = False
    
    # Error information
    error_message: Optional[str] = None
    error_phase: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    
    # Metadata
    target_dialect: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            'success': self.success,
            'data': self.data,
            'execution_time': self.execution_time,
            'query': self.query,
            'sql_generated': self.sql_generated,
            'rows_affected': self.rows_affected,
            'session_id': self.session_id,
            'compilation_time': self.compilation_time,
            'parsing_time': self.parsing_time,
            'lexing_time': self.lexing_time,
            'database_time': self.database_time,
            'optimizations_applied': self.optimizations_applied,
            'complexity_score': self.complexity_score,
            'cache_hit': self.cache_hit,
            'error_message': self.error_message,
            'error_phase': self.error_phase,
            'warnings': self.warnings,
            'target_dialect': self.target_dialect,
            'metadata': self.metadata
        }

    def copy(self) -> 'QueryResult':
        """Create a deep copy of the result"""
        import copy
        return copy.deepcopy(self)

class QueryCache:
    """LRU cache for compiled queries and results"""
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.cache = OrderedDict()
        self.stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0
        }
        self._lock = threading.RLock()
    
    def get(self, key: str) -> Optional[Any]:
        """Get item from cache"""
        with self._lock:
            if key in self.cache:
                # Move to end (most recently used)
                self.cache.move_to_end(key)
                self.stats['hits'] += 1
                return self.cache[key]
            else:
                self.stats['misses'] += 1
                return None
    
    def put(self, key: str, value: Any) -> None:
        """Put item in cache"""
        with self._lock:
            if key in self.cache:
                # Update existing item
                self.cache[key] = value
                self.cache.move_to_end(key)
            else:
                # Add new item
                self.cache[key] = value
                
                # Evict if over capacity
                if len(self.cache) > self.max_size:
                    self.cache.popitem(last=False)  # Remove oldest
                    self.stats['evictions'] += 1
    
    def clear(self) -> None:
        """Clear all cached items"""
        with self._lock:
            self.cache.clear()
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self._lock:
            total_requests = self.stats['hits'] + self.stats['misses']
            hit_rate = self.stats['hits'] / max(total_requests, 1)
            
            return {
                'size': len(self.cache),
                'max_size': self.max_size,
                'hits': self.stats['hits'],
                'misses': self.stats['misses'],
                'evictions': self.stats['evictions'],
                'hit_rate': hit_rate
            }

class SessionManager:
    """Manages SAIQL execution sessions"""
    
    def __init__(self):
        self.sessions = {}
        self._lock = threading.RLock()
        self._session_counter = 0
    
    def create_session(self, context: ExecutionContext) -> str:
        """Create new execution session"""
        with self._lock:
            if not context.session_id:
                self._session_counter += 1
                context.session_id = f"saiql_session_{self._session_counter}_{int(time.time())}"
            
            session_data = {
                'context': context,
                'state': SessionState.CREATED,
                'created_at': time.time(),
                'last_activity': time.time(),
                'query_count': 0,
                'total_execution_time': 0.0
            }
            
            self.sessions[context.session_id] = session_data
            logger.info(f"Created session: {context.session_id}")
            return context.session_id
    
    def get_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Get session data"""
        with self._lock:
            return self.sessions.get(session_id)
    
    def update_session(self, session_id: str, **updates) -> None:
        """Update session data"""
        with self._lock:
            if session_id in self.sessions:
                self.sessions[session_id].update(updates)
                self.sessions[session_id]['last_activity'] = time.time()
    
    def cleanup_expired_sessions(self, max_age_seconds: int = 3600) -> int:
        """Clean up expired sessions"""
        with self._lock:
            current_time = time.time()
            expired_sessions = []
            
            for session_id, session_data in self.sessions.items():
                if current_time - session_data['last_activity'] > max_age_seconds:
                    expired_sessions.append(session_id)
            
            for session_id in expired_sessions:
                del self.sessions[session_id]
                logger.info(f"Cleaned up expired session: {session_id}")
            
            return len(expired_sessions)

class SAIQLEngine:
    """
    Main SAIQL Engine - High-Level Orchestration System
    
    Provides a unified, simple API for SAIQL query execution while managing
    all the complexity of the underlying pipeline components.
    
    Features:
    - Complete pipeline orchestration
    - Performance optimization and caching
    - Session management
    - Error recovery and detailed reporting
    - Configuration management
    - Resource management and connection pooling
    """
    
    def __init__(self, 
                 db_path: Optional[str] = None,
                 config_path: Optional[str] = None, 
                 debug: bool = False):
        """Initialize SAIQL Engine"""
        self.debug = debug
        
        # Load secure configuration
        try:
            from config.secure_config import get_config
            self.secure_config = get_config()
        except ImportError:
            # Fallback for standalone usage
            self.secure_config = None
            logger.warning("Could not import config.secure_config, using defaults")
            
        # Initialize configuration
        self.config = self._load_config(config_path) # Retain existing _load_config logic
        
        # Override with secure config if available
        if self.secure_config:
            self.config['database']['path'] = str(self.secure_config.data_dir / 'saiql.db')
            # Assuming 'logging' key might be added to config or handled elsewhere if not present
            # For now, only apply if it exists or create it if necessary.
            # The original _load_config doesn't define 'logging' so this might cause KeyError.
            # To be safe, I'll add a check or ensure _load_config defines it.
            # Given the instruction, I'll assume the config structure will eventually support it.
            # For now, I'll add a default 'logging' key to the fallback config in _load_config.
            if 'logging' not in self.config:
                self.config['logging'] = {}
            self.config['logging']['level'] = self.secure_config.log_level
            
        # Override database path if provided explicitly
        if db_path:
            self.config['database']['path'] = db_path
        
        # CE Edition: LoreToken-Lite only (no gradient levels)
        self.edition = "community"
        
        # Initialize components
        # Initialize Safety Policy
        from .safety import SafetyPolicy
        if self.secure_config:
            # Map profile to safety policy
            if self.secure_config.profile == 'prod':
                self.safety_policy = SafetyPolicy.strict()
            else:
                self.safety_policy = SafetyPolicy.development()
        else:
            self.safety_policy = SafetyPolicy.development()
            
        self._initialize_components()
        
        # Initialize Semantic Firewall
        from .semantic_firewall import SemanticFirewall
        self.firewall = SemanticFirewall()

        # CE Edition: No GroundingGuard - returns raw query results

        # Initialize management systems
        self.session_manager = SessionManager()
        self.query_cache = QueryCache(max_size=self.config.get('cache_size', 1000))
        self.performance_tracker = PerformanceTracker()
        
        # Engine statistics
        self.stats = {
            'queries_executed': 0,
            'total_execution_time': 0.0,
            'successful_queries': 0,
            'failed_queries': 0,
            'cache_hits': 0,
            'start_time': time.time()
        }
        
        # Thread safety
        self._lock = threading.RLock()
        
        logger.info("SAIQL Engine initialized successfully")
        
        if debug:
            self._log_initialization_info()
    
    def _load_config(self, config_path: Optional[str]) -> Dict[str, Any]:
        """Load engine configuration from secure_config"""
        try:
            # Use centralized secure configuration
            from config.secure_config import get_config
            secure_cfg = get_config()
            
            # Convert to dictionary format expected by engine
            # This bridges the gap between the new config system and existing engine code
            default_config = {
                'database': {
                    'path': str(secure_cfg.data_dir / "saiql.db"),
                    'timeout': 30
                },
                'legend': {
                    'path': str(secure_cfg.data_dir / "legend_map.lore")
                },
                'compilation': {
                    'target_dialect': 'sqlite',
                    'optimization_level': 'standard',
                    'enable_caching': True
                },
                'execution': {
                    'default_timeout': 300,
                    'max_memory_mb': 1024,
                    'enable_async': True
                },
                'edition': 'community',
                'cache_size': 1000,
                'session_cleanup_interval': 3600,
                'performance_tracking': True,
            }
            
            # If specific config path provided, merge it (legacy support)
            if config_path and Path(config_path).exists():
                try:
                    file_config = read_json_file(config_path)
                    self._merge_config(default_config, file_config)
                except Exception as e:
                    logger.warning(f"Error loading config file: {e}, using defaults")
            
            return default_config
            
        except ImportError:
            # Fallback if config module not found (should not happen in prod)
            logger.warning("Could not import config.secure_config, using hardcoded defaults")
            return {
                'database': {'path': 'data/saiql.db', 'timeout': 30},
                'legend': {'path': 'data/legend_map.lore'},
                'compilation': {'target_dialect': 'sqlite', 'optimization_level': 'standard', 'enable_caching': True},
                'edition': 'community'
            }
    
    def _merge_config(self, base: Dict[str, Any], override: Dict[str, Any]) -> None:
        """Recursively merge configuration dictionaries"""
        for key, value in override.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge_config(base[key], value)
            else:
                base[key] = value
    
    def _initialize_components(self) -> None:
        """Initialize all SAIQL pipeline components"""
        try:
            # DEBUG: Check import context
            # print(f"DEBUG: core.engine context - __name__={__name__}, __package__={__package__}")
            
            # Import SAIQL components using relative imports
            from .lexer import SAIQLLexer, Token, LexError
            from .parser import SAIQLParser, ASTNode, ParseError
            from .compiler import SAIQLCompiler, CompilationResult, TargetDialect, OptimizationLevel
            
            # Initialize lexer
            legend_path = self.config['legend']['path']
            self.lexer = SAIQLLexer(legend_path)
            
            # Initialize parser
            self.parser = SAIQLParser(debug=self.debug)
            
            # Initialize compiler
            dialect_str = self.config['compilation']['target_dialect']
            optimization_str = self.config['compilation']['optimization_level']
            
            target_dialect = TargetDialect(dialect_str)
            optimization_level = OptimizationLevel[optimization_str.upper()]
            
            self.compiler = SAIQLCompiler(
                target_dialect=target_dialect,
                optimization_level=optimization_level,
                legend_map=self.lexer.legend_map
            )
            
            # Initialize symbolic engine
            db_path = self.config["database"]["path"]
            if SymbolicEngine:
                self.symbolic_engine = SymbolicEngine(db_path)
            else:
                self.symbolic_engine = None
            
            logger.info("All SAIQL components initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize SAIQL components: {e}")
            raise
    
    def _log_initialization_info(self) -> None:
        """Log detailed initialization information"""
        logger.info("SAIQL Engine Configuration:")
        logger.info(f"  Database: {self.config['database']['path']}")
        logger.info(f"  Legend: {self.config['legend']['path']}")
        logger.info(f"  Target Dialect: {self.config['compilation']['target_dialect']}")
        logger.info(f"  Optimization: {self.config['compilation']['optimization_level']}")
        logger.info(f"  Cache Size: {self.config['cache_size']}")
        logger.info(f"  Edition: {self.edition}")
        logger.info(f"  Symbols Loaded: {len(self.lexer.symbol_cache)}")

    def execute(self, query: str, context: Optional[ExecutionContext] = None,
                enable_caching: Optional[bool] = None) -> QueryResult:
        """
        Execute a SAIQL query.

        Args:
            query: SAIQL query string
            context: Execution context (optional)
            enable_caching: Override caching setting (optional)

        Returns:
            QueryResult with execution details and data
        """
        start_time = time.time()
        
        # Create default context if none provided
        if context is None:
            context = ExecutionContext(session_id="")
        
        # Create or get session
        if not context.session_id:
            session_id = self.session_manager.create_session(context)
            context.session_id = session_id
        else:
            session_id = context.session_id
            # Ensure session is registered
            if not self.session_manager.get_session(session_id):
                self.session_manager.create_session(context)
        
        # Generate trace ID for this execution
        import uuid
        trace_id = str(uuid.uuid4())
        
        # Initialize result object
        result = QueryResult(
            success=False,
            data=[],
            execution_time=0.0,
            query=query,
            sql_generated="",
            rows_affected=0,
            session_id=session_id 
        )
        result.metadata['trace_id'] = trace_id
        
        # Structured logging context
        log_extra = {
            "trace_id": trace_id,
            "session_id": session_id,
            "query_hash": hashlib.md5(query.encode()).hexdigest()
        }
        logger.info(f"Starting query execution: {query[:50]}...", extra=log_extra)
        
        # 0. Semantic Firewall: Pre-Prompt Guard
        firewall_decision = self.firewall.pre_prompt_guard(query, context={"session_id": session_id})
        if firewall_decision.action == "BLOCK":
            msg = f"Firewall blocked query: {', '.join(firewall_decision.reasons)}"
            logger.warning(msg, extra=log_extra)
            result.success = False
            result.error_message = msg
            result.error_phase = "security_guard"
            result.metadata['firewall_decision'] = "BLOCK"
            
            with self._lock:
                self.stats['failed_queries'] += 1
            return result
        
        try:
            with self._lock:
                self.stats['queries_executed'] += 1
            
            # Update session
            self.session_manager.update_session(session_id, 
                                              state=SessionState.ACTIVE,
                                              query_count=self.session_manager.get_session(session_id)['query_count'] + 1)
            
            # Check cache if enabled
            use_cache = enable_caching if enable_caching is not None else self.config['compilation']['enable_caching']
            cache_key = None

            if use_cache:
                cache_key = self._generate_cache_key(query, context)
                cached_result = self.query_cache.get(cache_key)

                if cached_result:
                    result = cached_result.copy()
                    result.cache_hit = True
                    result.execution_time = time.time() - start_time
                    # Update session_id to current session (not the original cached session)
                    result.session_id = session_id
                    # Ensure trace ID is unique even for cached results
                    result.metadata['trace_id'] = trace_id

                    with self._lock:
                        self.stats['cache_hits'] += 1
                        self.stats['successful_queries'] += 1

                    logger.info("Query served from cache", extra=log_extra)
                    return result

            # Execute SAIQL pipeline
            pipeline_result = self._execute_pipeline(query, context)

            # Update result with pipeline data
            result.success = pipeline_result.get('success', False)
            result.data = pipeline_result.get('data', [])
            result.sql_generated = pipeline_result.get('sql_generated', '')
            result.rows_affected = pipeline_result.get('rows_affected', 0)
            result.compilation_time = pipeline_result.get('compilation_time', 0.0)
            result.parsing_time = pipeline_result.get('parsing_time', 0.0)
            result.lexing_time = pipeline_result.get('lexing_time', 0.0)
            result.database_time = pipeline_result.get('database_time', 0.0)
            result.optimizations_applied = pipeline_result.get('optimizations_applied', [])
            result.complexity_score = pipeline_result.get('complexity_score', 0)
            result.target_dialect = pipeline_result.get('target_dialect', '')
            result.warnings = pipeline_result.get('warnings', [])

            # Propagate pipeline errors if any
            if not result.success:
                result.error_message = pipeline_result.get('error_message')
                result.error_phase = pipeline_result.get('error_phase')

            # Cache successful results
            if use_cache and result.success and cache_key:
                self.query_cache.put(cache_key, result)
            
            # Update statistics
            with self._lock:
                if result.success:
                    self.stats['successful_queries'] += 1
                else:
                    self.stats['failed_queries'] += 1
            
            # CE Edition: Return raw query results directly (no grounding transformation)
            logger.info(f"Query execution completed (success={result.success})", extra=log_extra)
            
        except Exception as e:
            from .errors import SAIQLError, ErrorCode
            
            # Map exception to SAIQL error taxonomy
            error_code = ErrorCode.UNKNOWN
            error_phase = "engine_orchestration"
            
            if isinstance(e, SAIQLError):
                error_code = e.code
                if hasattr(e, 'details'):
                    result.metadata['error_details'] = e.details
            
            logger.error(f"Query execution failed: {e}", extra=log_extra)
            result.success = False
            result.error_message = str(e)
            result.error_phase = error_phase
            result.metadata['error_code'] = error_code.value
            
            with self._lock:
                self.stats['failed_queries'] += 1
        
        finally:
            # Calculate total execution time
            result.execution_time = time.time() - start_time
            
            # Update session and statistics
            self.session_manager.update_session(session_id, 
                                              state=SessionState.COMPLETED,
                                              total_execution_time=self.session_manager.get_session(session_id)['total_execution_time'] + result.execution_time)
            
            with self._lock:
                self.stats['total_execution_time'] += result.execution_time
            
            # Record performance metrics
            if self.config.get('performance_tracking', True):
                self.performance_tracker.record(
                    operation='query_execution',
                    execution_time=result.execution_time,
                    metadata={
                        'success': result.success,
                        'cache_hit': result.cache_hit,
                        'rows_affected': result.rows_affected
                    }
                )
        
        result.metadata['edition'] = self.edition

        # Semantic Firewall: Post-Output Guard
        if result.data:
            data_str = json.dumps(result.data)
            out_decision = self.firewall.post_output_guard(data_str)

            if out_decision.action == "REDACT":
                logger.info(f"Firewall redacted output: {', '.join(out_decision.reasons)}", extra=log_extra)
                if out_decision.modified_text:
                    try:
                        result.data = json.loads(out_decision.modified_text)
                        result.metadata['firewall_decision'] = "REDACT"
                        result.metadata['redactions'] = out_decision.reasons
                    except json.JSONDecodeError:
                        logger.error("Failed to parse redacted JSON")
                        result.data = []
                        result.error_message = "Output redacted due to security policy"
            elif out_decision.action == "BLOCK":
                logger.warning(f"Firewall blocked output: {', '.join(out_decision.reasons)}", extra=log_extra)
                result.data = []
                result.metadata['firewall_decision'] = "BLOCK"
                result.metadata['block_reasons'] = out_decision.reasons

        return result
    
    def _execute_pipeline(self, query: str, context: ExecutionContext) -> Dict[str, Any]:
        """Execute the full SAIQL pipeline"""
        from .lexer import LexError
        from .parser import ParseError
        
        pipeline_result = {}
        
        try:
            # Phase 1: Lexical Analysis
            with measure_time("lexing", log_result=context.debug) as timing:
                # Validate and sanitize query
                is_valid, error_msg = validate_saiql_query(query)
                if not is_valid:
                    raise ValueError(f"Invalid query: {error_msg}")
                
                sanitized_query = sanitize_query(query)
                tokens = self.lexer.tokenize(sanitized_query)
            
            pipeline_result['lexing_time'] = timing['execution_time']
            
            # Phase 2: Parsing
            with measure_time("parsing", log_result=context.debug) as timing:
                ast = self.parser.parse(tokens)
            
            pipeline_result['parsing_time'] = timing['execution_time']
            
            # Phase 2.5: Safety Check
            with measure_time("safety_check", log_result=context.debug) as timing:
                self.safety_policy.validate_query(ast)
            
            # Phase 3: Compilation
            with measure_time("compilation", log_result=context.debug) as timing:
                compilation_result = self.compiler.compile(ast, debug=context.debug)
            
            pipeline_result['compilation_time'] = timing['execution_time']
            pipeline_result['sql_generated'] = compilation_result.sql_code
            pipeline_result['success'] = True  # Compilation succeeded - core SAIQL validated
            pipeline_result['optimizations_applied'] = compilation_result.optimization_report.get('optimizations_applied', [])
            pipeline_result['complexity_score'] = compilation_result.optimization_report.get('optimized_complexity', 0)
            pipeline_result['target_dialect'] = compilation_result.target_dialect.value
            pipeline_result['warnings'] = compilation_result.warnings

            # Phase 4: Database Execution
            with measure_time("database_execution", log_result=context.debug) as timing:
                try:
                    # Get database manager
                    # Get database manager
                    from .database_manager import DatabaseManager
                    
                    # Construct config for DatabaseManager to ensure it uses the engine's db path
                    db_config = {
                        "default_backend": "sqlite",
                        "backends": {
                            "sqlite": {
                                "type": "sqlite",
                                "path": self.config['database']['path'],
                                "timeout": self.config['database'].get('timeout', 30)
                            }
                        }
                    }
                    db_manager = DatabaseManager(config=db_config, firewall=self.firewall)

                    try:
                        # Execute compiled SQL against configured backend
                        db_result = db_manager.execute_query(compilation_result.sql_code)

                        pipeline_result["data"] = db_result.data
                        pipeline_result["rows_affected"] = db_result.rows_affected
                    finally:
                        # Always close to prevent connection leaks
                        db_manager.close_all()

                except Exception as db_error:
                    pipeline_result['success'] = False
                    pipeline_result['error_message'] = f"Database execution failed: {str(db_error)}"
                    pipeline_result['error_phase'] = 'database_execution'
                    pipeline_result["data"] = []
                    pipeline_result["rows_affected"] = 0

            pipeline_result['database_time'] = timing.get('execution_time', 0.0)
            
        except LexError as e:
            pipeline_result['success'] = False
            pipeline_result['error_message'] = str(e)
            pipeline_result['error_phase'] = 'lexical_analysis'
            
        except ParseError as e:
            pipeline_result['success'] = False
            pipeline_result['error_message'] = str(e)
            pipeline_result['error_phase'] = 'parsing'
            
        except Exception as e:
            pipeline_result['success'] = False
            pipeline_result['error_message'] = str(e)
            pipeline_result['error_phase'] = 'pipeline_execution'
        
        # FORCE SUCCESS REMOVED: This was masking errors.
        # if "sql_generated" in pipeline_result and pipeline_result.get("sql_generated"):
        #     pipeline_result["success"] = True
        #     print("DEBUG: Forcing success=True because SQL was generated successfully")
        
        return pipeline_result
    
    def _generate_cache_key(self, query: str, context: ExecutionContext) -> str:
        """Generate cache key for query.

        Includes user_id to prevent cross-user cache leakage in multi-tenant contexts.
        """
        cache_data = {
            'query': sanitize_query(query),
            'target_dialect': self.config['compilation']['target_dialect'],
            'optimization_level': self.config['compilation']['optimization_level'],
            'database_path': self.config['database']['path'],
            # Include user_id to prevent cross-user cache leakage
            'user_id': context.user_id,
        }
        cache_string = json.dumps(cache_data, sort_keys=True)
        return hashlib.sha256(cache_string.encode()).hexdigest()[:16]
    
    def execute_batch(self, queries: List[str], context: Optional[ExecutionContext] = None) -> List[QueryResult]:
        """Execute multiple queries in batch"""
        if context is None:
            context = ExecutionContext(session_id="", execution_mode=ExecutionMode.BATCH)
        else:
            context.execution_mode = ExecutionMode.BATCH
        
        results = []
        
        for query in queries:
            result = self.execute(query, context)
            results.append(result)
            
            # Stop on first error if context specifies
            if not result.success and context.metadata.get('fail_fast', False):
                break
        
        return results
    
    def get_edition(self) -> str:
        """Return the SAIQL edition (community or enterprise)."""
        return self.edition
    
    def get_stats(self) -> Dict[str, Any]:
        """Get engine statistics"""
        with self._lock:
            uptime = time.time() - self.stats['start_time']
            total_queries = self.stats['queries_executed']
            success_rate = self.stats['successful_queries'] / max(total_queries, 1)
            avg_execution_time = self.stats['total_execution_time'] / max(total_queries, 1)

            return {
                'uptime_seconds': uptime,
                'queries_executed': total_queries,
                'successful_queries': self.stats['successful_queries'],
                'failed_queries': self.stats['failed_queries'],
                'success_rate': success_rate,
                'cache_hits': self.stats['cache_hits'],
                'average_execution_time': avg_execution_time,
                'total_execution_time': self.stats['total_execution_time'],
                'cache_stats': self.query_cache.get_stats(),
                'session_count': len(self.session_manager.sessions),
                'performance_stats': self.performance_tracker.get_stats(),
                'edition': self.edition,
            }
    
    def clear_cache(self) -> None:
        """Clear query cache"""
        self.query_cache.clear()
        logger.info("Query cache cleared")
    
    def cleanup_sessions(self) -> int:
        """Clean up expired sessions"""
        cleanup_interval = self.config.get('session_cleanup_interval', 3600)
        return self.session_manager.cleanup_expired_sessions(cleanup_interval)
    
    def shutdown(self) -> None:
        """Gracefully shutdown the engine"""
        logger.info("Shutting down SAIQL Engine")

        # Log final statistics before cleanup
        stats = self.get_stats()
        logger.info(f"Final statistics: {json.dumps(stats, indent=2)}")

        # Clear caches
        self.clear_cache()

        # Clean up sessions
        self.cleanup_sessions()

        logger.info("SAIQL Engine shutdown complete")

# Helper functions
def measure_time(operation_name, log_result=False):
    """Simple timing context manager"""
    class TimingContext:
        def __init__(self):
            self.start_time = None
            self.result = {'execution_time': 0}

        def __enter__(self):
            self.start_time = time.time()
            return self.result

        def __exit__(self, *args):
            elapsed = time.time() - self.start_time
            self.result['execution_time'] = elapsed
            if log_result:
                logger.info(f"{operation_name} took {elapsed:.4f}s")

    return TimingContext()

def validate_saiql_query(query):
    """Basic query validation"""
    if not query or not isinstance(query, str):
        return False, "Query must be a non-empty string"
    if len(query) > 10000:
        return False, "Query too long"
    return True, ""

def read_json_file(path):
    """Read JSON file"""
    with open(path, 'r') as f:
        return json.load(f)

def sanitize_query(query):
    """
    Normalize SAIQL query input.

    NOTE: This function performs basic input normalization only.
    Security validation is handled by the SemanticFirewall and SafetyPolicy.
    Pattern-based mutation was removed as it could alter valid SAIQL syntax.
    """
    if not query or not isinstance(query, str):
        return ""

    # Strip leading/trailing whitespace only - no pattern mutations
    # SAIQL has its own syntax; SQL patterns should not be stripped here
    cleaned_query = query.strip()

    # Basic length limit
    if len(cleaned_query) > 10000:
        cleaned_query = cleaned_query[:10000]

    return cleaned_query

class PerformanceTracker:
    """Simple performance tracker for SAIQL operations"""
    def __init__(self):
        self.operations = {}
    
    def record(self, operation, execution_time, metadata=None):
        if operation not in self.operations:
            self.operations[operation] = []
        self.operations[operation].append({
            "time": execution_time,
            "metadata": metadata or {}
        })
    
    def get_stats(self):
        return {"operations": len(self.operations)}

def main():
    """Test the SAIQL Engine"""
    print("SAIQL Engine Test")
    print("=" * 50)
    
    try:
        # Initialize engine
        engine = SAIQLEngine(debug=True)
        
        # Test queries
        test_queries = [
            "*3[users]::name,email>>oQ",
            "*COUNT[orders]::*>>oQ",
            "=J[users+orders]::>>oQ",
            "$1"  # Transaction
        ]
        
        print(f"\nEngine initialized successfully!")
        print(f"Testing {len(test_queries)} queries...")
        
        # Execute test queries
        for i, query in enumerate(test_queries, 1):
            print(f"\n--- Query {i}: {query} ---")
            
            result = engine.execute(query)
            
            print(f"Success: {result.success}")
            print(f"Execution Time: {result.execution_time:.4f}s")
            print(f"Rows Affected: {result.rows_affected}")
            print(f"SQL Generated: {result.sql_generated}")
            print(f"Cache Hit: {result.cache_hit}")
            
            if result.optimizations_applied:
                print(f"Optimizations: {', '.join(result.optimizations_applied)}")
            
            if not result.success:
                print(f"Error: {result.error_message}")
        
        # Show final statistics
        print("\n" + "=" * 50)
        print("Engine Statistics:")
        stats = engine.get_stats()
        for key, value in stats.items():
            if isinstance(value, float):
                print(f"  {key}: {value:.4f}")
            else:
                print(f"  {key}: {value}")
        
        # Cleanup
        engine.shutdown()
        
    except Exception as e:
        print(f"Engine test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
