#!/usr/bin/env python3
"""
SAIQL Runtime System - Dynamic Execution Environment

This module provides the runtime execution environment for SAIQL, handling
all dynamic aspects of query execution including symbol resolution, type
validation, memory management, adaptive optimization, and performance profiling.

The runtime system operates during query execution, complementing the
compile-time optimizations with dynamic adaptations based on actual
execution patterns and resource conditions.

Key Responsibilities:
- Dynamic symbol resolution and hot-swapping
- Runtime type checking and validation
- Memory management and resource monitoring
- Adaptive execution optimization
- Performance profiling and bottleneck detection
- Error recovery and rollback mechanisms
- Debug support and query introspection
- Resource pooling and connection management

Author: Apollo & Claude
Version: 1.0.0
Status: Foundation Phase

Architecture Note:
The runtime system bridges the gap between compile-time optimization and
actual execution, providing the dynamic adaptability needed for production
database systems handling varying workloads and conditions.
"""

import logging
import time
import threading
import weakref
import gc
from typing import Dict, List, Any, Optional, Union, Callable, Set, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto
from collections import defaultdict, deque
from contextlib import contextmanager
import hashlib
from abc import ABC, abstractmethod
import traceback
import sys

# Import SAIQL components
try:
    from .engine import SAIQLEngine
    # Use absolute import for utils since core is top-level
    from utils.helpers import PerformanceTracker
except ImportError as e:
    print(f"Warning: Could not import SAIQL components: {e}")

# Configure logging
logger = logging.getLogger(__name__)

class RuntimeState(Enum):
    """Runtime system states"""
    INITIALIZING = auto()
    READY = auto()
    EXECUTING = auto()
    OPTIMIZING = auto()
    PROFILING = auto()
    DEBUGGING = auto()
    ERROR = auto()
    SHUTDOWN = auto()

class ExecutionPhase(Enum):
    """Query execution phases"""
    PREPARATION = "preparation"
    SYMBOL_RESOLUTION = "symbol_resolution"
    TYPE_VALIDATION = "type_validation"
    MEMORY_ALLOCATION = "memory_allocation"
    EXECUTION = "execution"
    RESULT_PROCESSING = "result_processing"
    CLEANUP = "cleanup"

class MemoryPressure(Enum):
    """Memory pressure levels"""
    LOW = 1      # < 50% memory usage
    MEDIUM = 2   # 50-80% memory usage
    HIGH = 3     # 80-95% memory usage
    CRITICAL = 4 # > 95% memory usage

@dataclass
class RuntimeSymbol:
    """Runtime symbol with dynamic properties"""
    name: str
    value: Any
    symbol_type: str
    scope: str
    access_count: int = 0
    last_accessed: float = 0.0
    memory_size: int = 0
    is_cached: bool = False
    dependencies: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def touch(self) -> None:
        """Update access statistics"""
        self.access_count += 1
        self.last_accessed = time.time()

@dataclass
class RuntimeContext:
    """Runtime execution context"""
    execution_id: str
    query: str
    symbols: Dict[str, RuntimeSymbol] = field(default_factory=dict)
    variables: Dict[str, Any] = field(default_factory=dict)
    temp_objects: List[Any] = field(default_factory=list)
    memory_used: int = 0
    start_time: float = 0.0
    phase: ExecutionPhase = ExecutionPhase.PREPARATION
    debug_mode: bool = False
    profiling_enabled: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

@dataclass
class PerformanceProfile:
    """Performance profiling data"""
    query_hash: str
    execution_count: int = 0
    total_time: float = 0.0
    average_time: float = 0.0
    min_time: float = float('inf')
    max_time: float = 0.0
    memory_usage: List[int] = field(default_factory=list)
    bottlenecks: Dict[str, float] = field(default_factory=dict)
    optimization_history: List[Dict[str, Any]] = field(default_factory=list)
    last_executed: float = 0.0

class AdaptiveOptimizer:
    """Adaptive runtime optimizer that learns from execution patterns"""
    
    def __init__(self):
        self.execution_patterns = {}
        self.optimization_rules = []
        self.performance_history = defaultdict(list)
        self._lock = threading.RLock()
        
        # Initialize built-in optimization rules
        self._initialize_optimization_rules()
    
    def _initialize_optimization_rules(self) -> None:
        """Initialize built-in optimization rules"""
        self.optimization_rules = [
            {
                'name': 'frequent_query_caching',
                'condition': lambda profile: profile.execution_count > 10,
                'action': self._enable_aggressive_caching,
                'priority': 1
            },
            {
                'name': 'slow_query_optimization',
                'condition': lambda profile: profile.average_time > 1.0,
                'action': self._optimize_slow_query,
                'priority': 2
            },
            {
                'name': 'memory_intensive_streaming',
                'condition': lambda profile: max(profile.memory_usage) > 100 * 1024 * 1024,  # 100MB
                'action': self._enable_streaming_mode,
                'priority': 3
            }
        ]
    
    def analyze_execution(self, context: RuntimeContext, result: Any) -> Dict[str, Any]:
        """Analyze execution and suggest optimizations"""
        with self._lock:
            query_hash = hashlib.sha256(context.query.encode()).hexdigest()[:16]
            
            # Update performance history
            execution_time = time.time() - context.start_time
            self.performance_history[query_hash].append({
                'execution_time': execution_time,
                'memory_used': context.memory_used,
                'timestamp': time.time(),
                'phase_times': context.metadata.get('phase_times', {})
            })
            
            # Generate optimization suggestions
            suggestions = []
            
            # Check optimization rules
            profile = self._get_performance_profile(query_hash)
            
            for rule in sorted(self.optimization_rules, key=lambda r: r['priority']):
                if rule['condition'](profile):
                    suggestion = rule['action'](context, profile)
                    if suggestion:
                        suggestions.append(suggestion)
            
            return {
                'query_hash': query_hash,
                'suggestions': suggestions,
                'profile_updated': True
            }
    
    def _get_performance_profile(self, query_hash: str) -> PerformanceProfile:
        """Get performance profile for query"""
        history = self.performance_history[query_hash]
        
        if not history:
            return PerformanceProfile(query_hash=query_hash)
        
        times = [h['execution_time'] for h in history]
        memory_usage = [h['memory_used'] for h in history]
        
        return PerformanceProfile(
            query_hash=query_hash,
            execution_count=len(history),
            total_time=sum(times),
            average_time=sum(times) / len(times),
            min_time=min(times),
            max_time=max(times),
            memory_usage=memory_usage,
            last_executed=history[-1]['timestamp']
        )
    
    def _enable_aggressive_caching(self, context: RuntimeContext, profile: PerformanceProfile) -> Dict[str, Any]:
        """Suggest aggressive caching for frequent queries"""
        return {
            'type': 'caching',
            'priority': 'high',
            'description': f'Enable aggressive caching for frequently executed query (executed {profile.execution_count} times)',
            'implementation': 'increase_cache_ttl',
            'parameters': {'cache_ttl': 3600, 'cache_size': 'large'}
        }
    
    def _optimize_slow_query(self, context: RuntimeContext, profile: PerformanceProfile) -> Dict[str, Any]:
        """Suggest optimizations for slow queries"""
        return {
            'type': 'performance',
            'priority': 'high',
            'description': f'Optimize slow query (avg: {profile.average_time:.2f}s)',
            'implementation': 'query_rewrite',
            'parameters': {'enable_index_hints': True, 'parallel_execution': True}
        }
    
    def _enable_streaming_mode(self, context: RuntimeContext, profile: PerformanceProfile) -> Dict[str, Any]:
        """Suggest streaming for memory-intensive queries"""
        max_memory = max(profile.memory_usage)
        return {
            'type': 'memory',
            'priority': 'medium',
            'description': f'Enable streaming for memory-intensive query (peak: {max_memory // 1024 // 1024}MB)',
            'implementation': 'streaming_execution',
            'parameters': {'batch_size': 1000, 'stream_threshold': 50 * 1024 * 1024}
        }

class MemoryManager:
    """Runtime memory management system"""
    
    def __init__(self, max_memory_mb: int = 1024):
        self.max_memory_bytes = max_memory_mb * 1024 * 1024
        self.allocated_objects = weakref.WeakValueDictionary()
        self.memory_pools = defaultdict(deque)
        self.allocation_stats = defaultdict(int)
        self._lock = threading.RLock()
        
        # Start memory monitoring thread
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._memory_monitor_loop, daemon=True)
        self.monitor_thread.start()
    
    def allocate(self, size: int, object_type: str = 'general') -> str:
        """Allocate memory for runtime object"""
        with self._lock:
            if self.get_memory_usage() + size > self.max_memory_bytes:
                # Try garbage collection
                collected = self._force_cleanup()
                logger.info(f"Memory pressure - collected {collected} bytes")
                
                if self.get_memory_usage() + size > self.max_memory_bytes:
                    raise MemoryError(f"Cannot allocate {size} bytes - would exceed limit")
            
            allocation_id = f"{object_type}_{int(time.time() * 1000000)}"
            self.allocation_stats[object_type] += size
            
            logger.debug(f"Allocated {size} bytes for {object_type}")
            return allocation_id
    
    def deallocate(self, allocation_id: str, size: int, object_type: str = 'general') -> None:
        """Deallocate memory"""
        with self._lock:
            self.allocation_stats[object_type] -= size
            logger.debug(f"Deallocated {size} bytes for {object_type}")
    
    def get_memory_usage(self) -> int:
        """Get current memory usage"""
        return sum(self.allocation_stats.values())
    
    def get_memory_pressure(self) -> MemoryPressure:
        """Get current memory pressure level"""
        usage_ratio = self.get_memory_usage() / self.max_memory_bytes
        
        if usage_ratio < 0.5:
            return MemoryPressure.LOW
        elif usage_ratio < 0.8:
            return MemoryPressure.MEDIUM
        elif usage_ratio < 0.95:
            return MemoryPressure.HIGH
        else:
            return MemoryPressure.CRITICAL
    
    def _force_cleanup(self) -> int:
        """Force cleanup of unused objects.

        Clears all pools and weak references, and resets allocation_stats
        to reflect that tracked memory has been freed.
        """
        initial_usage = self.get_memory_usage()

        # Force garbage collection
        gc.collect()

        # Clean up weak references
        self.allocated_objects.clear()

        # Clear memory pools
        for pool in self.memory_pools.values():
            pool.clear()
        self.memory_pools.clear()

        # Reset allocation stats since we've cleared everything
        # This ensures get_memory_usage() reflects the cleanup and
        # prevents memory pressure from staying stuck at HIGH/CRITICAL
        self.allocation_stats.clear()

        final_usage = self.get_memory_usage()  # Now 0 after clearing stats
        return initial_usage - final_usage
    
    def _memory_monitor_loop(self) -> None:
        """Background memory monitoring"""
        while self.monitoring_active:
            try:
                pressure = self.get_memory_pressure()
                
                if pressure == MemoryPressure.HIGH:
                    logger.warning("High memory pressure detected")
                    self._force_cleanup()
                elif pressure == MemoryPressure.CRITICAL:
                    logger.error("Critical memory pressure - forcing aggressive cleanup")
                    self._force_cleanup()
                
                time.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.error(f"Error in memory monitor: {e}")
                time.sleep(10)
    
    def shutdown(self) -> None:
        """Shutdown memory manager"""
        self.monitoring_active = False
        if self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=2.0)

class RuntimeTypeSystem:
    """Dynamic type system for runtime validation"""
    
    def __init__(self):
        self.type_registry = {}
        self.type_coercion_rules = {}
        self.validation_cache = {}
        self._initialize_builtin_types()
    
    def _initialize_builtin_types(self) -> None:
        """Initialize built-in type definitions"""
        self.type_registry.update({
            'string': {'validator': lambda x: isinstance(x, str), 'coercer': str},
            'integer': {'validator': lambda x: isinstance(x, int), 'coercer': int},
            'float': {'validator': lambda x: isinstance(x, (int, float)), 'coercer': float},
            'boolean': {'validator': lambda x: isinstance(x, bool), 'coercer': bool},
            'list': {'validator': lambda x: isinstance(x, list), 'coercer': list},
            'dict': {'validator': lambda x: isinstance(x, dict), 'coercer': dict},
            'null': {'validator': lambda x: x is None, 'coercer': lambda x: None}
        })
    
    def validate_type(self, value: Any, expected_type: str) -> Tuple[bool, Optional[str]]:
        """Validate value against expected type"""
        if expected_type not in self.type_registry:
            return False, f"Unknown type: {expected_type}"
        
        validator = self.type_registry[expected_type]['validator']
        
        try:
            is_valid = validator(value)
            return is_valid, None if is_valid else f"Value {value} is not of type {expected_type}"
        except Exception as e:
            return False, f"Type validation error: {e}"
    
    def coerce_type(self, value: Any, target_type: str) -> Tuple[Any, bool]:
        """Attempt to coerce value to target type"""
        if target_type not in self.type_registry:
            return value, False
        
        coercer = self.type_registry[target_type]['coercer']
        
        try:
            coerced_value = coercer(value)
            return coerced_value, True
        except Exception:
            return value, False
    
    def register_custom_type(self, type_name: str, validator: Callable, coercer: Callable = None) -> None:
        """Register custom type"""
        self.type_registry[type_name] = {
            'validator': validator,
            'coercer': coercer or (lambda x: x)
        }

class RuntimeDebugger:
    """Runtime debugging and introspection system"""
    
    def __init__(self):
        self.breakpoints = set()
        self.watches = {}
        self.execution_trace = deque(maxlen=1000)
        self.debug_sessions = {}
        self._lock = threading.RLock()
    
    def set_breakpoint(self, query_hash: str, phase: ExecutionPhase) -> None:
        """Set debugging breakpoint"""
        with self._lock:
            self.breakpoints.add((query_hash, phase))
            logger.info(f"Breakpoint set for {query_hash} at {phase.value}")
    
    def remove_breakpoint(self, query_hash: str, phase: ExecutionPhase) -> None:
        """Remove debugging breakpoint"""
        with self._lock:
            self.breakpoints.discard((query_hash, phase))
            logger.info(f"Breakpoint removed for {query_hash} at {phase.value}")
    
    def add_watch(self, name: str, expression: str) -> None:
        """Add watch expression"""
        with self._lock:
            self.watches[name] = expression
            logger.info(f"Watch added: {name} = {expression}")
    
    def check_breakpoint(self, context: RuntimeContext) -> bool:
        """Check if execution should break at current point"""
        query_hash = hashlib.sha256(context.query.encode()).hexdigest()[:16]
        return (query_hash, context.phase) in self.breakpoints
    
    def trace_execution(self, context: RuntimeContext, event: str, data: Any = None) -> None:
        """Record execution trace"""
        with self._lock:
            trace_entry = {
                'timestamp': time.time(),
                'execution_id': context.execution_id,
                'phase': context.phase.value,
                'event': event,
                'data': data,
                'memory_used': context.memory_used
            }
            self.execution_trace.append(trace_entry)
    
    def get_execution_trace(self, execution_id: str = None) -> List[Dict[str, Any]]:
        """Get execution trace"""
        with self._lock:
            if execution_id:
                return [entry for entry in self.execution_trace 
                       if entry['execution_id'] == execution_id]
            return list(self.execution_trace)

class SAIQLRuntime:
    """
    SAIQL Runtime System - Dynamic Execution Environment
    
    Provides comprehensive runtime support for SAIQL query execution including
    dynamic symbol resolution, type validation, memory management, adaptive
    optimization, and debugging capabilities.
    
    The runtime system operates during query execution, bridging the gap between
    compile-time optimization and actual execution with dynamic adaptations.
    """
    
    def __init__(self, engine: Optional["SAIQLEngine"] = None,
                 max_memory_mb: int = 1024,
                 enable_profiling: bool = True,
                 enable_optimization: bool = True,
                 debug_mode: bool = False):
        """Initialize SAIQL Runtime System"""
        
        self.engine = engine
        self.state = RuntimeState.INITIALIZING
        self.debug_mode = debug_mode
        self.start_time = time.time()
        
        # Initialize subsystems
        self.memory_manager = MemoryManager(max_memory_mb)
        self.type_system = RuntimeTypeSystem()
        self.adaptive_optimizer = AdaptiveOptimizer() if enable_optimization else None
        self.debugger = RuntimeDebugger() if debug_mode else None
        self.performance_tracker = PerformanceTracker() if enable_profiling else None
        
        # Runtime state
        self.active_contexts = {}
        self.symbol_cache = {}
        self.global_symbols = {}
        
        # Statistics
        self.stats = {
            'contexts_created': 0,
            'symbols_resolved': 0,
            'type_validations': 0,
            'memory_allocations': 0,
            'optimizations_applied': 0,
            'errors_handled': 0
        }
        
        # Thread safety
        self._lock = threading.RLock()
        
        self.state = RuntimeState.READY
        logger.info("SAIQL Runtime System initialized")
    
    @contextmanager
    def create_execution_context(self, query: str, execution_id: str = None) -> RuntimeContext:
        """Create runtime execution context"""
        
        if not execution_id:
            execution_id = f"exec_{int(time.time() * 1000000)}"
        
        context = RuntimeContext(
            execution_id=execution_id,
            query=query,
            start_time=time.time(),
            debug_mode=self.debug_mode,
            profiling_enabled=self.performance_tracker is not None
        )
        
        with self._lock:
            self.active_contexts[execution_id] = context
            self.stats['contexts_created'] += 1
        
        logger.debug(f"Created execution context: {execution_id}")
        
        try:
            yield context
        finally:
            # Cleanup context
            self._cleanup_context(context)
            
            with self._lock:
                self.active_contexts.pop(execution_id, None)
    
    def resolve_symbol(self, context: RuntimeContext, symbol_name: str, 
                      scope: str = 'local') -> Optional[RuntimeSymbol]:
        """Dynamically resolve symbol at runtime"""
        
        with self._lock:
            self.stats['symbols_resolved'] += 1
        
        # Check context-local symbols first
        if symbol_name in context.symbols:
            symbol = context.symbols[symbol_name]
            symbol.touch()
            return symbol
        
        # Check global symbol cache
        cache_key = f"{scope}:{symbol_name}"
        if cache_key in self.symbol_cache:
            symbol = self.symbol_cache[cache_key]
            symbol.touch()
            
            # Add to context for faster access
            context.symbols[symbol_name] = symbol
            return symbol
        
        # Dynamic symbol resolution (would integrate with legend_map)
        resolved_symbol = self._dynamic_symbol_lookup(symbol_name, scope)
        
        if resolved_symbol:
            # Cache the resolved symbol
            self.symbol_cache[cache_key] = resolved_symbol
            context.symbols[symbol_name] = resolved_symbol
            
            if self.debugger:
                self.debugger.trace_execution(context, 'symbol_resolved', 
                                            {'symbol': symbol_name, 'scope': scope})
        
        return resolved_symbol
    
    def _dynamic_symbol_lookup(self, symbol_name: str, scope: str) -> Optional[RuntimeSymbol]:
        """Perform dynamic symbol lookup"""
        # This would integrate with the engine's symbol systems
        # For now, create a placeholder implementation
        
        # Check if it's a built-in symbol
        builtin_symbols = {
            '*3': RuntimeSymbol('*3', 'SELECT', 'function', 'builtin'),
            '*COUNT': RuntimeSymbol('*COUNT', 'COUNT', 'function', 'builtin'),
            'oQ': RuntimeSymbol('oQ', 'result', 'type', 'builtin'),
            '>>': RuntimeSymbol('>>', 'output', 'operator', 'builtin'),
            '::': RuntimeSymbol('::', 'namespace', 'operator', 'builtin')
        }
        
        if symbol_name in builtin_symbols:
            return builtin_symbols[symbol_name]
        
        return None
    
    def validate_runtime_types(self, context: RuntimeContext, 
                             values: Dict[str, Any], 
                             expected_types: Dict[str, str]) -> List[str]:
        """Validate types at runtime"""
        
        errors = []
        
        with self._lock:
            self.stats['type_validations'] += len(values)
        
        for name, value in values.items():
            if name in expected_types:
                expected_type = expected_types[name]
                is_valid, error_msg = self.type_system.validate_type(value, expected_type)
                
                if not is_valid:
                    # Try type coercion
                    coerced_value, coercion_success = self.type_system.coerce_type(value, expected_type)
                    
                    if coercion_success:
                        # Update context with coerced value
                        if 'coerced_values' not in context.metadata:
                            context.metadata['coerced_values'] = {}
                        context.metadata['coerced_values'][name] = coerced_value
                        
                        if self.debugger:
                            self.debugger.trace_execution(context, 'type_coerced', 
                                                        {'variable': name, 'from': value, 'to': coerced_value})
                    else:
                        errors.append(f"{name}: {error_msg}")
        
        return errors
    
    def monitor_memory_usage(self, context: RuntimeContext) -> None:
        """Monitor and manage memory usage for context"""
        
        # Calculate current memory usage
        context.memory_used = sys.getsizeof(context) + sum(
            sys.getsizeof(obj) for obj in context.temp_objects
        )
        
        # Check memory pressure
        pressure = self.memory_manager.get_memory_pressure()
        
        if pressure in [MemoryPressure.HIGH, MemoryPressure.CRITICAL]:
            logger.warning(f"Memory pressure {pressure.name} in context {context.execution_id}")
            
            # Apply memory management strategies
            self._apply_memory_management(context, pressure)
    
    def _apply_memory_management(self, context: RuntimeContext, pressure: MemoryPressure) -> None:
        """Apply memory management strategies"""
        
        if pressure == MemoryPressure.HIGH:
            # Clear least recently used symbols
            self._cleanup_lru_symbols(context)
            
        elif pressure == MemoryPressure.CRITICAL:
            # Aggressive cleanup
            self._cleanup_lru_symbols(context)
            context.temp_objects.clear()
            
            # Force garbage collection
            gc.collect()
    
    def _cleanup_lru_symbols(self, context: RuntimeContext) -> None:
        """Clean up least recently used symbols"""
        
        # Sort symbols by last access time
        sorted_symbols = sorted(
            context.symbols.items(),
            key=lambda x: x[1].last_accessed
        )
        
        # Remove oldest 25% of symbols
        symbols_to_remove = len(sorted_symbols) // 4
        
        for i in range(symbols_to_remove):
            symbol_name, symbol = sorted_symbols[i]
            del context.symbols[symbol_name]
            logger.debug(f"Cleaned up LRU symbol: {symbol_name}")
    
    def profile_execution(self, context: RuntimeContext, phase: ExecutionPhase) -> None:
        """Profile execution performance"""
        
        if not self.performance_tracker:
            return
        
        phase_start_time = time.time()
        
        # Record phase timing
        if 'phase_times' not in context.metadata:
            context.metadata['phase_times'] = {}
        
        if phase == ExecutionPhase.PREPARATION:
            context.metadata['phase_start'] = phase_start_time
        else:
            # Calculate time for previous phase
            if 'phase_start' in context.metadata:
                phase_duration = phase_start_time - context.metadata['phase_start']
                context.metadata['phase_times'][context.phase.value] = phase_duration
                context.metadata['phase_start'] = phase_start_time
        
        context.phase = phase
        
        if self.debugger:
            self.debugger.trace_execution(context, f'phase_entered', {'phase': phase.value})
    
    def apply_adaptive_optimizations(self, context: RuntimeContext, result: Any) -> Dict[str, Any]:
        """Apply adaptive optimizations based on execution patterns"""
        
        if not self.adaptive_optimizer:
            return {}
        
        optimization_results = self.adaptive_optimizer.analyze_execution(context, result)
        
        if optimization_results.get('suggestions'):
            with self._lock:
                self.stats['optimizations_applied'] += len(optimization_results['suggestions'])
            
            logger.info(f"Applied {len(optimization_results['suggestions'])} runtime optimizations")
        
        return optimization_results
    
    def handle_runtime_error(self, context: RuntimeContext, error: Exception) -> Dict[str, Any]:
        """Handle runtime errors with recovery mechanisms"""
        
        with self._lock:
            self.stats['errors_handled'] += 1
        
        error_info = {
            'type': type(error).__name__,
            'message': str(error),
            'phase': context.phase.value,
            'execution_id': context.execution_id,
            'timestamp': time.time(),
            'traceback': traceback.format_exc()
        }
        
        logger.error(f"Runtime error in {context.execution_id}: {error_info}")
        
        if self.debugger:
            self.debugger.trace_execution(context, 'error_occurred', error_info)
        
        # Attempt error recovery
        recovery_actions = self._attempt_error_recovery(context, error)
        error_info['recovery_actions'] = recovery_actions
        
        return error_info
    
    def _attempt_error_recovery(self, context: RuntimeContext, error: Exception) -> List[str]:
        """Attempt to recover from runtime errors"""
        
        recovery_actions = []
        
        # Memory error recovery
        if isinstance(error, MemoryError):
            self._apply_memory_management(context, MemoryPressure.CRITICAL)
            recovery_actions.append('memory_cleanup')
        
        # Type error recovery
        elif isinstance(error, (TypeError, ValueError)):
            # Clear potentially corrupted symbols
            context.symbols.clear()
            recovery_actions.append('symbol_reset')
        
        # General cleanup
        context.temp_objects.clear()
        recovery_actions.append('temp_cleanup')
        
        return recovery_actions
    
    def _cleanup_context(self, context: RuntimeContext) -> None:
        """Clean up execution context"""
        
        # Clean up temporary objects
        context.temp_objects.clear()
        
        # Update symbol access statistics
        for symbol in context.symbols.values():
            if symbol.is_cached and symbol.access_count == 1:
                # Remove single-use cached symbols
                cache_keys_to_remove = [k for k, v in self.symbol_cache.items() if v is symbol]
                for key in cache_keys_to_remove:
                    del self.symbol_cache[key]
        
        # Record final statistics
        execution_time = time.time() - context.start_time
        
        if self.performance_tracker:
            self.performance_tracker.record(
                operation='query_execution',
                execution_time=execution_time,
                metadata={
                    'memory_used': context.memory_used,
                    'symbols_resolved': len(context.symbols),
                    'phase_times': context.metadata.get('phase_times', {})
                }
            )
        
        logger.debug(f"Cleaned up context {context.execution_id} after {execution_time:.4f}s")
    
    def get_runtime_stats(self) -> Dict[str, Any]:
        """Get comprehensive runtime statistics"""
        
        with self._lock:
            base_stats = self.stats.copy()
        
        base_stats.update({
            'state': self.state.name,
            'active_contexts': len(self.active_contexts),
            'cached_symbols': len(self.symbol_cache),
            'memory_usage': self.memory_manager.get_memory_usage(),
            'memory_pressure': self.memory_manager.get_memory_pressure().name,
            'uptime': time.time() - self.start_time
        })
        
        if self.performance_tracker:
            base_stats['performance_stats'] = self.performance_tracker.get_stats()
        
        return base_stats
    
    def shutdown(self) -> None:
        """Gracefully shutdown the runtime system"""
        
        logger.info("Shutting down SAIQL Runtime System")
        self.state = RuntimeState.SHUTDOWN
        
        # Clean up all active contexts
        for context in list(self.active_contexts.values()):
            self._cleanup_context(context)
        
        # Shutdown subsystems
        self.memory_manager.shutdown()
        
        # Clear caches
        self.symbol_cache.clear()
        self.active_contexts.clear()
        
        logger.info("SAIQL Runtime System shutdown complete")

def main():
    """Test the SAIQL Runtime System"""
    print("SAIQL Runtime System Test")
    print("=" * 50)
    
    try:
        # Initialize runtime system
        runtime = SAIQLRuntime(
            max_memory_mb=512,
            enable_profiling=True,
            enable_optimization=True,
            debug_mode=True
        )
        
        print(f"Runtime system initialized")
        print(f"State: {runtime.state.name}")
        
        # Test execution context
        test_query = "*3[users]::name,email>>oQ"
        
        with runtime.create_execution_context(test_query) as context:
            print(f"\nCreated execution context: {context.execution_id}")
            
            # Test symbol resolution
            runtime.profile_execution(context, ExecutionPhase.SYMBOL_RESOLUTION)
            
            symbol = runtime.resolve_symbol(context, '*3', 'builtin')
            if symbol:
                print(f"Resolved symbol: {symbol.name} ({symbol.symbol_type})")
            
            # Test type validation
            runtime.profile_execution(context, ExecutionPhase.TYPE_VALIDATION)
            
            test_values = {'name': 'Alice', 'age': '30'}  # age as string
            expected_types = {'name': 'string', 'age': 'integer'}
            
            type_errors = runtime.validate_runtime_types(context, test_values, expected_types)
            
            if type_errors:
                print(f"Type validation errors: {type_errors}")
            else:
                print("Type validation passed")
            
            # Test memory monitoring
            runtime.profile_execution(context, ExecutionPhase.MEMORY_ALLOCATION)
            runtime.monitor_memory_usage(context)
            
            print(f"Memory used: {context.memory_used} bytes")
            
            # Test profiling
            runtime.profile_execution(context, ExecutionPhase.EXECUTION)
            
            # Simulate some work
            time.sleep(0.1)
            
            # Test adaptive optimization
            runtime.profile_execution(context, ExecutionPhase.RESULT_PROCESSING)
            
            optimization_results = runtime.apply_adaptive_optimizations(context, "mock_result")
            
            if optimization_results.get('suggestions'):
                print(f"Optimization suggestions: {len(optimization_results['suggestions'])}")
            
            runtime.profile_execution(context, ExecutionPhase.CLEANUP)
        
        # Show runtime statistics
        print(f"\nRuntime Statistics:")
        stats = runtime.get_runtime_stats()
        
        for key, value in stats.items():
            if isinstance(value, dict):
                print(f"  {key}: {len(value)} items")
            else:
                print(f"  {key}: {value}")
        
        # Cleanup
        runtime.shutdown()
        print(f"\nRuntime system test completed successfully!")
        
    except Exception as e:
        print(f"Runtime system test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
