#!/usr/bin/env python3
"""
SAIQL Interactive Query Shell (Command-line shell for SAIQL)

A command-line interface for executing SAIQL queries in real-time.
Provides interactive parsing, execution, and result display with
debugging capabilities and human-readable translations.

Author: Apollo & Claude
Version: 1.0.0
Status: Foundation Phase
"""

import sys
import os
import readline
import json
from typing import List, Optional, Dict, Any
from datetime import datetime
import signal

# Add core modules to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from core import SAIQLParser, QueryComponents
from core.symbolic_engine import SymbolicEngine, ExecutionResult, ExecutionStatus

class SAIQLShell:
    """Interactive SAIQL Command Shell"""
    
    def __init__(self):
        self.parser = SAIQLParser()
        self.engine = SymbolicEngine()
        self.history = []
        self.debug_mode = False
        self.human_readable = False
        self.show_sql = True
        self.show_timing = True
        
        # Command aliases
        self.commands = {
            'help': self.show_help,
            'h': self.show_help,
            '?': self.show_help,
            'quit': self.quit_shell,
            'exit': self.quit_shell,
            'q': self.quit_shell,
            'clear': self.clear_screen,
            'cls': self.clear_screen,
            'history': self.show_history,
            'stats': self.show_stats,
            'debug': self.toggle_debug,
            'hr': self.toggle_human_readable,
            'sql': self.toggle_sql_display,
            'timing': self.toggle_timing,
            'tables': self.show_tables,
            'schema': self.show_schema,
            'examples': self.show_examples,
            'legend': self.show_legend_sample
        }
        
        # Set up signal handling for graceful exit
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # Initialize readline for command history
        self.setup_readline()

    def setup_readline(self):
        """Configure readline for command history and completion"""
        try:
            # Enable tab completion
            readline.set_completer(self.completer)
            readline.parse_and_bind("tab: complete")
            
            # Load command history if it exists
            history_file = os.path.expanduser("~/.saiql_history")
            if os.path.exists(history_file):
                readline.read_history_file(history_file)
                
        except ImportError:
            # readline not available on some systems
            pass

    def completer(self, text: str, state: int) -> Optional[str]:
        """Auto-completion for commands and SAIQL symbols"""
        options = []
        
        # Command completion
        if text.startswith('.'):
            command_options = [f".{cmd}" for cmd in self.commands.keys() if cmd.startswith(text[1:])]
            options.extend(command_options)
        else:
            # SAIQL symbol completion
            common_patterns = [
                "*3[", "+V[", "=J[", "*COUNT[", "*SUM[", "*AVG[",
                "$1", "$2", "$3", ">>oQ", "::", "+", "="
            ]
            options.extend([pattern for pattern in common_patterns if pattern.startswith(text)])
        
        try:
            return options[state]
        except IndexError:
            return None

    def signal_handler(self, signum, frame):
        """Handle Ctrl+C gracefully"""
        print("\n\nSAIQL Shell interrupted. Use .quit to exit gracefully.")
        self.display_prompt()

    def display_banner(self):
        """Display startup banner"""
        banner = """
===============================================================
                SAIQL Interactive Shell
            Semantic AI Query Language v1.0

  Ready to execute symbolic database queries!
  Type .help for commands or try: *3[users]::name>>oQ
===============================================================
        """
        print(banner)
        
        # Show quick stats
        try:
            stats = self.engine.get_stats()
            print(f"Database: Connected | Queries executed: {stats['total_queries']}")
        except Exception as e:
            print(f"Database connection: {e}")
        
        print()

    def display_prompt(self) -> str:
        """Display input prompt"""
        return "SAIQL> "

    def parse_command(self, input_line: str) -> tuple:
        """Parse input line for commands and queries"""
        input_line = input_line.strip()
        
        if input_line.startswith('.'):
            # Shell command
            parts = input_line[1:].split(' ', 1)
            command = parts[0].lower()
            args = parts[1] if len(parts) > 1 else ""
            return ('command', command, args)
        elif input_line == "":
            return ('empty', "", "")
        else:
            # SAIQL query
            return ('query', input_line, "")

    def execute_query(self, query: str):
        """Execute a SAIQL query and display results"""
        try:
            print(f"Parsing: {query}")
            
            # Parse the query
            components = self.parser.parse(query, debug=self.debug_mode)
            
            if self.human_readable:
                print(f"Human: {components.human_readable}")
            
            if self.show_sql:
                print(f"SQL: {components.sql_translation}")
            
            # Execute the query
            print("Executing...")
            result = self.engine.execute(components, debug=self.debug_mode)
            
            # Display results
            self.display_results(result)
            
            # Add to history
            self.history.append({
                'timestamp': datetime.now().isoformat(),
                'query': query,
                'status': result.status.value,
                'rows': result.rows_affected,
                'execution_time': result.execution_time
            })
            
        except Exception as e:
            print(f"Error: {e}")
            if self.debug_mode:
                import traceback
                traceback.print_exc()

    def display_results(self, result: ExecutionResult):
        """Display query execution results"""
        # Status indicator
        status_symbols = {
            ExecutionStatus.SUCCESS: "[OK]",
            ExecutionStatus.ERROR: "[ERROR]", 
            ExecutionStatus.WARNING: "[WARN]",
            ExecutionStatus.TIMEOUT: "[TIMEOUT]"
        }
        
        symbol = status_symbols.get(result.status, "[UNKNOWN]")
        print(f"\n{symbol} Status: {result.status.value}")
        
        if result.error_message:
            print(f"Error: {result.error_message}")
            return
        
        # Execution details
        if self.show_timing:
            print(f"Execution time: {result.execution_time:.4f}s")
        
        print(f"Rows affected/returned: {result.rows_affected}")
        
        # Data display
        if result.data:
            print(f"\nResults:")
            
            if len(result.data) == 1:
                # Single row - show as key-value pairs
                row = result.data[0]
                for key, value in row.items():
                    print(f"  {key}: {value}")
            elif len(result.data) <= 10:
                # Multiple rows - show as table
                self.display_table(result.data)
            else:
                # Many rows - show sample and count
                print(f"  Showing first 5 of {len(result.data)} rows:")
                self.display_table(result.data[:5])
        
        print()  # Empty line for readability

    def display_table(self, data: List[Dict[str, Any]]):
        """Display data in table format"""
        if not data:
            print("  No data returned")
            return
        
        # Get column names
        columns = list(data[0].keys())
        
        # Calculate column widths
        widths = {}
        for col in columns:
            widths[col] = max(len(col), max(len(str(row.get(col, ''))) for row in data))
        
        # Header
        header = "  " + " | ".join(col.ljust(widths[col]) for col in columns)
        print(header)
        print("  " + "-" * (len(header) - 2))
        
        # Rows
        for row in data:
            row_str = "  " + " | ".join(str(row.get(col, '')).ljust(widths[col]) for col in columns)
            print(row_str)

    def show_help(self, args: str = ""):
        """Display help information"""
        help_text = """
SAIQL Interactive Shell - Help

SAIQL QUERY SYNTAX:
  *3[table]::columns>>oQ     - SELECT query
  +V[table]::values>>oQ      - INSERT query  
  *4[table]::set+where>>oQ   - UPDATE query
  =J[table1+table2]::>>oQ    - JOIN query
  *COUNT[table]::*>>oQ       - COUNT aggregation
  $1                         - BEGIN transaction
  $2                         - COMMIT transaction
  $3                         - ROLLBACK transaction

SHELL COMMANDS:
  .help, .h, .?     - Show this help
  .quit, .exit, .q  - Exit SAIQL shell
  .clear, .cls      - Clear screen
  .history          - Show query history
  .stats            - Show execution statistics
  .debug            - Toggle debug mode
  .hr               - Toggle human-readable output
  .sql              - Toggle SQL translation display
  .timing           - Toggle execution timing
  .tables           - Show available tables
  .schema [table]   - Show table schema
  .examples         - Show example queries
  .legend           - Show symbol legend sample

EXAMPLES:
  *3[users]::name,email>>oQ
  *COUNT[orders]::*>>oQ
  =J[users+orders]::users.id=orders.user_id>>oQ
  $1
        """
        print(help_text)

    def show_history(self, args: str = ""):
        """Show command history"""
        if not self.history:
            print("No query history available")
            return
        
        print("\nQuery History:")
        print("-" * 60)
        
        for i, entry in enumerate(self.history[-10:], 1):  # Show last 10
            timestamp = entry['timestamp'][:19]  # Remove microseconds
            print(f"{i:2d}. [{timestamp}] {entry['query']}")
            print(f"    Status: {entry['status']} | Rows: {entry['rows']} | Time: {entry['execution_time']:.4f}s")

    def show_stats(self, args: str = ""):
        """Show execution statistics"""
        stats = self.engine.get_stats()
        
        print("\nExecution Statistics:")
        print("-" * 30)
        print(f"Total queries: {stats['total_queries']}")
        print(f"Successful: {stats['successful_queries']}")
        print(f"Failed: {stats['failed_queries']}")
        print(f"Average execution time: {stats['avg_execution_time']:.4f}s")
        
        if stats['total_queries'] > 0:
            success_rate = (stats['successful_queries'] / stats['total_queries']) * 100
            print(f"Success rate: {success_rate:.1f}%")

    def toggle_debug(self, args: str = ""):
        """Toggle debug mode"""
        self.debug_mode = not self.debug_mode
        print(f"Debug mode: {'ON' if self.debug_mode else 'OFF'}")

    def toggle_human_readable(self, args: str = ""):
        """Toggle human-readable output"""
        self.human_readable = not self.human_readable
        print(f"Human-readable output: {'ON' if self.human_readable else 'OFF'}")

    def toggle_sql_display(self, args: str = ""):
        """Toggle SQL translation display"""
        self.show_sql = not self.show_sql
        print(f"SQL translation display: {'ON' if self.show_sql else 'OFF'}")

    def toggle_timing(self, args: str = ""):
        """Toggle execution timing display"""
        self.show_timing = not self.show_timing
        print(f"Execution timing: {'ON' if self.show_timing else 'OFF'}")

    def show_tables(self, args: str = ""):
        """Show available database tables"""
        try:
            # Query SQLite master table
            with self.engine.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                tables = cursor.fetchall()
                
            print("\nAvailable Tables:")
            print("-" * 20)
            for table in tables:
                print(f"  {table[0]}")
                
        except Exception as e:
            print(f"Error retrieving tables: {e}")

    def show_schema(self, args: str = ""):
        """Show table schema"""
        table_name = args.strip()
        
        if not table_name:
            print("Usage: .schema <table_name>")
            return
            
        try:
            with self.engine.db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                
            if not columns:
                print(f"Table '{table_name}' not found")
                return
                
            print(f"\nSchema for table '{table_name}':")
            print("-" * 40)
            print("Column".ljust(20) + "Type".ljust(15) + "Null".ljust(8) + "Default")
            print("-" * 40)
            
            for col in columns:
                name = col[1]
                data_type = col[2]
                not_null = "NO" if col[3] else "YES"
                default = col[4] if col[4] else ""
                print(f"{name:<20}{data_type:<15}{not_null:<8}{default}")
                
        except Exception as e:
            print(f"Error retrieving schema: {e}")

    def show_examples(self, args: str = ""):
        """Show example SAIQL queries"""
        examples = [
            ("*3[users]::*>>oQ", "Select all users"),
            ("*3[users]::name,email>>oQ", "Select name and email from users"),
            ("*COUNT[orders]::*>>oQ", "Count all orders"),
            ("*SUM[orders]::price>>oQ", "Sum order prices"),
            ("=J[users+orders]::>>oQ", "Join users and orders"),
            ("+V[products]::laptop,999.99>>oQ", "Insert product"),
            ("*4[users]::email='new@email.com'>>oQ", "Update user email"),
            ("$1", "Begin transaction"),
            ("$2", "Commit transaction"),
            ("$3", "Rollback transaction")
        ]
        
        print("\nSAIQL Query Examples:")
        print("-" * 50)
        
        for query, description in examples:
            print(f"{query:<35} # {description}")

    def show_legend_sample(self, args: str = ""):
        """Show sample of symbol legend"""
        print("\nSAIQL Symbol Legend (Sample):")
        print("-" * 40)
        print("*3    - SELECT (getter method)")
        print("+V    - INSERT (add value)")
        print("*4    - UPDATE (setter method)")
        print("=J    - INNER JOIN")
        print("=L    - LEFT JOIN")
        print("*COUNT- COUNT function")
        print("*SUM  - SUM function")
        print("$1    - BEGIN transaction")
        print("$2    - COMMIT transaction")
        print(">>oQ  - Output to query result")
        print("::    - Namespace separator")
        print("[table] - Table container")

    def clear_screen(self, args: str = ""):
        """Clear the screen"""
        os.system('cls' if os.name == 'nt' else 'clear')

    def quit_shell(self, args: str = ""):
        """Exit the SAIQL shell"""
        # Save command history
        try:
            history_file = os.path.expanduser("~/.saiql_history")
            readline.write_history_file(history_file)
        except:
            pass
            
        print("\nGoodbye! SAIQL shell terminated.")
        sys.exit(0)

    def run(self):
        """Main shell loop"""
        self.display_banner()
        
        while True:
            try:
                # Get user input
                user_input = input(self.display_prompt())
                
                # Parse command type
                cmd_type, content, args = self.parse_command(user_input)
                
                if cmd_type == 'empty':
                    continue
                elif cmd_type == 'command':
                    # Execute shell command
                    if content in self.commands:
                        self.commands[content](args)
                    else:
                        print(f"Unknown command: .{content}")
                        print("Type .help for available commands")
                elif cmd_type == 'query':
                    # Execute SAIQL query
                    self.execute_query(content)
                    
            except EOFError:
                # Handle Ctrl+D
                print("\nGoodbye!")
                break
            except KeyboardInterrupt:
                # Handle Ctrl+C
                print("\nUse .quit to exit")
                continue
            except Exception as e:
                print(f"Shell error: {e}")
                if self.debug_mode:
                    import traceback
                    traceback.print_exc()

def main():
    """Entry point for SAIQL shell"""
    import argparse
    from core.schema_ir import RoutineCapability
    
    parser = argparse.ArgumentParser(description="SAIQL Interface")
    parser.add_argument("--migrate", action="store_true", help="Run in migration mode")
    parser.add_argument("--source", type=str, help="Source backend (e.g. oracle)")
    parser.add_argument("--target", type=str, help="Target backend (e.g. postgres)")
    parser.add_argument("--routines-mode", type=str, choices=['none', 'analyze', 'stub', 'subset_translate'], default='none')
    parser.add_argument("--routines-out", type=str, default="./migration_report")
    
    args, unknown = parser.parse_known_args()
    
    if args.migrate:
        # Migration Mode
        if not args.source or not args.target:
            print("Error: --source and --target required for migration")
            sys.exit(1)
            
        print(f"Starting SAIQL Migration: {args.source} -> {args.target}")
        
        # Map mode string to Enum
        mode_map = {
            'none': RoutineCapability.NONE,
            'analyze': RoutineCapability.ANALYZE,
            'stub': RoutineCapability.STUB,
            'subset_translate': RoutineCapability.SUBSET_TRANSLATE
        }
        
        # Construct config
        # In real usage, we should merge this with secure_config/database_config
        config = {
            "default_backend": args.target, # Arbitrary default for manager
            # Backends config is loaded by manager from file usually.
            # We assume database_config.json exists and has entries for args.source/args.target
            "routines": {
                "mode": mode_map[args.routines_mode],
                "out_dir": args.routines_out
            }
        }
        
        from core.migration import MigrationRunner
        runner = MigrationRunner(config)
        runner.run(args.source, args.target)
        
    else:
        # Interactive Shell
        shell = SAIQLShell()
        shell.run()

if __name__ == "__main__":
    main()
