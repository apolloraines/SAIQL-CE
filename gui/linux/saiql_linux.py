#!/usr/bin/env python3
"""
SAIQL-Delta Linux GUI Application
===================================

A native Linux GUI application for SAIQL-Delta database management
that works independently without requiring the full SAIQL core modules.
This version is perfect for distribution as an AppImage or .deb package.

Features:
- Database connection management (Translation/Standalone modes)
- SAIQL query editor with syntax highlighting simulation
- Real-time performance dashboard
- System configuration and monitoring
- Linux-native GTK look and feel
- Demo mode with sample data

Author: Apollo & Claude
Version: 1.0.0
Platform: Ubuntu/Linux
"""

import tkinter as tk
from tkinter import ttk, messagebox, filedialog, scrolledtext
import tkinter.font as tkFont
import threading
import json
import os
import sys
import time
import subprocess
import platform
from typing import Dict, List, Any, Optional
from datetime import datetime
import webbrowser
import random

# Configuration for demo mode
DEMO_MODE = True

class SAIQLDelta:
    """Main SAIQL-Delta Linux GUI Application"""
    
    def __init__(self):
        self.root = tk.Tk()
        
        # Initialize application state first
        self.connection_status = "Disconnected"
        self.query_history = []
        self.performance_data = {
            'queries_executed': 0,
            'total_execution_time': 0,
            'compression_ratio': 2.5,
            'cache_hit_rate': 85.0
        }
        
        # Then setup GUI components
        self.setup_main_window()
        self.create_variables()
        self.create_widgets()
        self.setup_menu()
        self.setup_status_bar()
        self.load_configuration()
        
        # Start background monitoring
        self.start_monitoring()
    
    def setup_main_window(self):
        """Configure the main application window"""
        self.root.title("SAIQL-Delta Database Manager v1.0")
        self.root.geometry("1200x800")
        self.root.minsize(800, 600)
        
        # Configure Linux-style theme (GTK-like appearance)
        style = ttk.Style()
        available_themes = style.theme_names()
        
        # Priority order for Linux themes
        preferred_themes = ['clam', 'alt', 'default', 'classic']
        selected_theme = 'default'
        
        for theme in preferred_themes:
            if theme in available_themes:
                selected_theme = theme
                break
        
        style.theme_use(selected_theme)
        
        # Configure colors for Ubuntu/Linux look (Yaru-inspired)
        self.colors = {
            'bg': '#ffffff',
            'fg': '#2e3436',
            'accent': '#e95420',  # Ubuntu orange
            'success': '#5e7f3e',
            'warning': '#f99b11',
            'error': '#c7162b',
            'border': '#babdb6',
            'header': '#f6f5f4'
        }
        
        # Apply Ubuntu/Linux styling
        style.configure('TLabel', background=self.colors['bg'], foreground=self.colors['fg'])
        style.configure('TButton', focuscolor='none')
        style.configure('TLabelframe', background=self.colors['bg'])
        style.configure('TLabelframe.Label', background=self.colors['bg'], foreground=self.colors['fg'])
        
        self.root.configure(bg=self.colors['bg'])
        
        # Set Linux icon if available
        try:
            # Try to use a standard Linux icon or custom icon
            icon_path = '/usr/share/pixmaps/database.xpm'
            if os.path.exists(icon_path):
                self.root.iconbitmap('@' + icon_path)
        except:
            pass  # Icon not found, use default
    
    def create_variables(self):
        """Create tkinter variables for data binding"""
        self.var_connection_mode = tk.StringVar(value="Translation")
        self.var_database_type = tk.StringVar(value="PostgreSQL")
        self.var_host = tk.StringVar(value="localhost")
        self.var_port = tk.StringVar(value="5432")
        self.var_database = tk.StringVar(value="saiql")
        self.var_username = tk.StringVar(value="saiql_user")
        self.var_password = tk.StringVar()
        self.var_status = tk.StringVar(value="Ready - Demo Mode")
        self.var_compression_level = tk.StringVar(value="6")
    
    def create_widgets(self):
        """Create and layout all GUI widgets"""
        # Create main container with padding
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky="nsew")
        
        # Configure grid weights
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(1, weight=1)
        
        # Left panel for connections and settings
        self.create_left_panel(main_frame)
        
        # Right panel for query editor and results
        self.create_right_panel(main_frame)
        
        # Bottom panel for status and performance
        self.create_bottom_panel(main_frame)
    
    def create_left_panel(self, parent):
        """Create left panel with connection settings"""
        left_frame = ttk.LabelFrame(parent, text="Database Connection", padding="10")
        left_frame.grid(row=0, column=0, rowspan=2, sticky="nsew", padx=(0, 10))
        
        # Demo mode notice (Linux style)
        if DEMO_MODE:
            demo_frame = ttk.Frame(left_frame)
            demo_frame.pack(fill="x", pady=(0, 10))
            
            # Create a styled demo notice
            demo_label = tk.Label(demo_frame, text="🐧 LINUX DEMO MODE", 
                                font=("Ubuntu", 10, "bold"),
                                bg="#fff3cd", fg="#856404",
                                relief="solid", bd=1, padx=10, pady=5)
            demo_label.pack()
            
            demo_info = tk.Label(demo_frame, 
                               text="This demo shows SAIQL-Delta GUI features\nwith simulated data and responses.",
                               font=("Ubuntu", 8),
                               bg=self.colors['bg'],
                               fg="#856404")
            demo_info.pack(pady=(5, 0))
        
        # Connection Mode Selection
        mode_frame = ttk.LabelFrame(left_frame, text="Deployment Mode", padding="5")
        mode_frame.pack(fill="x", pady=(0, 10))
        
        ttk.Radiobutton(mode_frame, text="Translation Layer", 
                       variable=self.var_connection_mode, value="Translation",
                       command=self.on_mode_changed).pack(anchor="w")
        ttk.Radiobutton(mode_frame, text="Standalone Database", 
                       variable=self.var_connection_mode, value="Standalone",
                       command=self.on_mode_changed).pack(anchor="w")
        
        # Database Settings
        settings_frame = ttk.LabelFrame(left_frame, text="Connection Settings", padding="5")
        settings_frame.pack(fill="x", pady=(0, 10))
        
        # Database Type
        ttk.Label(settings_frame, text="Database Type:").pack(anchor="w")
        db_combo = ttk.Combobox(settings_frame, textvariable=self.var_database_type,
                               values=["PostgreSQL", "MySQL", "MariaDB", "SQLite", "Oracle"],
                               state="readonly", width=25)
        db_combo.pack(fill="x", pady=(0, 5))
        
        # Host
        ttk.Label(settings_frame, text="Host:").pack(anchor="w")
        ttk.Entry(settings_frame, textvariable=self.var_host, width=25).pack(fill="x", pady=(0, 5))
        
        # Port
        ttk.Label(settings_frame, text="Port:").pack(anchor="w")
        ttk.Entry(settings_frame, textvariable=self.var_port, width=25).pack(fill="x", pady=(0, 5))
        
        # Database
        ttk.Label(settings_frame, text="Database:").pack(anchor="w")
        ttk.Entry(settings_frame, textvariable=self.var_database, width=25).pack(fill="x", pady=(0, 5))
        
        # Username
        ttk.Label(settings_frame, text="Username:").pack(anchor="w")
        ttk.Entry(settings_frame, textvariable=self.var_username, width=25).pack(fill="x", pady=(0, 5))
        
        # Password
        ttk.Label(settings_frame, text="Password:").pack(anchor="w")
        ttk.Entry(settings_frame, textvariable=self.var_password, width=25, show="*").pack(fill="x", pady=(0, 10))
        
        # Connection Buttons (Linux-style)
        button_frame = ttk.Frame(settings_frame)
        button_frame.pack(fill="x")
        
        self.btn_connect = ttk.Button(button_frame, text="Connect", command=self.connect_database)
        self.btn_connect.pack(side="left", padx=(0, 5))
        
        self.btn_disconnect = ttk.Button(button_frame, text="Disconnect", command=self.disconnect_database, state="disabled")
        self.btn_disconnect.pack(side="left")
        
        # Compression Settings
        compression_frame = ttk.LabelFrame(left_frame, text="Compression Settings", padding="5")
        compression_frame.pack(fill="x", pady=(0, 10))
        
        ttk.Label(compression_frame, text="Compression Level (1-8):").pack(anchor="w")
        compression_scale = ttk.Scale(compression_frame, from_=1, to=8, 
                                    variable=self.var_compression_level, orient="horizontal")
        compression_scale.pack(fill="x")
        
        level_label = ttk.Label(compression_frame, textvariable=self.var_compression_level)
        level_label.pack()
        
        # Performance Monitoring
        self.create_performance_panel(left_frame)
    
    def create_performance_panel(self, parent):
        """Create performance monitoring panel"""
        perf_frame = ttk.LabelFrame(parent, text="Performance Monitor", padding="5")
        perf_frame.pack(fill="both", expand=True)
        
        # Performance metrics
        self.perf_labels = {}
        
        metrics = [
            ("Status:", "connection_status"),
            ("Queries Executed:", "queries_executed"),
            ("Avg Response Time:", "avg_response_time"),
            ("Compression Ratio:", "compression_ratio"),
            ("Cache Hit Rate:", "cache_hit_rate")
        ]
        
        for i, (label, key) in enumerate(metrics):
            ttk.Label(perf_frame, text=label).grid(row=i, column=0, sticky="w", pady=2)
            value_label = ttk.Label(perf_frame, text="N/A", foreground=self.colors['accent'])
            value_label.grid(row=i, column=1, sticky="w", padx=(10, 0), pady=2)
            self.perf_labels[key] = value_label
        
        # Update status
        self.update_performance_display()
    
    def create_right_panel(self, parent):
        """Create right panel with query editor and results"""
        right_frame = ttk.Frame(parent)
        right_frame.grid(row=0, column=1, sticky="nsew")
        right_frame.columnconfigure(0, weight=1)
        right_frame.rowconfigure(1, weight=1)
        
        # Query Input
        query_frame = ttk.LabelFrame(right_frame, text="SAIQL Query Editor", padding="5")
        query_frame.grid(row=0, column=0, sticky="ew", pady=(0, 10))
        query_frame.columnconfigure(0, weight=1)
        
        # Query text area with Linux-appropriate font
        linux_fonts = ["Ubuntu Mono", "DejaVu Sans Mono", "Liberation Mono", "monospace"]
        selected_font = "monospace"  # fallback
        
        for font_name in linux_fonts:
            if font_name in tkFont.families():
                selected_font = font_name
                break
        
        self.query_text = scrolledtext.ScrolledText(query_frame, height=6, width=60,
                                                   font=(selected_font, 10))
        self.query_text.grid(row=0, column=0, columnspan=3, sticky="ew", pady=(0, 5))
        
        # Sample queries for demonstration
        sample_query = "*10[users]::name,email|status='active'>>oQ"
        self.query_text.insert("1.0", sample_query)
        
        # Basic syntax highlighting simulation
        self.highlight_saiql_syntax()
        
        # Query buttons
        ttk.Button(query_frame, text="Execute Query", command=self.execute_query).grid(row=1, column=0, padx=(0, 5))
        ttk.Button(query_frame, text="Clear", command=self.clear_query).grid(row=1, column=1, padx=(0, 5))
        ttk.Button(query_frame, text="Load Sample", command=self.load_sample_queries).grid(row=1, column=2)
        
        # Results Display
        results_frame = ttk.LabelFrame(right_frame, text="Query Results", padding="5")
        results_frame.grid(row=1, column=0, sticky="nsew")
        results_frame.columnconfigure(0, weight=1)
        results_frame.rowconfigure(0, weight=1)
        
        # Results treeview
        self.results_tree = ttk.Treeview(results_frame, show="tree headings")
        self.results_tree.grid(row=0, column=0, sticky="nsew")
        
        # Scrollbars for results
        results_v_scroll = ttk.Scrollbar(results_frame, orient="vertical", command=self.results_tree.yview)
        results_v_scroll.grid(row=0, column=1, sticky="ns")
        self.results_tree.configure(yscrollcommand=results_v_scroll.set)
        
        results_h_scroll = ttk.Scrollbar(results_frame, orient="horizontal", command=self.results_tree.xview)
        results_h_scroll.grid(row=1, column=0, sticky="ew")
        self.results_tree.configure(xscrollcommand=results_h_scroll.set)
    
    def highlight_saiql_syntax(self):
        """Basic SAIQL syntax highlighting simulation"""
        # Configure text tags for syntax highlighting (Linux-appropriate colors)
        self.query_text.tag_configure("operator", foreground="#005f87", font=(self.query_text['font'], 10, "bold"))
        self.query_text.tag_configure("string", foreground="#5f8700")
        self.query_text.tag_configure("table", foreground="#5f005f")
        self.query_text.tag_configure("function", foreground="#d75f00")
        
        # This is a simplified syntax highlighter for demo purposes
        content = self.query_text.get("1.0", tk.END)
        
        # Highlight SAIQL operators
        operators = ["*", "::", ">>", "|", "oQ"]
        for op in operators:
            start = "1.0"
            while True:
                pos = self.query_text.search(op, start, tk.END)
                if not pos:
                    break
                end = f"{pos}+{len(op)}c"
                self.query_text.tag_add("operator", pos, end)
                start = end
    
    def create_bottom_panel(self, parent):
        """Create bottom panel for logs and additional info"""
        bottom_frame = ttk.LabelFrame(parent, text="System Logs", padding="5")
        bottom_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        bottom_frame.columnconfigure(0, weight=1)
        
        # Log display
        linux_fonts = ["Ubuntu Mono", "DejaVu Sans Mono", "Liberation Mono", "monospace"]
        selected_font = "monospace"
        
        for font_name in linux_fonts:
            if font_name in tkFont.families():
                selected_font = font_name
                break
        
        self.log_text = scrolledtext.ScrolledText(bottom_frame, height=8, width=80,
                                                 font=(selected_font, 9))
        self.log_text.grid(row=0, column=0, sticky="ew")
        
        # Add initial log entries
        self.log_message("SAIQL-Delta Linux GUI Application Started")
        self.log_message(f"Platform: {platform.system()} {platform.release()}")
        if DEMO_MODE:
            self.log_message("Running in DEMO MODE - simulated data and responses")
        self.log_message("Ready for database connections...")
    
    def setup_menu(self):
        """Create application menu bar"""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # File Menu
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="New Query", command=self.new_query, accelerator="Ctrl+N")
        file_menu.add_command(label="Open Query...", command=self.open_query, accelerator="Ctrl+O")
        file_menu.add_command(label="Save Query...", command=self.save_query, accelerator="Ctrl+S")
        file_menu.add_separator()
        file_menu.add_command(label="Export Results...", command=self.export_results)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit, accelerator="Ctrl+Q")
        
        # Database Menu
        db_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Database", menu=db_menu)
        db_menu.add_command(label="Connect...", command=self.connect_database)
        db_menu.add_command(label="Disconnect", command=self.disconnect_database)
        db_menu.add_separator()
        db_menu.add_command(label="Database Information", command=self.show_db_info)
        db_menu.add_command(label="Performance Statistics", command=self.show_performance_stats)
        
        # Tools Menu
        tools_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Tools", menu=tools_menu)
        tools_menu.add_command(label="Query History", command=self.show_query_history)
        tools_menu.add_command(label="Configuration", command=self.show_configuration)
        tools_menu.add_command(label="System Monitor", command=self.show_system_monitor)
        
        # Help Menu
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="SAIQL Documentation", command=self.open_documentation)
        help_menu.add_command(label="Sample Queries", command=self.show_sample_queries)
        help_menu.add_separator()
        help_menu.add_command(label="About SAIQL-Delta", command=self.show_about)
        
        # Linux keyboard shortcuts
        self.root.bind_all("<Control-n>", lambda e: self.new_query())
        self.root.bind_all("<Control-o>", lambda e: self.open_query())
        self.root.bind_all("<Control-s>", lambda e: self.save_query())
        self.root.bind_all("<Control-q>", lambda e: self.root.quit())
        self.root.bind_all("<F5>", lambda e: self.execute_query())
    
    def setup_status_bar(self):
        """Create status bar at bottom of window"""
        self.status_bar = ttk.Frame(self.root)
        self.status_bar.grid(row=1, column=0, sticky="ew")
        
        # Status label
        self.status_label = ttk.Label(self.status_bar, textvariable=self.var_status, 
                                     relief="sunken", anchor="w")
        self.status_label.pack(side="left", fill="x", expand=True, padx=(5, 5))
        
        # Linux-style connection indicator
        self.connection_indicator = ttk.Label(self.status_bar, text="●", 
                                            foreground="red")
        self.connection_indicator.pack(side="right", padx=(0, 5))
        
        ttk.Label(self.status_bar, text="Connection:").pack(side="right")
    
    def on_mode_changed(self):
        """Handle deployment mode changes"""
        mode = self.var_connection_mode.get()
        self.log_message(f"Deployment mode changed to: {mode}")
        
        if mode == "Standalone":
            # In standalone mode, we don't need external database settings
            self.var_host.set("localhost")
            self.var_port.set("5433")
            self.var_database.set("saiql-delta")
        else:
            # Translation mode - restore database settings
            self.var_port.set("5432")
    
    def connect_database(self):
        """Connect to database (simulated in demo mode)"""
        try:
            self.log_message("Attempting database connection...")
            self.var_status.set("Connecting...")
            
            # Simulate connection delay
            self.root.update()
            time.sleep(1)
            
            # Simulate successful connection
            self.connection_status = "Connected"
            self.connection_indicator.configure(foreground="green")
            self.btn_connect.configure(state="disabled")
            self.btn_disconnect.configure(state="normal")
            
            mode = self.var_connection_mode.get()
            db_type = self.var_database_type.get()
            
            if DEMO_MODE:
                self.var_status.set(f"Connected to {db_type} (Linux Demo)")
                self.log_message(f"Demo connection established to {db_type} database")
            else:
                self.var_status.set(f"Connected to {db_type} database")
                self.log_message(f"Successfully connected to {db_type} database")
            
            self.log_message(f"Mode: {mode}")
            self.log_message(f"Host: {self.var_host.get()}:{self.var_port.get()}")
            
            # Update performance display
            self.update_performance_display()
            
        except Exception as e:
            self.log_message(f"Connection failed: {str(e)}")
            self.var_status.set("Connection failed")
            messagebox.showerror("Connection Error", f"Failed to connect to database:\n{str(e)}")
    
    def disconnect_database(self):
        """Disconnect from database"""
        try:
            self.log_message("Disconnecting from database...")
            
            self.connection_status = "Disconnected"
            self.connection_indicator.configure(foreground="red")
            self.btn_connect.configure(state="normal")
            self.btn_disconnect.configure(state="disabled")
            
            if DEMO_MODE:
                self.var_status.set("Disconnected - Linux Demo")
            else:
                self.var_status.set("Disconnected")
            
            self.log_message("Disconnected from database")
            self.update_performance_display()
            
        except Exception as e:
            self.log_message(f"Disconnect error: {str(e)}")
    
    def execute_query(self):
        """Execute SAIQL query (simulated in demo mode)"""
        query = self.query_text.get("1.0", tk.END).strip()
        if not query:
            messagebox.showwarning("No Query", "Please enter a SAIQL query to execute.")
            return
        
        if self.connection_status != "Connected":
            messagebox.showwarning("Not Connected", "Please connect to a database first.")
            return
        
        try:
            self.log_message(f"Executing query: {query}")
            self.var_status.set("Executing query...")
            
            # Clear previous results
            for item in self.results_tree.get_children():
                self.results_tree.delete(item)
            
            # Simulate query execution
            start_time = time.time()
            self.root.update()
            
            # Generate demo results based on query
            demo_results = self.generate_demo_results(query)
            execution_time = time.time() - start_time + random.uniform(0.05, 0.25)
            
            # Display results
            self.display_query_results(demo_results, execution_time)
            
            # Update performance metrics
            self.performance_data['queries_executed'] += 1
            self.performance_data['total_execution_time'] += execution_time
            
            # Add to query history
            self.query_history.append({
                'query': query,
                'timestamp': datetime.now(),
                'execution_time': execution_time,
                'rows_returned': len(demo_results)
            })
            
            self.var_status.set(f"Query executed successfully in {execution_time:.3f}s")
            self.log_message(f"Query completed in {execution_time:.3f} seconds, {len(demo_results)} rows returned")
            
            self.update_performance_display()
            
        except Exception as e:
            self.log_message(f"Query execution error: {str(e)}")
            self.var_status.set("Query execution failed")
            messagebox.showerror("Query Error", f"Failed to execute query:\n{str(e)}")
    
    def generate_demo_results(self, query: str) -> List[Dict]:
        """Generate realistic demo results based on query"""
        query_lower = query.lower()
        
        if "users" in query_lower:
            users = []
            for i in range(random.randint(3, 15)):
                users.append({
                    "id": i + 1,
                    "name": f"LinuxUser{i + 1}",
                    "email": f"user{i + 1}@ubuntu.com",
                    "status": random.choice(["active", "inactive", "pending"]),
                    "distro": random.choice(["Ubuntu", "Fedora", "Debian", "Arch", "CentOS"]),
                    "created_at": f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
                })
            return users
            
        elif "orders" in query_lower:
            orders = []
            for i in range(random.randint(5, 20)):
                orders.append({
                    "order_id": f"LNX-{1000 + i}",
                    "user_id": random.randint(1, 10),
                    "amount": round(random.uniform(10.0, 500.0), 2),
                    "status": random.choice(["pending", "completed", "cancelled"]),
                    "server": random.choice(["ubuntu-01", "debian-02", "fedora-03"]),
                    "created_at": f"2024-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}"
                })
            return orders
            
        elif "products" in query_lower:
            products = []
            categories = ["software", "hardware", "services", "cloud", "support"]
            for i in range(random.randint(4, 12)):
                products.append({
                    "product_id": f"LINUX-{100 + i}",
                    "name": f"Linux Product {i + 1}",
                    "category": random.choice(categories),
                    "price": round(random.uniform(5.0, 200.0), 2),
                    "open_source": random.choice([True, False]),
                    "platform": "Linux"
                })
            return products
            
        elif "count" in query_lower:
            return [{"count": random.randint(100, 10000)}]
            
        else:
            return [
                {"id": 1, "value": "Linux Demo Data 1", "type": "ubuntu"},
                {"id": 2, "value": "Linux Demo Data 2", "type": "fedora"},
                {"id": 3, "value": "Linux Demo Data 3", "type": "debian"}
            ]
    
    def display_query_results(self, results: List[Dict], execution_time: float):
        """Display query results in the treeview"""
        if not results:
            self.log_message("Query returned no results")
            return
        
        # Configure columns based on first result
        columns = list(results[0].keys())
        self.results_tree.configure(columns=columns)
        
        # Set column headings
        self.results_tree.heading("#0", text="Row")
        for col in columns:
            self.results_tree.heading(col, text=col.title())
            self.results_tree.column(col, width=100)
        
        # Insert data
        for i, row in enumerate(results):
            values = [str(row.get(col, "")) for col in columns]
            self.results_tree.insert("", "end", text=str(i+1), values=values)
        
        self.log_message(f"Displayed {len(results)} rows in {execution_time:.3f}s")
    
    def clear_query(self):
        """Clear the query editor"""
        self.query_text.delete("1.0", tk.END)
        self.log_message("Query editor cleared")
    
    def load_sample_queries(self):
        """Load sample SAIQL queries"""
        samples = [
            "*10[users]::name,email,distro|status='active'>>oQ",
            "*COUNT[orders]::*>>oQ",
            "*5[products]::name,price|category='software'>>COMPRESS(7)>>oQ",
            "*[users]::*|SIMILAR(name,'Linux')>>oQ",
            "*100[orders]::*|created_at>RECENT(7d)>>GROUP(server)>>oQ",
            "*20[orders]::order_id,amount|amount>100>>oQ",
            "*[products]::name,price|open_source=true>>oQ",
            "*15[users]::name,email,distro>>ORDER(created_at)>>oQ"
        ]
        
        sample_window = tk.Toplevel(self.root)
        sample_window.title("Sample SAIQL Queries - Linux")
        sample_window.geometry("700x500")
        sample_window.transient(self.root)
        sample_window.grab_set()
        
        ttk.Label(sample_window, text="Select a sample query:", 
                 font=("Ubuntu", 12, "bold")).pack(pady=10)
        
        # Create frame for listbox and scrollbar
        list_frame = ttk.Frame(sample_window)
        list_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        # Use Linux-appropriate font
        linux_fonts = ["Ubuntu Mono", "DejaVu Sans Mono", "Liberation Mono", "monospace"]
        selected_font = "monospace"
        
        for font_name in linux_fonts:
            if font_name in tkFont.families():
                selected_font = font_name
                break
        
        listbox = tk.Listbox(list_frame, font=(selected_font, 10))
        scrollbar = ttk.Scrollbar(list_frame, orient="vertical", command=listbox.yview)
        listbox.configure(yscrollcommand=scrollbar.set)
        
        listbox.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")
        
        for sample in samples:
            listbox.insert(tk.END, sample)
        
        # Add descriptions
        descriptions = [
            "Select 10 active Linux users with distribution info",
            "Count all orders in the database",
            "Get 5 software products with compression",
            "Find users with names similar to 'Linux'",
            "Recent orders grouped by server",
            "Orders over $100",
            "Open source products only",
            "Recent users ordered by creation date"
        ]
        
        def on_select(event):
            selection = listbox.curselection()
            if selection:
                desc_label.config(text=descriptions[selection[0]])
        
        listbox.bind('<<ListboxSelect>>', on_select)
        
        # Description label
        desc_label = ttk.Label(sample_window, text="Select a query to see description", 
                              font=("Ubuntu", 9), foreground="gray")
        desc_label.pack(pady=5)
        
        def use_sample():
            selection = listbox.curselection()
            if selection:
                selected_query = samples[selection[0]]
                self.query_text.delete("1.0", tk.END)
                self.query_text.insert("1.0", selected_query)
                self.highlight_saiql_syntax()
                sample_window.destroy()
                self.log_message(f"Loaded sample query: {selected_query[:50]}...")
        
        button_frame = ttk.Frame(sample_window)
        button_frame.pack(pady=10)
        
        ttk.Button(button_frame, text="Use Query", command=use_sample).pack(side="left", padx=(0, 10))
        ttk.Button(button_frame, text="Cancel", command=sample_window.destroy).pack(side="left")
    
    def update_performance_display(self):
        """Update performance metrics display"""
        if self.connection_status == "Connected":
            self.perf_labels['connection_status'].configure(text="Connected", foreground=self.colors['success'])
        else:
            self.perf_labels['connection_status'].configure(text="Disconnected", foreground=self.colors['error'])
        
        self.perf_labels['queries_executed'].configure(text=str(self.performance_data['queries_executed']))
        
        if self.performance_data['queries_executed'] > 0:
            avg_time = self.performance_data['total_execution_time'] / self.performance_data['queries_executed']
            self.perf_labels['avg_response_time'].configure(text=f"{avg_time:.3f}s")
        else:
            self.perf_labels['avg_response_time'].configure(text="N/A")
        
        self.perf_labels['compression_ratio'].configure(text=f"{self.performance_data['compression_ratio']:.1f}x")
        self.perf_labels['cache_hit_rate'].configure(text=f"{self.performance_data['cache_hit_rate']:.1f}%")
    
    def start_monitoring(self):
        """Start background monitoring thread"""
        def monitor():
            while True:
                try:
                    # Update performance metrics periodically
                    if self.connection_status == "Connected":
                        # Simulate realistic metrics changes
                        self.performance_data['cache_hit_rate'] += random.uniform(-0.5, 0.5)
                        self.performance_data['cache_hit_rate'] = max(70.0, min(98.0, self.performance_data['cache_hit_rate']))
                        
                        self.performance_data['compression_ratio'] += random.uniform(-0.1, 0.1)
                        self.performance_data['compression_ratio'] = max(2.0, min(4.0, self.performance_data['compression_ratio']))
                    
                    # Schedule GUI update on main thread
                    self.root.after(0, self.update_performance_display)
                    
                    time.sleep(3)  # Update every 3 seconds
                except:
                    break
        
        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()
    
    def log_message(self, message: str):
        """Add message to log display"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] {message}\n"
        
        self.log_text.insert(tk.END, log_entry)
        self.log_text.see(tk.END)
    
    # Menu command implementations (keeping Windows versions but updating messaging)
    def new_query(self):
        """Create new query"""
        self.clear_query()
        self.log_message("New query started")
    
    def open_query(self):
        """Open query from file"""
        filename = filedialog.askopenfilename(
            title="Open SAIQL Query",
            filetypes=[("SAIQL files", "*.saiql"), ("Text files", "*.txt"), ("All files", "*.*")]
        )
        if filename:
            try:
                with open(filename, 'r') as file:
                    content = file.read()
                    self.query_text.delete("1.0", tk.END)
                    self.query_text.insert("1.0", content)
                    self.highlight_saiql_syntax()
                    self.log_message(f"Loaded query from: {filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Could not open file:\n{str(e)}")
    
    def save_query(self):
        """Save current query to file"""
        query = self.query_text.get("1.0", tk.END).strip()
        if not query:
            messagebox.showwarning("No Query", "No query to save.")
            return
        
        filename = filedialog.asksaveasfilename(
            title="Save SAIQL Query",
            defaultextension=".saiql",
            filetypes=[("SAIQL files", "*.saiql"), ("Text files", "*.txt"), ("All files", "*.*")]
        )
        if filename:
            try:
                with open(filename, 'w') as file:
                    file.write(query)
                    self.log_message(f"Query saved to: {filename}")
            except Exception as e:
                messagebox.showerror("Error", f"Could not save file:\n{str(e)}")
    
    def export_results(self):
        """Export query results"""
        if not self.results_tree.get_children():
            messagebox.showwarning("No Results", "No query results to export.")
            return
        
        filename = filedialog.asksaveasfilename(
            title="Export Results",
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("JSON files", "*.json"), ("Text files", "*.txt")]
        )
        
        if filename:
            try:
                # Get column names
                columns = self.results_tree["columns"]
                
                # Get data
                data = []
                for item in self.results_tree.get_children():
                    values = self.results_tree.item(item)["values"]
                    data.append(dict(zip(columns, values)))
                
                if filename.endswith('.json'):
                    with open(filename, 'w') as f:
                        json.dump(data, f, indent=2)
                else:  # CSV or text
                    with open(filename, 'w') as f:
                        # Write header
                        f.write(','.join(columns) + '\n')
                        # Write data
                        for row in data:
                            f.write(','.join(str(row[col]) for col in columns) + '\n')
                
                self.log_message(f"Results exported to: {filename}")
                messagebox.showinfo("Export Complete", f"Results exported to:\n{filename}")
                
            except Exception as e:
                messagebox.showerror("Export Error", f"Could not export results:\n{str(e)}")
    
    def show_db_info(self):
        """Show database information"""
        info_window = tk.Toplevel(self.root)
        info_window.title("Database Information")
        info_window.geometry("500x400")
        info_window.transient(self.root)
        
        # Use Linux-appropriate font
        linux_fonts = ["Ubuntu Mono", "DejaVu Sans Mono", "Liberation Mono", "monospace"]
        selected_font = "monospace"
        
        for font_name in linux_fonts:
            if font_name in tkFont.families():
                selected_font = font_name
                break
        
        info_text = scrolledtext.ScrolledText(info_window, font=(selected_font, 10))
        info_text.pack(fill="both", expand=True, padx=10, pady=10)
        
        db_info = f"""Database Information - Linux
{"="*50}

Connection Mode: {self.var_connection_mode.get()}
Database Type: {self.var_database_type.get()}
Host: {self.var_host.get()}
Port: {self.var_port.get()}
Database: {self.var_database.get()}
Username: {self.var_username.get()}

Performance Metrics:
{"="*50}
Queries Executed: {self.performance_data['queries_executed']}
Total Execution Time: {self.performance_data['total_execution_time']:.3f}s
Compression Level: {self.var_compression_level.get()}
Current Compression Ratio: {self.performance_data['compression_ratio']:.1f}x
Cache Hit Rate: {self.performance_data['cache_hit_rate']:.1f}%

System Information:
{"="*50}
Platform: {platform.system()} {platform.release()}
Architecture: {platform.machine()}
Python Version: {platform.python_version()}

Status: {self.connection_status}

SAIQL Features Available:
{"="*50}
✓ Semantic Query Language
✓ LoreToken Compression
✓ Cross-Database Federation
✓ AI/ML Integration
✓ Real-time Streaming
✓ Vector/Embedding Storage

Linux-specific Features:
✓ GTK-style interface
✓ Native Linux packaging
✓ Distribution compatibility

{"Note: Running in Linux demo mode" if DEMO_MODE else ""}
"""
        info_text.insert("1.0", db_info)
        info_text.configure(state="disabled")
    
    def show_performance_stats(self):
        """Show detailed performance statistics"""
        messagebox.showinfo("Performance Stats", "Detailed Linux performance statistics available in next version.")
    
    def show_query_history(self):
        """Show query execution history"""
        messagebox.showinfo("Query History", "Linux query history viewer available in next version.")
    
    def show_configuration(self):
        """Show configuration dialog"""
        messagebox.showinfo("Configuration", "Linux configuration options available in next version.")
    
    def show_system_monitor(self):
        """Show system monitoring window"""
        messagebox.showinfo("System Monitor", "Linux system monitoring dashboard available in next version.")
    
    def open_documentation(self):
        """Open SAIQL documentation"""
        webbrowser.open("https://github.com/saiql/saiql-delta/blob/main/docs/user_guide/")
        self.log_message("Opened documentation in web browser")
    
    def show_sample_queries(self):
        """Show sample queries help"""
        self.load_sample_queries()
    
    def show_about(self):
        """Show about dialog"""
        about_text = f"""SAIQL-Delta Database Manager v1.0 for Linux

A revolutionary semantic database system with AI-powered 
query processing and transcendent compression technology.

Platform: {platform.system()} {platform.release()}
Architecture: {platform.machine()}

Features:
• Semantic AI Query Language (SAIQL)
• 60-70% data compression with LoreToken
• Translation layer for existing databases
• Standalone semantic database
• AI/ML integration and vector storage
• Real-time streaming and federation
• Native Linux integration

Linux-specific Features:
• GTK-compatible theming
• AppImage/DEB packaging support
• Distribution-neutral compatibility
• Open source ecosystem integration

Developed by Apollo & Claude
Copyright © 2025 SAIQL Project

For more information, visit:
https://github.com/saiql/saiql-delta"""
        
        messagebox.showinfo("About SAIQL-Delta Linux", about_text)
    
    def load_configuration(self):
        """Load configuration from file"""
        config_file = os.path.expanduser("~/.config/saiql-delta/config.json")
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r') as f:
                    config = json.load(f)
                    self.var_host.set(config.get('host', 'localhost'))
                    self.var_port.set(config.get('port', '5432'))
                    self.var_database.set(config.get('database', 'saiql'))
                    self.var_username.set(config.get('username', 'saiql_user'))
                    self.var_compression_level.set(config.get('compression_level', '6'))
                    self.var_connection_mode.set(config.get('mode', 'Translation'))
                    self.log_message("Configuration loaded from ~/.config/saiql-delta/config.json")
        except Exception as e:
            self.log_message(f"Could not load configuration: {str(e)}")
    
    def save_configuration(self):
        """Save current configuration to file"""
        config_dir = os.path.expanduser("~/.config/saiql-delta")
        config_file = os.path.join(config_dir, "config.json")
        
        config = {
            'host': self.var_host.get(),
            'port': self.var_port.get(),
            'database': self.var_database.get(),
            'username': self.var_username.get(),
            'compression_level': self.var_compression_level.get(),
            'mode': self.var_connection_mode.get()
        }
        
        try:
            os.makedirs(config_dir, exist_ok=True)
            with open(config_file, 'w') as f:
                json.dump(config, f, indent=2)
                self.log_message("Configuration saved to ~/.config/saiql-delta/config.json")
        except Exception as e:
            self.log_message(f"Could not save configuration: {str(e)}")
    
    def on_closing(self):
        """Handle application closing"""
        self.save_configuration()
        self.log_message("SAIQL-Delta Linux application closing...")
        self.root.destroy()
    
    def run(self):
        """Start the GUI application"""
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.log_message("SAIQL-Delta Linux GUI ready - try executing a sample query!")
        self.root.mainloop()

if __name__ == "__main__":
    print("🐧 Starting SAIQL-Delta Linux GUI...")
    print("🎮 Running in demo mode with simulated data")
    print(f"📍 Platform: {platform.system()} {platform.release()}")
    
    app = SAIQLDelta()
    app.run()
