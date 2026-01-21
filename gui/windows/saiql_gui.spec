# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller Specification File for SAIQL-Charlie Windows GUI
============================================================

This spec file defines how PyInstaller should build the SAIQL-Charlie
Windows executable. It includes all necessary modules, data files,
and configuration for creating a standalone .exe file.

Usage:
    pyinstaller saiql_gui.spec

Author: Apollo & Claude
Version: 1.0.0
"""

import os
import sys
from pathlib import Path

# Get paths
current_dir = Path.cwd()
project_root = current_dir.parent.parent

block_cipher = None

# Define data files to include
datas = [
    (str(project_root / 'core'), 'core'),
    (str(project_root / 'extensions'), 'extensions'),
    (str(project_root / 'config'), 'config'),
    (str(project_root / 'data' / 'legend_map.lore'), 'data'),
    (str(project_root / 'requirements.txt'), '.'),
]

# Hidden imports - modules that PyInstaller might miss
hiddenimports = [
    'tkinter',
    'tkinter.ttk',
    'tkinter.filedialog',
    'tkinter.messagebox',
    'tkinter.scrolledtext',
    'psycopg2',
    'psycopg2.pool',
    'psycopg2.extras',
    'mysql.connector',
    'sqlite3',
    'json',
    'threading',
    'webbrowser',
    'subprocess',
    'time',
    'datetime',
    'pathlib',
    'typing',
    'dataclasses',
    'enum',
    'abc',
    'collections',
    'urllib.parse',
    'hashlib',
    'logging',
]

# Analysis of the main script
a = Analysis(
    ['saiql_gui.py'],
    pathex=[str(current_dir), str(project_root)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'matplotlib',
        'numpy',
        'scipy',
        'pandas',
        'jupyter',
        'notebook',
        'IPython',
        'pytest',
        'sphinx',
        'setuptools',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

# Remove duplicate entries
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# Create executable
exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='SAIQL-Charlie',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,  # Compress with UPX if available
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,  # Hide console window
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # Windows-specific options
    version='version_info.txt',  # Version information file
    icon='assets/saiql_icon.ico',  # Application icon
)