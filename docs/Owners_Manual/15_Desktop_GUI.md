# 🖥️ Desktop GUI (SAIQL-Charlie)

**Goal**: Install and use the optional graphical user interface for Linux.

SAIQL includes a native Linux GUI called **SAIQL-Charlie**. It provides a visual query builder, performance monitoring, and connection management.

---

## 1. Features
- **Visual Query Builder**: Construct SAIQL queries without typing.
- **Real-time Monitoring**: View server performance graphs.
- **Dual Mode**: Works in both Standalone and Translation Layer modes.
- **Export**: Save results to CSV/JSON.

## 2. Location
The source code and build scripts are located in:
`/home/nova/SAIQL.DEV/gui/linux/`

## 3. Building the GUI
The GUI is a standalone Python/Tkinter application that must be built for your specific Linux distribution.

### Quick Build
1.  Navigate to the directory:
    ```bash
    cd /home/nova/SAIQL.DEV/gui/linux/
    ```
2.  Run the build script:
    ```bash
    chmod +x quick_build.sh
    ./quick_build.sh
    ```
3.  Locate the executable:
    -   **Executable**: `dist/SAIQL-Charlie`
    -   **Tarball**: `SAIQL-Charlie-Linux-ubuntu-v1.0.tar.gz`

## 4. Running
```bash
./dist/SAIQL-Charlie
```

> **Note**: This requires a desktop environment (GNOME, KDE, XFCE). It will not run on a headless server.

## 5. Troubleshooting
-   **Missing Tkinter**: Install `python3-tk` (`sudo apt install python3-tk`).
-   **PyInstaller Errors**: Ensure `pyinstaller` is in your PATH.

---

### Next Step
- **[README.md](./README.md)**: Return to the main index.
