@echo off
REM ============================================================================
REM Epstein DOJ Files - Search Interface Launcher (Windows)
REM Starts the secure web server with auto-reload
REM ============================================================================

REM Navigate to project root (parent of scripts\)
cd /d "%~dp0\.."

echo ========================================
echo Epstein DOJ Files - Search Interface
echo ========================================
echo.

REM Check if search.html exists
if not exist "static\search.html" (
    echo Error: static\search.html not found
    echo Make sure you're running this script from the project directory
    pause
    exit /b 1
)

REM Check if JSON file exists
if not exist "data\epstein_pdfs_search_index.json" (
    if not exist "epstein_pdfs_search_index.json" (
        echo Warning: search index JSON not found
        echo.
        echo You need to create the search index first:
        echo   1. Run: python -m src.extractor
        echo   2. Wait for it to process all PDFs
        echo   3. Then run this script again
        echo.
        pause
    )
)

echo Starting secure web server with auto-reload...
echo.
echo Press Ctrl+C to stop the server
echo.

python -m src.server

pause
