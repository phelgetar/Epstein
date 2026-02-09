#!/bin/bash

################################################################################
# Epstein DOJ Files - Search Interface Launcher
# Starts the secure web server with auto-reload
################################################################################

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Navigate to project root (parent of scripts/)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Epstein DOJ Files - Search Interface${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if search.html exists
if [ ! -f "static/search.html" ]; then
    echo -e "${RED}Error: static/search.html not found${NC}"
    echo "Make sure you're running this script from the project directory"
    exit 1
fi

# Check if JSON file exists
if [ ! -f "data/epstein_pdfs_search_index.json" ] && [ ! -f "epstein_pdfs_search_index.json" ]; then
    echo -e "${YELLOW}Warning: search index JSON not found${NC}"
    echo ""
    echo "You need to create the search index first:"
    echo "  1. Run: python3 -m src.extractor"
    echo "  2. Wait for it to process all PDFs"
    echo "  3. Then run this script again"
    echo ""
    read -p "Continue anyway? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo -e "${GREEN}Starting secure web server with auto-reload...${NC}"
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop the server${NC}"
echo ""

python3 -m src.server
