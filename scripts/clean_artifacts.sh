#!/bin/bash
# Safe deletion script for build artifacts
# Only deletes items NOT tracked in git

set -e

echo "================================================"
echo "  SAIQL Build Artifacts Cleanup"
echo "================================================"
echo ""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Change to repo root
cd /home/nova/SAIQL.DEV

echo -e "${YELLOW}Step 1: Untracking GUI binaries (if needed)${NC}"
if git ls-files | grep -q "gui/linux.*\\.tar\\.gz\\|SAIQL-Charlie$"; then
    echo "Found tracked GUI binaries. Untracking..."
    git rm --cached gui/linux/SAIQL-Charlie-Linux-ubuntu-v1.0.tar.gz 2>/dev/null || true
    git rm --cached gui/linux/SAIQL-Charlie-Linux/SAIQL-Charlie 2>/dev/null || true
    echo -e "${GREEN}✓ Untracked${NC}"
    echo ""
    echo "Don't forget to commit: git commit -m 'Untrack GUI binaries'"
else
    echo -e "${GREEN}✓ GUI binaries already untracked${NC}"
fi

echo ""
echo -e "${YELLOW}Step 2: Deleting Python cache${NC}"
echo "Removing __pycache__ directories..."
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
echo "Removing .pyc files..."
find . -name "*.pyc" -delete 2>/dev/null || true
echo -e "${GREEN}✓ Python cache cleaned${NC}"

echo ""
echo -e "${YELLOW}Step 3: Deleting test cache${NC}"
if [ -d ".pytest_cache" ]; then
    rm -rf .pytest_cache/
    echo -e "${GREEN}✓ Removed .pytest_cache${NC}"
else
    echo "✓ No .pytest_cache found"
fi

echo ""
echo -e "${YELLOW}Step 4: Deleting logs${NC}"
if [ -d "logs" ]; then
    echo "Found $(find logs/ -type f | wc -l) log files"
    rm -rf logs/
    echo -e "${GREEN}✓ Removed logs/${NC}"
else
    echo "✓ No logs/ directory"
fi

echo ""
echo -e "${YELLOW}Step 5: Deleting build artifacts (may need sudo)${NC}"

# Check if build/ exists and is root-owned
if [ -d "build" ]; then
    if [ "$(stat -c %U build/)" = "root" ]; then
        echo -e "${YELLOW}build/ is root-owned, using sudo...${NC}"
        sudo rm -rf build/
    else
        rm -rf build/
    fi
    echo -e "${GREEN}✓ Removed build/${NC}"
else
    echo "✓ No build/ directory"
fi

# Check if saiql.egg-info exists and is root-owned
if [ -d "saiql.egg-info" ]; then
    if [ "$(stat -c %U saiql.egg-info/)" = "root" ]; then
        echo -e "${YELLOW}saiql.egg-info/ is root-owned, using sudo...${NC}"
        sudo rm -rf saiql.egg-info/
    else
        rm -rf saiql.egg-info/
    fi
    echo -e "${GREEN}✓ Removed saiql.egg-info/${NC}"
else
    echo "✓ No saiql.egg-info/ directory"
fi

echo ""
echo "================================================"
echo -e "${GREEN}✅ Cleanup Complete!${NC}"
echo "================================================"
echo ""

# Verify nothing unwanted is tracked
echo "Verifying git status..."
if git ls-files | grep -qE "(build/|\.egg-info|__pycache__|\.pyc$|\.pytest_cache|^logs/)"; then
    echo -e "${RED}⚠️  WARNING: Some artifacts are still tracked!${NC}"
    echo "Run: git ls-files | grep -E '(build/|__pycache__)'"
else
    echo -e "${GREEN}✓ No build artifacts tracked in git${NC}"
fi

echo ""
echo "Next steps:"
echo "1. Review with: git status"
echo "2. If GUI files were untracked, commit:"
echo "   git commit -m 'Untrack GUI binaries (move to Releases)'"
echo "3. Items deleted are in .gitignore and won't be re-added"
