#!/bin/bash
# Automated fresh clone build test
set -e

echo "========================================"
echo "  Fresh Clone Build Test"
echo "========================================"

# Setup
TEST_DIR="/tmp/saiql-test-$(date +%s)"
REPO_PATH="/home/nova/SAIQL.DEV"

echo ""
echo "1. Cloning to $TEST_DIR..."
git clone "$REPO_PATH" "$TEST_DIR"
cd "$TEST_DIR"

echo "2. Creating venv..."
python3 -m venv .venv
source .venv/bin/activate

echo "3. Installing dependencies..."
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt

echo "4. Testing core imports..."
python3 -c "from core.lexer import SAIQLLexer; print('   ✓ Lexer OK')" || exit 1
python3 -c "from core.parser import SAIQLParser; print('   ✓ Parser OK')" || exit 1
python3 -c "from core.engine import SAIQLEngine; print('   ✓ Engine OK')" || exit 1

echo "5. Checking repository size..."
git_size=$(du -sm .git | cut -f1)
echo "   Git directory: ${git_size}MB"
if [ "$git_size" -gt 50 ]; then
    echo "   ⚠️  WARNING: Repository is large (>${git_size}MB)"
else
    echo "   ✓ Repository size is healthy"
fi

echo ""
echo "========================================"
echo "✅ Fresh Clone Build Test PASSED!"
echo "========================================"
echo ""
echo "Test completed in: $TEST_DIR"
echo "Cleanup with: rm -rf $TEST_DIR"

# Optionally auto-cleanup
read -p "Delete test directory? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    cd /tmp
    rm -rf "$TEST_DIR"
    echo "✓ Cleaned up"
fi
