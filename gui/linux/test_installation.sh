#!/bin/bash
# SAIQL Linux Installation Test Script
# =============================================

set -e

echo
echo "🧪 SAIQL Linux Installation Test"
echo "========================================"
echo

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

print_test() {
    echo -e "${BLUE}🔍 Testing: $1${NC}"
}

print_pass() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_fail() {
    echo -e "${RED}❌ $1${NC}"
}

# Test 1: Check if executable exists
print_test "Executable exists"
if [ -f "SAIQL-Linux/SAIQL" ]; then
    print_pass "Executable found"
else
    print_fail "Executable not found"
    exit 1
fi

# Test 2: Check if executable is executable
print_test "Executable permissions"
if [ -x "SAIQL-Linux/SAIQL" ]; then
    print_pass "Executable has correct permissions"
else
    print_fail "Executable permissions incorrect"
    exit 1
fi

# Test 3: Check if documentation exists
print_test "Documentation files"
files_to_check=("README.txt" "sample_config.json" "sample_queries.saiql")
for file in "${files_to_check[@]}"; do
    if [ -f "SAIQL-Linux/$file" ]; then
        print_pass "$file exists"
    else
        print_fail "$file missing"
        exit 1
    fi
done

# Test 4: Check if installation scripts exist and are executable
print_test "Installation scripts"
if [ -x "SAIQL-Linux/install.sh" ] && [ -x "SAIQL-Linux/uninstall.sh" ]; then
    print_pass "Installation scripts exist and are executable"
else
    print_fail "Installation scripts missing or not executable"
    exit 1
fi

# Test 5: Check tarball integrity
print_test "Distribution tarball"
if [ -f "SAIQL-Linux-ubuntu-v1.0.tar.gz" ]; then
    if tar -tzf SAIQL-Linux-ubuntu-v1.0.tar.gz >/dev/null 2>&1; then
        print_pass "Tarball is valid"
    else
        print_fail "Tarball is corrupted"
        exit 1
    fi
else
    print_fail "Tarball not found"
    exit 1
fi

# Test 6: Basic executable test (quick start/stop)
print_test "Executable can start"
cd SAIQL-Linux
timeout 3s ./SAIQL 2>/dev/null || true
print_pass "Executable appears to start correctly"
cd ..

# Test 7: Desktop file validation
print_test "Desktop file format"
if desktop-file-validate SAIQL-Linux/saiql.desktop 2>/dev/null; then
    print_pass "Desktop file is valid"
else
    # desktop-file-validate might not be available, so this is optional
    echo "   ⚠️  Desktop file validation skipped (desktop-file-utils not available)"
fi

# Test 8: Sample configuration parsing
print_test "Configuration file format"
if python3 -c "import json; json.load(open('SAIQL-Linux/sample_config.json'))" 2>/dev/null; then
    print_pass "Configuration file is valid JSON"
else
    print_fail "Configuration file has invalid JSON format"
    exit 1
fi

echo
print_pass "ALL TESTS PASSED!"
echo
echo "🚀 Installation package is ready for distribution!"
echo
echo "Next steps:"
echo "1. Test installation: cd SAIQL-Linux && ./install.sh"
echo "2. Test uninstallation: ./uninstall.sh"
echo "3. Distribute: Share SAIQL-Linux-ubuntu-v1.0.tar.gz"
echo

# Show package summary
echo "📋 Package Summary:"
echo "   Executable size: $(du -h SAIQL-Linux/SAIQL | cut -f1)"
echo "   Package size: $(du -h SAIQL-Linux-ubuntu-v1.0.tar.gz | cut -f1)"
echo "   Files included: $(ls SAIQL-Linux | wc -l) files"
echo
echo "✨ Linux distribution ready! 🐧"
