#!/bin/bash
echo "======================================"
echo "MIGRATION STATUS CHECK"
echo "======================================"
echo ""

# Check if process is running
if ps aux | grep -q "[p]ython3 retry_failed_tables.py"; then
    echo "✅ Process Status: RUNNING"
    pid=$(ps aux | grep "[p]ython3 retry_failed_tables.py" | awk '{print $2}')
    echo "   PID: $pid"
else
    echo "❌ Process Status: NOT RUNNING"
fi

echo ""

# Count successes
success_count=$(grep -c "Successfully loaded data to StarRocks table" retry_output2.log 2>/dev/null || echo "0")
total=89
echo "📊 Progress: $success_count / $total tables loaded"

# Calculate percentage
percent=$((success_count * 100 / total))
echo "   $percent% complete"

echo ""

# Show last few loaded tables
echo "Recent Successes:"
grep "Successfully loaded data to StarRocks table" retry_output2.log | tail -n 5 | sed 's/.*WorldZone_NEW\./  - /'

echo ""

# Check current activity
echo "Current Activity:"
tail -n 3 retry_output2.log | grep -v "^\[Stage" | tail -n 2

echo ""
echo "======================================"
