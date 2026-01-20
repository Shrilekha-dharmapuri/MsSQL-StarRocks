#!/bin/bash
while true; do
    clear
    echo "==================================="
    echo "RETRY MIGRATION PROGRESS"
    echo "==================================="
    echo ""
    
    # Count SUCCESS and FAILED
    success_count=$(grep -c "✅ SUCCESS" retry_output2.log 2>/dev/null || echo "0")
    failed_count=$(grep -c "❌ FAILED" retry_output2.log 2>/dev/null || echo "0")
    
    echo "✅ Success: $success_count"
    echo "❌ Failed: $failed_count"
    echo ""
    
    # Show last 15 lines excluding Spark logs
    echo "Recent activity:"
    echo "-----------------------------------"
    tail -n 80 retry_output2.log | grep -v "INFO:app.services.spark_pipeline\|^\[Stage" | tail -n 15
    
    echo ""
    echo "==================================="
    echo "Press Ctrl+C to stop monitoring"
    echo "==================================="
    
    sleep 10
done
