"""
Test Script for Dimension Resolution Fix
Tests that the correct dimension is used for report generation
"""
import requests
import time
import json

BASE_URL = "http://localhost:8000"

def test_dimension_resolution():
    """Test that each dimension generates the correct report type"""
    
    print("\n" + "="*70)
    print("DIMENSION RESOLUTION TEST")
    print("="*70)
    
    # Test data for each dimension
    test_cases = [
        {
            "name": "1D Test",
            "payload": {
                "dimension": "1D",
                "employee_name": "Test User 1D",
                "employee_id": "E001"
            },
            "expected_dimension": "1D"
        },
        {
            "name": "2D Test (lowercase)",
            "payload": {
                "dimension": "2d",
                "employee_name": "Test User 2D",
                "employee_id": "E002"
            },
            "expected_dimension": "2D"
        },
        {
            "name": "3D Test (uppercase)",
            "payload": {
                "dimension": "3D",
                "employee_name": "Test User 3D",
                "employee_id": "E003"
            },
            "expected_dimension": "3D"
        },
        {
            "name": "4D Test (mixed case)",
            "payload": {
                "dimension": "4d",
                "employee_name": "Test User 4D",
                "employee_id": "E004"
            },
            "expected_dimension": "4D"
        }
    ]
    
    results = []
    
    for test_case in test_cases:
        print(f"\n{'─'*70}")
        print(f"Running: {test_case['name']}")
        print(f"Requested dimension: {test_case['payload']['dimension']}")
        print(f"Expected dimension: {test_case['expected_dimension']}")
        
        try:
            # Submit job
            print("  📤 Submitting job...")
            response = requests.post(f"{BASE_URL}/submit", json=test_case["payload"])
            response.raise_for_status()
            
            data = response.json()
            job_id = data["job_id"]
            returned_dimension = data.get("dimension")
            
            print(f"  ✅ Job submitted: {job_id}")
            print(f"  📊 Server confirmed dimension: {returned_dimension}")
            
            # Check if dimension was normalized correctly
            if returned_dimension != test_case["expected_dimension"]:
                print(f"  ⚠️  WARNING: Dimension mismatch at submission!")
                print(f"     Expected: {test_case['expected_dimension']}")
                print(f"     Got: {returned_dimension}")
            
            # Poll for completion
            print("  ⏳ Waiting for report generation...")
            max_wait = 60  # seconds
            start_time = time.time()
            
            while time.time() - start_time < max_wait:
                status_response = requests.get(f"{BASE_URL}/status/{job_id}")
                status_data = status_response.json()
                
                if status_data["status"] == "completed":
                    # Get result
                    result_response = requests.get(f"{BASE_URL}/result/{job_id}")
                    result_data = result_response.json()
                    
                    actual_dimension = result_data.get("dimension")
                    report = result_data.get("report", "")
                    
                    print(f"  ✅ Report generated")
                    print(f"  📊 Result dimension: {actual_dimension}")
                    print(f"  📝 Report length: {len(report)} characters")
                    
                    # Verify dimension
                    if actual_dimension == test_case["expected_dimension"]:
                        print(f"  ✅ SUCCESS: Correct dimension used!")
                        result = "PASS"
                    else:
                        print(f"  ❌ FAIL: Wrong dimension!")
                        print(f"     Expected: {test_case['expected_dimension']}")
                        print(f"     Got: {actual_dimension}")
                        result = "FAIL"
                    
                    # Check report content for dimension markers
                    report_lower = report.lower()
                    dim_mentions = {
                        "1D": "1d" in report_lower or "individual" in report_lower,
                        "2D": "2d" in report_lower or "relationship" in report_lower,
                        "3D": "3d" in report_lower or "team" in report_lower,
                        "4D": "4d" in report_lower or "organization" in report_lower
                    }
                    
                    if dim_mentions.get(test_case["expected_dimension"]):
                        print(f"  ✅ Report content matches expected dimension")
                    else:
                        print(f"  ⚠️  Report content may not match expected dimension")
                    
                    results.append({
                        "test": test_case["name"],
                        "requested": test_case["payload"]["dimension"],
                        "expected": test_case["expected_dimension"],
                        "actual": actual_dimension,
                        "result": result
                    })
                    
                    break
                    
                elif status_data["status"] == "failed":
                    error = status_data.get("error", "Unknown error")
                    print(f"  ❌ Report generation failed: {error}")
                    results.append({
                        "test": test_case["name"],
                        "requested": test_case["payload"]["dimension"],
                        "expected": test_case["expected_dimension"],
                        "actual": "ERROR",
                        "result": "ERROR",
                        "error": error
                    })
                    break
                
                time.sleep(2)
            
            else:
                print(f"  ⏱️  Timeout waiting for report")
                results.append({
                    "test": test_case["name"],
                    "requested": test_case["payload"]["dimension"],
                    "expected": test_case["expected_dimension"],
                    "actual": "TIMEOUT",
                    "result": "TIMEOUT"
                })
        
        except requests.exceptions.ConnectionError:
            print(f"  ❌ Could not connect to server")
            print(f"     Make sure the server is running: python api_server_improved.py")
            return
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            results.append({
                "test": test_case["name"],
                "requested": test_case["payload"]["dimension"],
                "expected": test_case["expected_dimension"],
                "actual": "ERROR",
                "result": "ERROR",
                "error": str(e)
            })
    
    # Summary
    print(f"\n{'='*70}")
    print("TEST SUMMARY")
    print("="*70)
    
    passed = sum(1 for r in results if r["result"] == "PASS")
    failed = sum(1 for r in results if r["result"] == "FAIL")
    errors = sum(1 for r in results if r["result"] == "ERROR")
    timeouts = sum(1 for r in results if r["result"] == "TIMEOUT")
    
    for result in results:
        status_icon = {
            "PASS": "✅",
            "FAIL": "❌",
            "ERROR": "💥",
            "TIMEOUT": "⏱️"
        }.get(result["result"], "❓")
        
        print(f"{status_icon} {result['test']}")
        print(f"   Requested: {result['requested']} → Expected: {result['expected']} → Actual: {result['actual']}")
        if "error" in result:
            print(f"   Error: {result['error']}")
    
    print(f"\n{'─'*70}")
    print(f"Total Tests: {len(results)}")
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"💥 Errors: {errors}")
    print(f"⏱️  Timeouts: {timeouts}")
    print("="*70)
    
    if passed == len(results):
        print("\n🎉 ALL TESTS PASSED! Dimension resolution is working correctly.")
    else:
        print("\n⚠️  SOME TESTS FAILED! Please review the results above.")


def test_concurrent_dimensions():
    """Test that concurrent requests with different dimensions work correctly"""
    
    print("\n" + "="*70)
    print("CONCURRENT DIMENSION TEST")
    print("="*70)
    print("Testing that different dimensions can be processed simultaneously...")
    
    payloads = [
        {"dimension": "1D", "employee_name": "Concurrent 1D", "employee_id": "C1"},
        {"dimension": "2D", "employee_name": "Concurrent 2D", "employee_id": "C2"},
        {"dimension": "3D", "employee_name": "Concurrent 3D", "employee_id": "C3"},
        {"dimension": "4D", "employee_name": "Concurrent 4D", "employee_id": "C4"},
    ]
    
    # Submit all jobs
    job_ids = []
    for payload in payloads:
        try:
            response = requests.post(f"{BASE_URL}/submit", json=payload)
            data = response.json()
            job_ids.append({
                "job_id": data["job_id"],
                "expected_dimension": payload["dimension"].upper()
            })
            print(f"  ✅ Submitted {payload['dimension']} job: {data['job_id']}")
        except Exception as e:
            print(f"  ❌ Failed to submit {payload['dimension']}: {e}")
    
    print(f"\n  ⏳ Waiting for all {len(job_ids)} jobs to complete...")
    
    # Check all jobs
    results = []
    for job_info in job_ids:
        job_id = job_info["job_id"]
        expected = job_info["expected_dimension"]
        
        # Poll for completion
        max_wait = 60
        start = time.time()
        
        while time.time() - start < max_wait:
            status_response = requests.get(f"{BASE_URL}/status/{job_id}")
            status_data = status_response.json()
            
            if status_data["status"] == "completed":
                result_response = requests.get(f"{BASE_URL}/result/{job_id}")
                result_data = result_response.json()
                actual = result_data.get("dimension")
                
                match = "✅" if actual == expected else "❌"
                print(f"  {match} Job {job_id}: Expected {expected}, Got {actual}")
                
                results.append(actual == expected)
                break
            elif status_data["status"] == "failed":
                print(f"  ❌ Job {job_id} failed")
                results.append(False)
                break
            
            time.sleep(2)
        else:
            print(f"  ⏱️  Job {job_id} timeout")
            results.append(False)
    
    if all(results):
        print("\n✅ CONCURRENT TEST PASSED: All dimensions processed correctly")
    else:
        print("\n❌ CONCURRENT TEST FAILED: Some dimensions were incorrect")


if __name__ == "__main__":
    print("""
╔════════════════════════════════════════════════════════════════════╗
║                  DIMENSION RESOLUTION TEST SUITE                   ║
║                                                                    ║
║  This test verifies that the dimension resolution bug is fixed.   ║
║  Each test will request a specific dimension and verify that       ║
║  the correct report type is generated.                             ║
╚════════════════════════════════════════════════════════════════════╝
    """)
    
    # Check if server is running
    try:
        response = requests.get(f"{BASE_URL}/health")
        print("✅ Server is running\n")
    except requests.exceptions.ConnectionError:
        print("❌ Server is not running!")
        print("   Please start the server first:")
        print("   python api_server_improved.py")
        exit(1)
    
    # Run tests
    test_dimension_resolution()
    
    print("\n" + "─"*70 + "\n")
    
    test_concurrent_dimensions()
    
    print("\n" + "="*70)
    print("TESTING COMPLETE")
    print("="*70)
