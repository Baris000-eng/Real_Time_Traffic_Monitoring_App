import http.client
import time
import sys

def wait_for_cluster_ready(host="flink-jobmanager", port=8081, max_retries=60, retry_interval=2):
    """
    Wait for Flink cluster to be fully ready (not just REST API responding).
    Checks that at least one TaskExecutor is registered and healthy.
    """
    retries = 0
    while retries < max_retries:
        try:
            conn = http.client.HTTPConnection(host, port, timeout=2)
            conn.request("GET", "/overview")
            response = conn.getresponse()
            data = response.read().decode()
            
            # Parse JSON response to check for registered task managers
            import json
            overview = json.loads(data)
            
            # Check if at least 1 task manager is registered
            if overview.get("taskmanagers", 0) > 0:
                print(f"✓ Flink cluster ready with {overview['taskmanagers']} TaskManager(s)")
                return True
            else:
                print(f"  Waiting for TaskManagers to register... (attempt {retries + 1}/{max_retries})")
        except Exception as e:
            print(f"  Cluster not ready: {e}")
        
        time.sleep(retry_interval)
        retries += 1
    
    print("✗ Flink cluster failed to become ready")
    return False

if __name__ == "__main__":
    success = wait_for_cluster_ready()
    sys.exit(0 if success else 1)
