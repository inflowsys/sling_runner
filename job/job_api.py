"""
Job script that runs Sling replication first, then dbt transformations.
Uses Tower REST API directly: https://docs.tower.dev/docs/reference/api/run-app
"""
import os
import sys
import time
import httpx
from typing import Optional, Dict, Any


# Tower API configuration
TOWER_API_BASE_URL = os.getenv("TOWER_API_BASE_URL", "https://api.tower.dev")
TOWER_API_KEY = os.getenv("TOWER_API_KEY")
TOWER_SESSION_TOKEN = os.getenv("TOWER_SESSION_TOKEN")


def get_auth_headers() -> Dict[str, str]:
    """Get authentication headers for Tower API requests."""
    headers = {"Content-Type": "application/json"}
    
    if TOWER_API_KEY:
        headers["Authorization"] = f"Bearer {TOWER_API_KEY}"
    elif TOWER_SESSION_TOKEN:
        headers["Authorization"] = f"Bearer {TOWER_SESSION_TOKEN}"
    else:
        # Try to get session token from Tower SDK's default location
        # Tower SDK typically stores session in ~/.tower/session.json
        session_path = os.path.expanduser("~/.tower/session.json")
        if os.path.exists(session_path):
            import json
            try:
                with open(session_path, 'r') as f:
                    session_data = json.load(f)
                    if 'token' in session_data:
                        headers["Authorization"] = f"Bearer {session_data['token']}"
            except Exception:
                pass
    
    if "Authorization" not in headers:
        raise ValueError(
            "No Tower API authentication found. Set TOWER_API_KEY or TOWER_SESSION_TOKEN "
            "environment variable, or ensure Tower SDK session exists."
        )
    
    return headers


def run_app(app_name: str, parameters: Optional[Dict[str, str]] = None) -> str:
    """
    Run a Tower app using the REST API.
    
    Args:
        app_name: Name of the app to run
        parameters: Optional dictionary of parameters to pass to the app
    
    Returns:
        Run ID of the started run
    """
    url = f"{TOWER_API_BASE_URL}/apps/{app_name}/runs"
    headers = get_auth_headers()
    
    payload = {}
    if parameters:
        payload["parameters"] = parameters
    
    print(f"Calling Tower API: POST {url}")
    if parameters:
        print(f"  Parameters: {list(parameters.keys())}")
    
    with httpx.Client() as client:
        response = client.post(url, json=payload, headers=headers, timeout=30.0)
        response.raise_for_status()
        result = response.json()
        run_id = result.get("run_id") or result.get("id")
        
        if not run_id:
            raise ValueError(f"Unexpected API response format: {result}")
        
        print(f"  Started run: {run_id}")
        return run_id


def get_run_status(run_id: str) -> Dict[str, Any]:
    """
    Get the status of a Tower run.
    
    Args:
        run_id: The run ID to check
    
    Returns:
        Dictionary containing run status information
    """
    url = f"{TOWER_API_BASE_URL}/runs/{run_id}"
    headers = get_auth_headers()
    
    with httpx.Client() as client:
        response = client.get(url, headers=headers, timeout=30.0)
        response.raise_for_status()
        return response.json()


def wait_for_run(run_id: str, poll_interval: int = 2, timeout: Optional[int] = None) -> Dict[str, Any]:
    """
    Wait for a Tower run to complete.
    
    Args:
        run_id: The run ID to wait for
        poll_interval: Seconds between status checks
        timeout: Maximum seconds to wait (None for no timeout)
    
    Returns:
        Final run status dictionary
    """
    start_time = time.time()
    
    while True:
        status_data = get_run_status(run_id)
        status = status_data.get("status", "unknown")
        
        print(f"  Run {run_id}: {status}")
        
        # Check if run is complete (success or failure)
        if status in ["succeeded", "failed", "cancelled", "error"]:
            return status_data
        
        # Check timeout
        if timeout and (time.time() - start_time) > timeout:
            raise TimeoutError(f"Run {run_id} did not complete within {timeout} seconds")
        
        time.sleep(poll_interval)


def main():
    """
    Main job workflow:
    1. Run Sling replication (sling_easy app)
    2. Wait for it to complete successfully
    3. If successful, run dbt transformations (dbt_easy app)
    """
    
    print("=" * 60)
    print("Starting ELT Pipeline Job (using Tower REST API)")
    print("=" * 60)
    
    # Get optional parameters from environment
    use_env = os.getenv("TOWER_PARAMETER_USE_ENV", "false")
    dbt_commands = os.getenv("DBT_COMMANDS", "run")
    dbt_target = os.getenv("DBT_TARGET", "dev")
    dbt_select = os.getenv("DBT_SELECT", "")
    
    ###
    # Step 1: Run Sling replication (Extract & Load)
    ###
    print("\n[Step 1] Running Sling replication (sling_easy)...")
    
    sling_params = {
        "TOWER_PARAMETER_USE_ENV": use_env
    }
    
    try:
        sling_run_id = run_app("sling_easy", parameters=sling_params)
        sling_status = wait_for_run(sling_run_id)
        
        final_status = sling_status.get("status", "unknown")
        if final_status != "succeeded":
            print(f"\n[ERROR] Sling replication failed!")
            print(f"   Run ID: {sling_run_id}, Status: {final_status}")
            if "error" in sling_status:
                print(f"   Error: {sling_status['error']}")
            sys.exit(1)
        
        print(f"[SUCCESS] Sling replication completed successfully!")
        print(f"   Run ID: {sling_run_id}, Status: {final_status}")
    
    except Exception as e:
        print(f"\n[ERROR] Failed to run Sling replication: {e}")
        sys.exit(1)
    
    ###
    # Step 2: Run dbt transformations (Transform)
    ###
    print("\n[Step 2] Running dbt transformations (dbt_easy)...")
    
    dbt_params = {
        "TOWER_PARAMETER_USE_ENV": use_env,
        "DBT_COMMANDS": dbt_commands,
        "DBT_TARGET": dbt_target,
    }
    
    if dbt_select:
        dbt_params["DBT_SELECT"] = dbt_select
    
    try:
        dbt_run_id = run_app("dbt_easy", parameters=dbt_params)
        dbt_status = wait_for_run(dbt_run_id)
        
        final_status = dbt_status.get("status", "unknown")
        if final_status != "succeeded":
            print(f"\n[ERROR] dbt transformations failed!")
            print(f"   Run ID: {dbt_run_id}, Status: {final_status}")
            if "error" in dbt_status:
                print(f"   Error: {dbt_status['error']}")
            sys.exit(1)
        
        print(f"[SUCCESS] dbt transformations completed successfully!")
        print(f"   Run ID: {dbt_run_id}, Status: {final_status}")
    
    except Exception as e:
        print(f"\n[ERROR] Failed to run dbt transformations: {e}")
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("[SUCCESS] ELT Pipeline completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
