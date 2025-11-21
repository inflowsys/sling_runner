"""
Job script that runs Sling replication first, then dbt transformations.
Based on Tower's fan-out pattern: https://github.com/tower/tower-examples/blob/main/08-fan-out-ticker-runs/task.py
"""
import tower
import os
import sys


def main():
    """
    Main job workflow:
    1. Run Sling replication (sling_easy app)
    2. Wait for it to complete successfully
    3. If successful, run dbt transformations (dbt_easy app)
    """
    
    # Get optional parameters
    use_env = os.getenv("TOWER_PARAMETER_USE_ENV", "false")
    dbt_commands = os.getenv("DBT_COMMANDS", "run")
    dbt_select = os.getenv("DBT_SELECT", "")
    dbt_target = os.getenv("DBT_TARGET", "dev")
    
    print("=" * 60)
    print("Starting ELT Pipeline Job")
    print("=" * 60)
    
    ###
    # Step 1: Run Sling replication (Extract & Load)
    ###
    print("\n[Step 1] Running Sling replication (sling_easy)...")
    
    sling_params = {
        "TOWER_PARAMETER_USE_ENV": use_env
    }
    
    sling_run = tower.run_app("sling_easy", parameters=sling_params)
    
    # Wait for Sling to complete
    (successful_runs, unsuccessful_runs) = tower.wait_for_runs([sling_run])
    
    if len(unsuccessful_runs) > 0:
        print(f"\n❌ Sling replication failed!")
        print(f"   Unsuccessful runs: {len(unsuccessful_runs)}")
        for run in unsuccessful_runs:
            print(f"   - Run ID: {run.run_id}, Status: {run.status}")
        sys.exit(1)
    
    print(f"✅ Sling replication completed successfully!")
    for run in successful_runs:
        print(f"   - Run ID: {run.run_id}, Status: {run.status}")
    
    ###
    # Step 2: Run dbt transformations (Transform)
    ###
    print("\n[Step 2] Running dbt transformations (dbt_easy)...")
    
    dbt_params = {
        "TOWER_PARAMETER_USE_ENV": use_env,
        "DBT_COMMANDS": dbt_commands,
        "DBT_TARGET": dbt_target,
    }
    
    # Only add DBT_SELECT if it's not empty
    if dbt_select:
        dbt_params["DBT_SELECT"] = dbt_select
    
    dbt_run = tower.run_app("dbt_easy", parameters=dbt_params)
    
    # Wait for dbt to complete
    (successful_runs, unsuccessful_runs) = tower.wait_for_runs([dbt_run])
    
    if len(unsuccessful_runs) > 0:
        print(f"\n❌ dbt transformations failed!")
        print(f"   Unsuccessful runs: {len(unsuccessful_runs)}")
        for run in unsuccessful_runs:
            print(f"   - Run ID: {run.run_id}, Status: {run.status}")
        sys.exit(1)
    
    print(f"✅ dbt transformations completed successfully!")
    for run in successful_runs:
        print(f"   - Run ID: {run.run_id}, Status: {run.status}")
    
    print("\n" + "=" * 60)
    print("✅ ELT Pipeline completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()

