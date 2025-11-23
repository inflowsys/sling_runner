"""
Job script that runs Sling replication first, then dbt transformations.
Based on Tower's fan-out pattern: https://github.com/tower/tower-examples/blob/main/08-fan-out-ticker-runs/task.py
"""
import tower
import sys


def main():
    """
    Main job workflow:
    1. Run Sling replication (sling_easy app)
    2. Wait for it to complete successfully
    3. If successful, run dbt transformations (dbt_easy app)
    """
    
    print("=" * 60)
    print("Starting ELT Pipeline Job")
    print("=" * 60)
    
    ###
    # Step 1: Run Sling replication (Extract & Load)
    ###
    print("\n[Step 1] Running Sling replication (sling_easy)...")
    
    sling_run = tower.run_app("sling_easy")
    (successful_runs, unsuccessful_runs) = tower.wait_for_runs([sling_run])
    
    if len(unsuccessful_runs) > 0:
        print(f"\n[ERROR] Sling replication failed!")
        print(f"   Unsuccessful runs: {len(unsuccessful_runs)}")
        for run in unsuccessful_runs:
            print(f"   - Run ID: {run.run_id}, Status: {run.status}")
        sys.exit(1)
    
    print(f"[SUCCESS] Sling replication completed successfully!")
    for run in successful_runs:
        print(f"   - Run ID: {run.run_id}, Status: {run.status}")
    
    ###
    # Step 2: Run dbt transformations (Transform)
    ###
    print("\n[Step 2] Running dbt transformations (dbt_easy)...")
    
    dbt_run = tower.run_app("dbt_easy")
    (successful_runs, unsuccessful_runs) = tower.wait_for_runs([dbt_run])
    
    if len(unsuccessful_runs) > 0:
        print(f"\n[ERROR] dbt transformations failed!")
        print(f"   Unsuccessful runs: {len(unsuccessful_runs)}")
        for run in unsuccessful_runs:
            print(f"   - Run ID: {run.run_id}, Status: {run.status}")
        sys.exit(1)
    
    print(f"[SUCCESS] dbt transformations completed successfully!")
    for run in successful_runs:
        print(f"   - Run ID: {run.run_id}, Status: {run.status}")
    
    print("\n" + "=" * 60)
    print("[SUCCESS] ELT Pipeline completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
