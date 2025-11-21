from __future__ import annotations

import os
import pathlib
import re
import tower
from dotenv import load_dotenv
from pathlib import Path


def _get_env_value(name: str) -> str | None:
    """Get environment variable value, returning None if not set or empty."""
    value = os.getenv(name)
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def main() -> None:
    """
    Main entry point for the dbt workflow.
    
    This script handles:
    - Loading .env file for local development (controlled by TOWER_PARAMETER_USE_ENV)
    - Resolving profiles.yml template with environment variables
    - Using Tower's built-in dbt API for running dbt workflows
    - Supporting both Tower runs and direct execution
    """
    # Get project root directory
    repo_root = Path(__file__).resolve().parent
    
    # Check if Tower parameter allows using .env file
    # When running via Tower: TOWER_PARAMETER_USE_ENV controls .env loading
    # When running directly (not via Tower): default to using .env for local development
    tower_param = os.getenv('TOWER_PARAMETER_USE_ENV')
    if tower_param is not None:
        # Running via Tower - respect the parameter
        use_env_file = tower_param.lower() == 'true'
    else:
        # Running directly (not via Tower) - default to using .env for local dev
        use_env_file = True
    
    env_file = repo_root / '.env'
    
    if use_env_file and env_file.exists():
        print(f"Loading environment variables from .env file: {env_file}")
        # Load .env file - override=True means .env values take precedence over existing env vars
        # This ensures Tower's .env file is used when TOWER_PARAMETER_USE_ENV=true
        load_dotenv(env_file, override=True)
    elif use_env_file and not env_file.exists():
        print("use_env_file is true but .env file not found, using environment variables only")
    else:
        print("use_env_file is false, using environment variables only (not loading .env file)")
    
    # Verify required variables are set
    required_vars = ['TAXI_PG_PASSWORD', 'TAXI_PG_USER']
    for var in required_vars:
        value = os.getenv(var)
        if value:
            if 'PASSWORD' in var:
                print(f"Found {var} (value hidden for security)")
            else:
                print(f"Found {var} - {value}")
        else:
            print(f"Warning: {var} is not set in .env or environment variables")
    
    # Load profiles.yml template and resolve environment variables
    profiles_yml_path = repo_root / 'profiles.yml'
    
    if profiles_yml_path.exists():
        with open(profiles_yml_path, 'r') as f:
            profiles_yml_content = f.read()
        
        # Resolve ${VAR} placeholders with actual environment variable values
        def resolve_env_vars(match):
            var_name = match.group(1)
            value = os.getenv(var_name, match.group(0))  # Return env var or original if not found
            
            # If we got a value (not the original placeholder), ensure it's quoted as a string
            # This prevents YAML from interpreting numbers as integers
            if value != match.group(0) and value:
                # Check if the value needs quoting (if it's a number or contains special chars)
                try:
                    float(value)  # If it can be parsed as a number, quote it
                    return f'"{value}"'
                except ValueError:
                    # Not a number, but check if already quoted
                    if not (value.startswith('"') and value.endswith('"')):
                        return f'"{value}"'
            return value
        
        # Replace ${VAR} with actual values
        resolved_profiles_yml = re.sub(r'\$\{([^}]+)\}', resolve_env_vars, profiles_yml_content)
        
        print("Resolved environment variables in profiles.yml")
        
        # Tower's load_profile_from_env() expects DBT_PROFILE_YAML with full profiles.yml content
        # Set it as an environment variable so Tower can load it
        os.environ['DBT_PROFILE_YAML'] = resolved_profiles_yml
    else:
        print("Warning: profiles.yml not found")
        # Tower's load_profile_from_env() will try to load from DBT_PROFILE_YAML env var
    
    # Path to the dbt project directory (contains dbt_project.yml)
    # Override with DBT_PROJECT_PATH env var or Towerfile parameter
    project_path = Path(_get_env_value("DBT_PROJECT_PATH") or str(repo_root))
    
    # Parse dbt configuration from environment variables
    # These can be overridden via Towerfile parameters
    
    # DBT_COMMANDS (optional): Comma-separated list of dbt commands to run (e.g., "seed,run,test")
    # Default to "run" if not specified
    commands_str = _get_env_value("DBT_COMMANDS") or "run"
    commands = tower.dbt.parse_command_plan(commands_str)
    
    # DBT_SELECT (optional): dbt selector for filtering models/tests (e.g., "tag:daily" or "model_name+")
    selector = _get_env_value("DBT_SELECT")
    
    # DBT_TARGET (optional): Target profile to use from profiles.yml (e.g., "dev", "prod")
    target = _get_env_value("DBT_TARGET") or "dev"
    
    # DBT_THREADS (optional): Number of threads for parallel execution
    threads_str = _get_env_value("DBT_THREADS")
    threads = int(threads_str) if threads_str else None
    
    # DBT_VARS_JSON (optional): JSON string of variables to pass to dbt (e.g., '{"key": "value"}')
    vars_json = _get_env_value("DBT_VARS_JSON")
    vars_payload = None
    if vars_json:
        try:
            import json
            vars_payload = json.loads(vars_json)
        except json.JSONDecodeError:
            # Fallback: pass raw YAML/JSON string through to dbt
            vars_payload = vars_json
    
    # DBT_FULL_REFRESH (optional): Whether to do a full refresh of incremental models (true/false)
    full_refresh_str = _get_env_value("DBT_FULL_REFRESH") or "false"
    full_refresh = full_refresh_str.lower() in {"1", "true", "yes"}
    
    print(f"Running dbt with:")
    print(f"  Project path: {project_path}")
    print(f"  Commands: {commands}")
    print(f"  Target: {target}")
    if selector:
        print(f"  Selector: {selector}")
    if threads:
        print(f"  Threads: {threads}")
    
    # Create and run the dbt workflow using Tower's built-in dbt API
    # tower.dbt.load_profile_from_env() will load from DBT_PROFILE_YAML environment variable
    profile_payload = tower.dbt.load_profile_from_env()
    
    workflow = tower.dbt(
        project_path=project_path,
        profile_payload=profile_payload,
        commands=commands,
        selector=selector,
        target=target,
        threads=threads,
        vars_payload=vars_payload,
        full_refresh=full_refresh,
    )
    
    workflow.run()


if __name__ == "__main__":
    main()
