import yaml
from sling import Replication
import os
import pathlib
import re
from dotenv import load_dotenv

# Get project root directory
repo_root = pathlib.Path(__file__).resolve().parent

# Note: We don't set SLING_HOME_DIR, so Sling will use its default location
# (typically user's home directory), not the repository root

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

# Ensure required variables are set (from .env or environment)
# These will be used to resolve ${VAR} syntax in env.yaml
required_vars_secret = ['TAXI_PG_PASSWORD', 'SLING_CLI_TOKEN']
for var in required_vars_secret:
    value = os.getenv(var)
    if value:
        print(f"Found {var} (value hidden for security)")
    else:
        print(f"Warning: {var} is not set in .env or environment variables")

required_vars = ['TAXI_PG_USER', 'INSTANCE']
for var in required_vars:
    value = os.getenv(var)
    if value:
        print(f"Found {var} - {value}")
    else:
        print(f"Warning: {var} is not set in .env or environment variables")

# Load env.yaml template and resolve environment variables
# Then set SLING_ENV_YAML so Sling uses it instead of looking for a file
env_yaml_path = repo_root / 'env.yaml'
if env_yaml_path.exists():
    with open(env_yaml_path, 'r') as f:
        env_yaml_content = f.read()
    
    # Resolve ${VAR} placeholders with actual environment variable values
    def resolve_env_vars(match):
        var_name = match.group(1)
        return os.getenv(var_name, match.group(0))  # Return env var or original if not found
    
    # Replace ${VAR} with actual values
    resolved_env_yaml = re.sub(r'\$\{([^}]+)\}', resolve_env_vars, env_yaml_content)
    
    # Set SLING_ENV_YAML environment variable so Sling uses it
    os.environ['SLING_ENV_YAML'] = resolved_env_yaml
    print("Set SLING_ENV_YAML from env.yaml template with resolved variables")
else:
    print("Warning: env.yaml not found, Sling will use default connection discovery")

#From a YAML file
replication = Replication(file_path='sling_easy.yaml')
replication.run()

# # # Or load into object
# with open('sling_easy.yaml') as file:
#   config = yaml.load(file, Loader=yaml.FullLoader)

# replication = Replication(**config)

# replication.run()