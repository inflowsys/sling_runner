import yaml
from sling import Replication
import os
import pathlib
from dotenv import load_dotenv

# Get project root directory
repo_root = pathlib.Path(__file__).resolve().parent

# If SLING_HOME_DIR is not set, default to project root
if not os.getenv("SLING_HOME_DIR"):
    os.environ["SLING_HOME_DIR"] = str(repo_root)
    print(f"SLING_HOME_DIR set to {os.environ['SLING_HOME_DIR']}")

# Load environment variables from .env file if it exists
# Priority: .env file first, then environment variables as fallback
# This allows Tower to use .env file, while GitHub Actions uses environment variables
env_file = repo_root / '.env'

if env_file.exists():
    print(f"Loading environment variables from .env file: {env_file}")
    # Load .env file - override=True means .env values take precedence over existing env vars
    # This ensures Tower's .env file is used when present
    load_dotenv(env_file, override=True)
else:
    print(".env file not found, using environment variables only")

# Ensure required variables are set (from .env or environment)
# These will be used by env.yaml via ${VAR} syntax
required_vars = ['TAXI_PG_PASSWORD', 'SLING_CLI_TOKEN']
for var in required_vars:
    value = os.getenv(var)
    if value:
        print(f"Found {var} (value hidden for security)")
    else:
        print(f"Warning: {var} is not set in .env or environment variables")

#From a YAML file
replication = Replication(file_path='sling_easy.yaml')
replication.run()

# # # Or load into object
# with open('sling_easy.yaml') as file:
#   config = yaml.load(file, Loader=yaml.FullLoader)

# replication = Replication(**config)

# replication.run()