import subprocess
import sys

# Run Sling CLI command directly
try:
    result = subprocess.run(
        ['sling', 'run', '-r', 'sling_easy.yaml'],
        check=True,
        capture_output=False  # Set to True if you want to capture output
    )
    sys.exit(0)
except subprocess.CalledProcessError as e:
    print(f"Sling command failed with exit code {e.returncode}", file=sys.stderr)
    sys.exit(e.returncode)