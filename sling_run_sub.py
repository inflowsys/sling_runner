import subprocess
import sys

# Run the same command that works in GitHub Actions
try:
    result = subprocess.run(
        ['uv', 'run', 'python', 'sling_run.py'],
        check=True,
        capture_output=False  # Set to True if you want to capture output
    )
    sys.exit(0)
except subprocess.CalledProcessError as e:
    print(f"Command failed with exit code {e.returncode}", file=sys.stderr)
    sys.exit(e.returncode)
except FileNotFoundError as e:
    print(f"Command not found: {e}. Make sure 'uv' is installed and in PATH.", file=sys.stderr)
    sys.exit(1)