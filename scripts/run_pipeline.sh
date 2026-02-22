set -e # Exit immediately if any command fails

# Go to project root (one level up from scripts/)
cd "$(dirname "$0")/.."

VENV_DIR=".venv"

echo "Checking virtual environment..."
if [ ! -d "$VENV_DIR" ]; then
  echo "Creating venv at $VENV_DIR..."
  python3 -m venv "$VENV_DIR"
fi

PY="$VENV_DIR/bin/python"
PIP="$VENV_DIR/bin/pip"

echo "Upgrading pip..."
"$PY" -m pip install --upgrade pip

echo "Installing Python dependencies..."
"$PIP" install -r requirements.txt

echo "Running pipeline..."
"$PY" scripts/run_pipeline.py

