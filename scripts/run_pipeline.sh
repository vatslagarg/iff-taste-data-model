set -e  # Exit immediately if any command fails

# Navigate to project root (one level up from scripts/)
cd "$(dirname "$0")/.."

echo "Installing Python dependencies..."
pip install -r requirements.txt


echo "Running pipeline..."
python3 scripts/run_pipeline.py
