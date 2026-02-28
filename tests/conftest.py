# tests/conftest.py
import sys
from pathlib import Path

# Garante que 'api' e importavel sem pip install -e .
sys.path.insert(0, str(Path(__file__).parent.parent))
