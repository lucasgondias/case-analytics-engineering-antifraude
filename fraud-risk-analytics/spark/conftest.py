"""Coloca spark/ no sys.path para `import fraud_risk_spark` nos testes."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
