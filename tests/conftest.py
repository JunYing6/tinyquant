from pathlib import Path
import sys


SRC = Path(__file__).resolve().parents[1] / "src"
src_string = str(SRC)
if src_string in sys.path:
    sys.path.remove(src_string)
sys.path.insert(0, src_string)
