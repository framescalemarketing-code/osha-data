from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.cli import main


if __name__ == "__main__":
    raise SystemExit(main(["ingest-public-signals", *sys.argv[1:]]))
