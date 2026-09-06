"""spark-submit spark/jobs/normalize.py context.json (project root on PYTHONPATH)."""
import json
import sys
from pathlib import Path

from pipeline.tasks import normalize

if __name__ == '__main__':
    normalize(json.loads(Path(sys.argv[1]).read_text(encoding='utf-8')))
