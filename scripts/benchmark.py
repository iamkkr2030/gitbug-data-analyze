"""Generate explicitly synthetic CSVs and record local Spark scaling evidence."""
import argparse
import csv
import json
import platform
import time
from pathlib import Path

from pipeline.source import inspect_source, write_json
from pipeline.spark_transform import create_spark, transform

parser = argparse.ArgumentParser()
parser.add_argument('--rows', nargs='+', type=int, default=[1000, 10000, 100000])
parser.add_argument('--output', default='data/benchmark')
args = parser.parse_args()
if any(n <= 0 for n in args.rows):
    parser.error('Row counts must be positive')
root = Path(args.output).resolve()
root.mkdir(parents=True, exist_ok=True)
started = time.perf_counter()
spark = create_spark()
startup = time.perf_counter() - started
results = []
try:
    for n in args.rows:
        source = root / f'synthetic-{n}.csv'
        with source.open('w', newline='', encoding='utf-8') as stream:
            writer = csv.writer(stream)
            writer.writerow(['Issue id', 'Duplicate id'])
            writer.writerows((i, f'{i+1}, {i+1}') for i in range(1, n+1))
        context = {**inspect_source(source), 'batch_id': f'synthetic-{n}',
                   'transform_version': 'benchmark-v1', 'raw_path': str(source),
                   'work_dir': str(root / f'result-{n}')}
        report = transform(context, spark)
        report['rows_per_second'] = n / report['transform_seconds']
        results.append(report)
finally:
    spark.stop()
write_json(root / 'benchmark.json', {'synthetic': True, 'platform': platform.platform(),
                                   'spark_startup_seconds': startup, 'results': results})
print(json.dumps(results, indent=2))
