import argparse
import json

from pipeline.config import TRANSFORM_VERSION, source_path


def main():
    parser = argparse.ArgumentParser(description='GitBugs snapshot pipeline')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('init-db')
    for name in ('profile', 'run'):
        sub = commands.add_parser(name)
        sub.add_argument('--source', default=str(source_path()))
        if name == 'run':
            sub.add_argument('--version', default=TRANSFORM_VERSION)
            sub.add_argument('--failpoint', choices=['after_staging_chunk', 'before_commit'])
        else:
            sub.add_argument('--output')
    for name in ('verify', 'recover'):
        commands.add_parser(name).add_argument('batch_id')
    args = parser.parse_args()
    if args.command == 'profile':
        from pipeline.quality import reference_profile
        from pipeline.source import write_json
        result = reference_profile(args.source)
        if args.output:
            write_json(args.output, result)
    elif args.command == 'run':
        from pipeline.tasks import run
        result = run(args.source, args.version, args.failpoint)
    else:
        from pipeline.database import init_schema, recover_batch, verify_batch
        if args.command == 'init-db':
            init_schema()
            result = {'schema': 'ready'}
        elif args.command == 'recover':
            result = {'recovered': recover_batch(args.batch_id)}
        else:
            result = verify_batch(args.batch_id)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


if __name__ == '__main__':
    main()
