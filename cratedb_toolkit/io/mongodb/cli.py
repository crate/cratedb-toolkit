import argparse
import json
import sys
import typing as t
from pathlib import Path

from rich.console import Console

from cratedb_toolkit import __version__
from cratedb_toolkit.io.mongodb.core import export, extract, translate
from cratedb_toolkit.util.common import setup_logging

console = Console(stderr=True)
rich = console


def extract_parser(subargs):
    parser = subargs.add_parser("extract", help="Extract a schema from a MongoDB database")
    parser.add_argument("--url", default="mongodb://localhost:27017", help="MongoDB URL")
    parser.add_argument("--database", required=True, help="MongoDB database")
    parser.add_argument("--collection", help="MongoDB collection to create a schema for")
    parser.add_argument(
        "--scan",
        choices=["full", "partial"],
        help="Whether to fully scan the MongoDB collections or only partially.",
    )
    parser.add_argument("--limit", type=int, default=0, required=False, help="Limit export to N documents")
    parser.add_argument("--transformation", type=Path, required=False, help="Zyp transformation file")
    parser.add_argument("-o", "--out", required=False)


def translate_parser(subargs):
    parser = subargs.add_parser(
        "translate",
        help="Translate a MongoDB schema definition to a CrateDB table schema",
    )
    parser.add_argument("-i", "--infile", help="The JSON file to read the MongoDB schema from")


def export_parser(subargs):
    parser = subargs.add_parser("export", help="Export a MongoDB collection as plain JSON")
    parser.add_argument("--url", default="mongodb://localhost:27017", help="MongoDB URL")
    parser.add_argument("--database", required=True, help="MongoDB database")
    parser.add_argument("--collection", required=True, help="MongoDB collection to export")
    parser.add_argument("--limit", type=int, default=0, required=False, help="Limit export to N documents")
    parser.add_argument("--transformation", type=Path, required=False, help="Zyp transformation file")


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        help="print package version of pyproject_fmt",
        version=f"%(prog)s ({__version__})",
    )
    subparsers = parser.add_subparsers(dest="command")
    extract_parser(subparsers)
    translate_parser(subparsers)
    export_parser(subparsers)
    return parser.parse_args()


def extract_to_file(args):
    """
    Extract a schema or set of schemas from MongoDB collections into a JSON file.
    """

    schema = extract(args)

    out_label = args.out or "stdout"
    rich.print(f"Writing resulting schema to {out_label}")
    fp: t.TextIO
    if args.out:
        fp = open(args.out, "w")
    else:
        fp = sys.stdout
    json.dump(schema, fp=fp, indent=4)
    fp.flush()


def translate_from_file(args):
    """
    Read in a JSON file and extract the schema from it.
    """
    fp: t.TextIO
    if args.infile:
        fp = open(args.infile, "r")
    else:
        fp = sys.stdin
    schema = json.load(fp)
    translate(schema)


def export_to_stdout(args):
    sys.stdout.buffer.write(export(args).read())


def main():
    setup_logging()
    args = get_args()
    headline_prefix = "[green bold]MongoDB[/green bold] -> [blue bold]CrateDB[/blue bold] Exporter"
    if args.command == "extract":
        rich.print(f"{headline_prefix} -> Schema Extractor")
        extract_to_file(args)
    elif args.command == "translate":
        rich.print(f"{headline_prefix} -> Schema Translator")
        translate_from_file(args)
    elif args.command == "export":
        rich.print(f"{headline_prefix} -> Data Exporter")
        export_to_stdout(args)
