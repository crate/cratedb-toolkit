import re

import pymongo
import pymongo.database
import rich
from rich.syntax import Syntax

from .extract import extract_schema_from_collection
from .translate import translate as translate_schema


def parse_input_numbers(s: str):
    """
    Parse an input string for numbers and ranges.

    Supports strings like '0 1 2', '0, 1, 2' as well as ranges such as
    '0-2'.
    """

    options: list = []
    for option in re.split(", | ", s):
        match = re.search(r"(\d+)-(\d+)", option)
        if match:
            lower, upper = sorted([match.group(1), match.group(2)])
            options = options + list(range(int(lower), int(upper) + 1))
        else:
            try:
                options.append(int(option))
            except ValueError:
                pass
    return options


def gather_collections(database):
    """
    Gather a list of collections to use from a MongoDB database, based on user input.
    """

    collections = database.list_collection_names()

    tbl = rich.table.Table(show_header=True, header_style="bold blue")
    tbl.add_column("Id", width=3)
    tbl.add_column("Collection Name")
    tbl.add_column("Estimated Size")

    for i, c in enumerate(collections):
        tbl.add_row(str(i), c, str(database[c].estimated_document_count()))

    rich.print(tbl)

    rich.print("\nCollections to exclude: (eg: '0 1 2', '0, 1, 2', '0-2')")

    collections_to_ignore = parse_input_numbers(input("> "))
    filtered_collections = []
    for i, c in enumerate(collections):
        if i not in collections_to_ignore:
            filtered_collections.append(c)

    # MongoDB 2 does not understand `include_system_collections=False`.
    filtered_collections = [item for item in filtered_collections if not item.startswith("system.")]

    return filtered_collections


def extract(args):
    """
    Extract schemas from MongoDB collections.

    This asks the user for which collections they would like to extract,
    iterates over these collections and returns a dictionary of schemas for
    each of the selected collections.
    """

    rich.print("\n[green bold]MongoDB[/green bold] -> [blue bold]CrateDB[/blue bold] Exporter :: Schema Extractor\n\n")

    client: pymongo.MongoClient = pymongo.MongoClient(args.host, int(args.port))
    db: pymongo.database.Database = client.get_database(args.database)
    if args.collection:
        filtered_collections = [args.collection]
    else:
        filtered_collections = gather_collections(db)

    if not filtered_collections:
        rich.print("\nExcluding all collections. Nothing to do.")
        exit(0)

    if args.scan:
        partial = args.scan == "partial"
    else:
        rich.print("\nDo a [red bold]full[/red bold] collection scan?")
        rich.print("A full scan will iterate over all documents in the collection, a partial only one document. (Y/n)")
        full = input(">  ").strip().lower()

        partial = full != "y"

        rich.print(f"\nExecuting a [red bold]{'partial' if partial else 'full'}[/red bold] scan...")

    schemas = {}
    for collection in filtered_collections:
        schemas[collection] = extract_schema_from_collection(db[collection], partial)
    return schemas


def translate(schema):
    """
    Translate a given schema into a CrateDB compatible SQL DDL statement.
    """
    rich.print("\n[green bold]MongoDB[/green bold] -> [blue bold]CrateDB[/blue bold] Exporter :: Schema Extractor\n\n")
    sql_queries = translate_schema(schema)
    for collection, query in sql_queries.items():
        syntax = Syntax(query, "sql")
        rich.print(f"Collection [blue bold]'{collection}'[/blue bold]:")
        rich.print(syntax)
        rich.print()
