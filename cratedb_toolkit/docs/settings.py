#!/usr/bin/env uv python
"""
CrateDB Settings Extractor

This tool extracts settings from CrateDB's documentation and outputs them
in either JSON or Markdown format, or the SQL statements to set the default value.
It parses the HTML structure of the documentation to identify settings, their
descriptions, default values, and whether they're runtime configurable.

Author: wolta
Date: April 2025
Source: https://gist.github.com/WalBeh/c863eb5cc35ee987d577851f38b64261
"""

# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "beautifulsoup4",
#     "requests",
#     "click",
#     "rich",
#     "tqdm",
# ]
# ///

import json
import logging
import re
import sys
from typing import Any, Dict, List, Optional

import click
import requests
from bs4 import BeautifulSoup
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import Progress, SpinnerColumn, TextColumn

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True, markup=True)],
)
logger = logging.getLogger("cratedb_settings")
console = Console()

# Constants
DOCS_URL = "https://cratedb.com/docs/crate/reference/en/latest/config/cluster.html"
DEFAULT_JSON_OUTPUT = "cratedb_settings.json"
DEFAULT_MD_OUTPUT = "cratedb_settings.md"
SET_CLUSTER = "SET GLOBAL PERSISTENT"


def extract_cratedb_settings() -> Dict[str, Dict[str, Any]]:
    """
    Extract CrateDB settings from the documentation website.

    Returns:
        Dict[str, Dict[str, Any]]: Dictionary of settings with their properties
    """
    settings = {}

    with console.status("[bold green]Fetching documentation...", spinner="dots"):
        response = requests.get(DOCS_URL, timeout=5)

    if response.status_code != 200:
        logger.error(f"Failed to fetch documentation: HTTP {response.status_code}")
        return {}

    logger.info("Successfully retrieved documentation page")

    # Parse HTML
    soup = BeautifulSoup(response.text, "html.parser")

    # Process content
    with Progress(SpinnerColumn(), TextColumn("[bold blue]{task.description}"), console=console) as progress:
        # Find all section divs that contain settings
        sections = soup.find_all(["div", "section"], class_=["section", "doc-content"])
        logger.debug(f"Found {len(sections)} potential sections")

        # Process tables
        task1 = progress.add_task("[yellow]Processing tables...", total=None)
        tables = soup.find_all("table")
        settings.update(_process_tables(tables))
        progress.update(task1, completed=True)

        # Process definition lists
        task2 = progress.add_task("[yellow]Processing definition lists...", total=None)
        dl_elements = soup.find_all("dl")
        settings.update(_process_definition_lists(dl_elements))
        progress.update(task2, completed=True)

        # Process headers
        task3 = progress.add_task("[yellow]Processing section headers...", total=None)
        settings.update(_process_section_headers(sections))
        progress.update(task3, completed=True)

        # Find runtime configurable settings
        task4 = progress.add_task("[yellow]Identifying runtime configurable settings...", total=None)
        _identify_runtime_settings(settings)
        progress.update(task4, completed=True)

    logger.info(f"Extracted {len(settings)} settings in total")

    # Count runtime settings
    runtime_count = sum(1 for info in settings.values() if info["runtime_configurable"])
    logger.info(f"Found {runtime_count} runtime configurable settings")

    return settings


def _process_tables(tables: List) -> Dict[str, Dict[str, Any]]:
    """Process tables to extract settings."""
    settings = {}

    for table in tables:
        # Check if this has headers
        headers_row = table.find("tr")
        if not headers_row:
            continue

        # Extract header information
        headers = [th.get_text().strip().lower() for th in headers_row.find_all(["th", "td"])]

        # Check if this looks like a settings table
        if not any(keyword in " ".join(headers) for keyword in ["setting", "name", "property"]):
            continue

        # Find column indices
        indices = {
            "setting": next(
                (i for i, h in enumerate(headers) if any(kw in h for kw in ["setting", "name", "property"])),
                None,
            ),
            "desc": next((i for i, h in enumerate(headers) if "description" in h), None),
            "runtime": next(
                (i for i, h in enumerate(headers) if any(kw in h for kw in ["runtime", "dynamic"])),
                None,
            ),
            "default": next((i for i, h in enumerate(headers) if "default" in h), None),
            "type": next((i for i, h in enumerate(headers) if "type" in h), None),
        }

        if indices["setting"] is None:
            continue

        # Process rows
        for row in table.find_all("tr")[1:]:  # Skip header row
            cells = row.find_all(["td", "th"])
            if len(cells) <= indices["setting"]:
                continue

            # Extract setting information
            setting_info = _extract_setting_from_table_row(cells, indices)
            if setting_info:
                settings[setting_info["name"]] = setting_info["data"]

    return settings


def _extract_setting_from_table_row(cells, indices) -> Optional[Dict[str, Any]]:
    """Extract a setting from a table row."""
    # Get setting name
    setting_cell = cells[indices["setting"]]
    setting_key = setting_cell.get_text().strip()

    # Try to get from code element if present
    code_elem = setting_cell.find("code")
    if code_elem:
        setting_key = code_elem.get_text().strip()

    # Clean up setting key
    setting_key = re.sub(r"\s+", " ", setting_key)
    setting_key = setting_key.split(":", 1)[0] if ":" in setting_key else setting_key

    # Skip if this doesn't look like a setting
    if not setting_key or setting_key.startswith("#") or len(setting_key) > 100:
        return None

    # Initialize structured fields
    setting_info = {
        "raw_description": "",
        "runtime_configurable": False,
        "default_value": "",
        "type": "",
        "purpose": "",
        "constraints": "",
        "related_settings": [],
        "deprecated": False,
    }

    # Get description
    if indices["desc"] is not None and indices["desc"] < len(cells):
        setting_info["raw_description"] = cells[indices["desc"]].get_text().strip()

    # Get default value directly from table if available
    if indices["default"] is not None and indices["default"] < len(cells):
        setting_info["default_value"] = cells[indices["default"]].get_text().strip()

    # Get type directly from table if available
    if indices["type"] is not None and indices["type"] < len(cells):
        setting_info["type"] = cells[indices["type"]].get_text().strip()

    # Check runtime configurability
    if indices["runtime"] is not None and indices["runtime"] < len(cells):
        runtime_text = cells[indices["runtime"]].get_text().strip().lower()
        setting_info["runtime_configurable"] = _is_runtime_configurable(runtime_text)

    # Parse description for additional information
    parse_description(setting_info)

    return {"name": setting_key, "data": setting_info}


def _process_definition_lists(dl_elements: List) -> Dict[str, Dict[str, Any]]:
    """Process definition lists to extract settings."""
    settings = {}

    for dl in dl_elements:
        dt_elements = dl.find_all("dt")
        dd_elements = dl.find_all("dd")

        if len(dt_elements) == 0 or len(dd_elements) == 0:
            continue

        for i in range(min(len(dt_elements), len(dd_elements))):
            dt = dt_elements[i]
            dd = dd_elements[i]

            # Get setting name
            setting_name = dt.get_text().strip()

            # Try to extract from code element if present
            code_elem = dt.find("code")
            if code_elem:
                setting_name = code_elem.get_text().strip()

            # Clean up setting key
            setting_name = re.sub(r"\s+", " ", setting_name)
            setting_key = setting_name.split(":", 1)[0] if ":" in setting_name else setting_name

            # Skip if doesn't look like a valid setting
            if not setting_key or len(setting_key) > 100:
                continue

            # Initialize structured fields
            setting_info = {
                "raw_description": dd.get_text().strip(),
                "runtime_configurable": False,
                "default_value": "",
                "type": "",
                "purpose": "",
                "constraints": "",
                "related_settings": [],
                "deprecated": False,
            }

            # Check runtime configurability
            desc_lower = setting_info["raw_description"].lower()
            setting_info["runtime_configurable"] = _is_runtime_configurable(desc_lower)

            # Parse description into structured fields
            parse_description(setting_info)

            # Add to settings dictionary
            settings[setting_key] = setting_info

    return settings


def _process_section_headers(sections: List) -> Dict[str, Dict[str, Any]]:
    """Process section headers to extract settings."""
    settings = {}

    for section in sections:
        # Look for setting headers
        headers = section.find_all(["h2", "h3", "h4"])
        for header in headers:
            header_text = header.get_text().strip()

            # Look for typical setting patterns
            if re.search(r"^[a-z0-9_.]+$", header_text, re.IGNORECASE) and header_text not in settings:
                setting_key = header_text

                # Initialize structured fields
                setting_info = {
                    "raw_description": "",
                    "runtime_configurable": False,
                    "default_value": "",
                    "type": "",
                    "purpose": "",
                    "constraints": "",
                    "related_settings": [],
                    "deprecated": False,
                }

                # Get the paragraph after the header for description
                next_elem = header.find_next(["p", "div"])
                if next_elem:
                    setting_info["raw_description"] = next_elem.get_text().strip()

                # Check runtime configurability
                desc_lower = str(setting_info["raw_description"]).lower()
                setting_info["runtime_configurable"] = _is_runtime_configurable(desc_lower)

                # Parse description into structured fields
                parse_description(setting_info)

                # Add to settings dictionary
                settings[setting_key] = setting_info

    return settings


def _identify_runtime_settings(settings: Dict[str, Dict[str, Any]]) -> None:
    """Identify runtime configurable settings based on documentation patterns."""
    for _, info in settings.items():
        if not info["runtime_configurable"]:
            desc_lower = info["raw_description"].lower()
            runtime_patterns = [
                r"runtime:\s*yes",
                r"dynamic:\s*true",
                r"runtime\s+configurable",
                r"can be changed at runtime",
            ]

            for pattern in runtime_patterns:
                if re.search(pattern, desc_lower) and "not runtime configurable" not in desc_lower:
                    info["runtime_configurable"] = True
                    break


def _is_runtime_configurable(text: str) -> bool:
    """Determine if a setting is runtime configurable based on text."""
    # First check if "Runtime: no" is explicitly stated
    if re.search(r"runtime:\s*no", text, re.IGNORECASE):
        return False

    # Then check positive indicators
    indicators = [
        "yes" in text and "runtime: yes" in text.lower(),
        "true" in text and "dynamic: true" in text.lower(),
        "✓" in text,
        text == "y",
        "runtime: yes" in text.lower(),
        "dynamic: true" in text.lower(),
        "runtime configurable" in text.lower() and "not runtime configurable" not in text.lower(),
    ]

    return any(indicators)


def parse_description(setting_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse description text to extract structured information.

    Args:
        setting_info: Dictionary containing setting information

    Returns:
        Updated setting information dictionary
    """
    desc = setting_info["raw_description"]

    # First check for "Runtime: yes/no" and explicitly set runtime_configurable
    runtime_match = re.search(r"runtime:\s*(yes|no)", desc, re.IGNORECASE)
    if runtime_match:
        setting_info["runtime_configurable"] = runtime_match.group(1).lower() == "yes"

    # Extract default value with improved regex for various formats
    default_match = re.search(r"default:?\s*([^\n]+)", desc, re.IGNORECASE)
    if default_match:
        # Extract the full default value line and clean it up
        default_value = default_match.group(1).strip()
        # If it ends with a period followed by whitespace and more text, trim there
        if re.search(r"\.\s+[A-Z]", default_value):
            default_value = re.split(r"\.\s+[A-Z]", default_value)[0] + "."
        setting_info["default_value"] = default_value

    # Extract type information
    type_patterns = [
        r"(type|data type|value type):?\s*([a-zA-Z0-9_\- ]+)",
        r"(string|integer|boolean|float|double|time|list|array|enum) (setting|value|type)",
    ]

    for pattern in type_patterns:
        type_match = re.search(pattern, desc, re.IGNORECASE)
        if type_match:
            if "type" in type_match.group(1).lower():
                setting_info["type"] = type_match.group(2).strip()
            else:
                setting_info["type"] = type_match.group(1).strip()
            break

    # Check for deprecated status
    setting_info["deprecated"] = "deprecated" in desc.lower()

    # Extract constraints
    constraint_patterns = [
        r"(must be|valid values|valid range|range is|range of|between|min|max|maximum|minimum).{1,100}",
        r"(only|strictly).{1,50}(positive|negative|greater than|less than|non-negative).{1,50}",
    ]

    for pattern in constraint_patterns:
        constraint_match = re.search(pattern, desc, re.IGNORECASE)
        if constraint_match:
            setting_info["constraints"] = constraint_match.group(0).strip()
            break

    # Extract related settings
    related_settings = re.findall(r"([a-z][a-z0-9_\-.]+\.[a-z0-9_\-.]+)", desc)
    if related_settings:
        setting_info["related_settings"] = list(set(related_settings))  # Remove duplicates

    # Extract purpose (the first sentence or two that's not about defaults, types, or constraints)
    purpose_text = desc

    # Remove sections about defaults, types, constraints
    patterns_to_remove = [
        r'default(s)? (is|are|to|value)?:?\s*[\'"`]?[^\'"`\n.,;]+[\'"`]?',
        r"type:?\s*[a-zA-Z0-9_\- ]+",
        r"(must be|valid values|valid range).{1,100}",
        r"(deprecated).{1,100}",
        r"runtime configurable",
        r"runtime:\s*yes",
        r"dynamic:\s*true",
    ]

    for pattern in patterns_to_remove:
        purpose_text = re.sub(pattern, "", purpose_text, flags=re.IGNORECASE)

    # Get the first 1-2 sentences
    sentences = re.split(r"(?<=[.!?])\s+", purpose_text)
    if sentences:
        purpose = " ".join(sentences[: min(2, len(sentences))])
        setting_info["purpose"] = purpose.strip()

    return setting_info


def write_markdown_table(settings: Dict[str, Dict[str, Any]], output_file: str) -> None:
    """
    Write settings to a markdown table file.

    Args:
        settings: Dictionary of settings
        output_file: Path to output file
    """
    with console.status(f"[bold green]Writing Markdown to {output_file}..."):
        with open(output_file, "w", encoding="utf-8") as f:
            # Write header with metadata
            f.write("# CrateDB Settings Reference\n\n")
            f.write(f"*Generated on {click.format_filename(output_file)}*\n\n")
            f.write("This document contains all CrateDB settings, their default values, and descriptions.\n\n")

            # Write runtime configurable settings table
            runtime_settings = {k: v for k, v in settings.items() if v["runtime_configurable"]}
            f.write(f"## Runtime Configurable Settings ({len(runtime_settings)})\n\n")
            f.write("These settings can be changed while the cluster is running.\n\n")
            f.write("| Setting | Default Value | Description | SQL Statement |\n")
            f.write("|---------|---------------|-------------|--------------|\n")

            # Sort settings for better readability
            for key, info in sorted(runtime_settings.items()):
                # Escape pipe symbols in all fields
                setting = key.replace("|", "\\|")
                default = info["default_value"].replace("|", "\\|") if info["default_value"] else "-"
                desc = info["purpose"].replace("\n", " ").replace("|", "\\|")
                stmt = info.get("stmt", "").replace("|", "\\|") if info.get("stmt") else "-"

                f.write(f"| {setting} | {default} | {desc} | {stmt} |\n")

            # Write non-runtime configurable settings table
            non_runtime_settings = {k: v for k, v in settings.items() if not v["runtime_configurable"]}
            f.write(f"\n\n## Non-Runtime Configurable Settings ({len(non_runtime_settings)})\n\n")
            f.write("These settings can only be changed by restarting the cluster.\n\n")
            f.write("| Setting | Default Value | Description |\n")
            f.write("|---------|---------------|-------------|\n")

            for key, info in sorted(non_runtime_settings.items()):
                # Escape pipe symbols in all fields
                setting = key.replace("|", "\\|")
                default = info["default_value"].replace("|", "\\|") if info["default_value"] else "-"
                desc = info["purpose"].replace("\n", " ").replace("|", "\\|")

                f.write(f"| {setting} | {default} | {desc} |\n")

    logger.info(f"Successfully wrote Markdown table to {output_file}")


def generate_sql_statements(settings: Dict[str, Dict[str, Any]]) -> None:
    """
    Generate SQL statements for runtime configurable settings.

    Args:
        settings: Dictionary of settings
    """
    with console.status("[bold green]Generating SQL statements..."):
        count = 0
        for setting_key, setting_info in settings.items():
            # First, remove any stmt field for non-runtime configurable settings
            if not setting_info["runtime_configurable"]:
                if "stmt" in setting_info:
                    del setting_info["stmt"]
                continue

            # Only generate statements for runtime configurable settings
            default_value = setting_info.get("default_value", "")

            # Handle wildcard settings
            if "*" in setting_key:
                # For wildcard settings, generate a template statement
                setting_info["stmt"] = f"{SET_CLUSTER} \"{setting_key}\" = '<VALUE>'; -- Requires specific value"
                count += 1
                continue

            # Skip empty default values that aren't wildcards
            if not default_value:
                continue

            # Convert 'true', 'false' to lowercase
            if default_value.lower() in ("true", "false"):
                default_value = default_value.lower()

            # Handle special float notation like 0.45f
            if re.match(r"^-?\d+\.\d+f$", default_value):
                default_value = default_value.rstrip("f")

            # Determine if the value needs quotes
            is_numeric = re.match(r"^-?\d+(\.\d+)?$", default_value)

            if is_numeric:
                stmt = f'{SET_CLUSTER} "{setting_key}" = {default_value}'
            else:
                # Handle values that already have quotes
                if default_value.startswith("'") and default_value.endswith("'"):
                    stmt = f'{SET_CLUSTER} "{setting_key}" = {default_value}'
                else:
                    stmt = f"{SET_CLUSTER} \"{setting_key}\" = '{default_value}'"

            setting_info["stmt"] = stmt
            count += 1

    logger.info(f"Generated {count} SQL statements for runtime configurable settings")

    # Verify no non-runtime settings have statements
    bad_stmts = sum(1 for info in settings.values() if not info["runtime_configurable"] and "stmt" in info)
    if bad_stmts > 0:
        logger.warning(f"Found {bad_stmts} SQL statements for non-runtime settings - removing them")
        for info in settings.values():
            if not info["runtime_configurable"] and "stmt" in info:
                del info["stmt"]


def print_sql_statements(settings: Dict[str, Dict[str, Any]]) -> None:
    """
    Print SQL statements for runtime configurable settings to stdout.

    Args:
        settings: Dictionary of settings
    """
    # Print header
    print("SQL Statements for Runtime Configurable CrateDB Settings")  # noqa: T201
    print("=" * 60)  # noqa: T201

    # Count statements
    statement_count = 0

    # Print all statements with comments and semicolons
    for _, setting_info in sorted(settings.items()):
        if setting_info["runtime_configurable"] and "stmt" in setting_info:
            stmt = setting_info["stmt"]

            # Ensure statement ends with semicolon
            if not stmt.endswith(";"):
                stmt += ";"

            # Try to find configuration options for commenting
            config_options = ""

            # Check constraints field
            if setting_info["constraints"]:
                config_options = setting_info["constraints"]

            # If no explicit constraints, check raw_description for allowed values
            elif "raw_description" in setting_info:
                # Look for patterns like "Allowed values: option1 | option2 | option3"
                allowed_match = re.search(
                    r"allowed values:?\s*([a-zA-Z0-9_\-]+((\s*\|\s*)[a-zA-Z0-9_\-]+)+)",
                    setting_info["raw_description"],
                    re.IGNORECASE,
                )
                if allowed_match:
                    options_list = allowed_match.group(1).strip()
                    config_options = f"Allowed values: {options_list}"

            # Add the comment if we found config options
            if config_options:
                stmt = f"{stmt} -- {config_options}"

            print(stmt)  # noqa: T201
            statement_count += 1

    print(f"\nTotal statements: {statement_count}")  # noqa: T201


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--format",
    "-f",
    "format_",
    type=click.Choice(["json", "markdown", "sql"]),
    default="json",
    help="Output format (json, markdown or sql)",
)
@click.option("--output", "-o", default=None, help="Output file name")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.version_option(version="1.0.0")
def main(format_: str, output: str, verbose: bool):
    """
    Extract CrateDB settings from documentation and save them in JSON, Markdown, or SQL format.

    This tool scrapes the CrateDB documentation to extract configuration settings,
    their default values, descriptions, and runtime configurability status.

    Examples:

        # Extract settings to JSON (default)
        python soup2.py

        # Extract settings to Markdown
        python soup2.py --format markdown

        # Extract SQL statements for runtime configurable settings
        python soup2.py --format sql

        # Specify custom output file
        python soup2.py --format markdown --output cratedb_reference.md
    """
    # Configure logging level
    if verbose:
        logger.setLevel(logging.DEBUG)

    try:
        # Extract settings
        settings = extract_cratedb_settings()

        if not settings:
            logger.error("No settings were extracted. Please check the script or documentation structure.")
            sys.exit(1)

        # Generate SQL statements for runtime configurable settings
        generate_sql_statements(settings)

        # Determine output file name
        if output is None:
            if format_ == "markdown":
                output = DEFAULT_MD_OUTPUT
            elif format_ == "sql":
                output = "cratedb_settings.sql"
            else:
                output = DEFAULT_JSON_OUTPUT

        # Save to file in selected format
        if format_ == "json":
            with open(output, "w", encoding="utf-8") as f:
                json.dump(settings, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(settings)} settings to {output}")
        elif format_ == "markdown":
            write_markdown_table(settings, output)
        elif format_ == "sql":
            with console.status(f"[bold green]Writing SQL statements to {output}..."):
                with open(output, "w", encoding="utf-8") as f:
                    f.write("-- CrateDB Runtime Configurable Settings\n")
                    f.write("-- Generated by settings extractor\n\n")

                    count = 0
                    for _, setting_info in sorted(settings.items()):
                        if setting_info["runtime_configurable"] and "stmt" in setting_info:
                            stmt = setting_info["stmt"]

                            # Ensure statement ends with semicolon
                            if not stmt.endswith(";"):
                                stmt += ";"

                            f.write(f"{stmt}\n")
                            count += 1

                    f.write(f"\n-- Total statements: {count}\n")
            logger.info(f"Saved {count} SQL statements to {output}")

        # Count runtime configurable settings
        runtime_count = sum(1 for info in settings.values() if info["runtime_configurable"])
        logger.info(f"Found {runtime_count} runtime configurable settings out of {len(settings)} total")

    except KeyboardInterrupt:
        logger.warning("Operation cancelled by user")
        sys.exit(130)
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
