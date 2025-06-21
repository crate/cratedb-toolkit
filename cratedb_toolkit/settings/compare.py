# ruff: noqa: T201
"""
Compare CrateDB cluster settings against default values.

Acquire default settings from CrateDB's documentation, and runtime settings
from a CrateDB cluster, and compare them against each other.

Also handles memory and time-based settings with appropriate tolerances.

## Install
```
uv pip install 'cratedb-toolkit[settings]'
```

## Usage
```
export CRATEDB_CLUSTER_URL=crate://crate@localhost:4200
ctk settings compare [OPTIONS]
```
"""  # noqa: E501

import logging
import re
import textwrap
from collections import defaultdict

import click

from cratedb_toolkit.option import option_cluster_url
from cratedb_toolkit.util.database import DatabaseAdapter

logger = logging.getLogger(__name__)


def flatten_dict(d, parent_key="", sep="."):
    """Convert nested dictionary to flat dictionary with dot notation keys."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def to_bytes(value_str, heap_size_bytes=None):
    """Convert memory value string to bytes."""
    if not value_str or not isinstance(value_str, str):
        return None

    value_str = value_str.lower().strip()

    # Handle percentage value.
    if "%" in value_str:
        if not heap_size_bytes:
            return None

        try:
            percent = float(value_str.replace("%", ""))
            return int(heap_size_bytes * percent / 100)
        except (ValueError, TypeError) as ex:
            raise ValueError("Percentage memory values require a heap size reference") from ex

    # Handle absolute values (kb, mb, gb)
    multipliers = {
        "kb": 1024,
        "mb": 1024 * 1024,
        "gb": 1024 * 1024 * 1024,
        "tb": 1024 * 1024 * 1024 * 1024,
    }

    # Match number and unit (case insensitive)
    match = re.match(r"(\d+(?:\.\d+)?)\s*(kb|mb|gb|tb|b)?", value_str, re.IGNORECASE)
    if not match:
        return None

    number = float(match.group(1))
    unit = match.group(2).lower() if match.group(2) else "b"

    if unit in multipliers:
        return int(number * multipliers[unit])
    return int(number)  # Assume bytes if no unit specified


def to_milliseconds(value_str):
    """Convert time value string to milliseconds."""
    if not value_str or not isinstance(value_str, str):
        return None

    value_str = value_str.lower().strip()

    # Match number and unit for time values
    match = re.match(r"(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)?", value_str, re.IGNORECASE)
    if not match:
        return None

    number = float(match.group(1))
    unit = match.group(2).lower() if match.group(2) else "ms"  # Default to ms if no unit

    # Convert to milliseconds
    multipliers = {
        "ms": 1,
        "s": 1000,
        "m": 60 * 1000,  # minutes
        "h": 60 * 60 * 1000,  # hours
        "d": 24 * 60 * 60 * 1000,  # days
    }

    if unit in multipliers:
        return int(number * multipliers[unit])
    return int(number)  # Assume milliseconds if no unit specified


def format_time(milliseconds):
    """Format milliseconds to human-readable string."""
    if milliseconds < 1000:
        return f"{milliseconds}ms"
    elif milliseconds < 60 * 1000:
        return f"{milliseconds / 1000:.1f}s"
    elif milliseconds < 60 * 60 * 1000:
        return f"{milliseconds / (60 * 1000):.1f}m"
    elif milliseconds < 24 * 60 * 60 * 1000:
        return f"{milliseconds / (60 * 60 * 1000):.1f}h"
    else:
        return f"{milliseconds / (24 * 60 * 60 * 1000):.1f}d"


def normalize_value(value):
    """Normalize value for simple comparison."""
    if value is None:
        return "null"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        value = value.lower().strip()
        # Remove parentheses and their contents - but preserve zeros
        if not value.startswith("0"):
            value = re.sub(r"\s*\([^)]*\)", "", value)
        else:
            # Special handling for "0s (disabled)" and similar values
            value = re.sub(r"\s*\(disabled\)", "", value)

        # Remove trailing 'f' from float values (Java float notation)
        value = re.sub(r"(\d+\.\d+)f$", r"\1", value)
        return value
    return str(value).lower()


def compare_time_settings(setting_key, current_value, default_value):
    """Compare time-based settings and return formatted output if different."""
    current_ms = to_milliseconds(current_value)
    default_ms = to_milliseconds(default_value)

    if current_ms is None or default_ms is None:
        return None

    if current_ms == 0 and default_ms == 0:
        return None

    # Special handling for 0 values - always show if one is zero and the other isn't
    if (current_ms == 0 and default_ms > 0) or (default_ms == 0 and current_ms > 0):
        return f"{setting_key}: {current_value} (default: {default_value})"

    # For non-zero values, check if they differ by more than 1%
    if current_ms != 0 and default_ms != 0:
        if abs(current_ms - default_ms) <= max(current_ms, default_ms) * 0.01:
            return None

    # Format output based on readability
    # If the time value is in milliseconds (likely not human readable), include the conversion
    if "ms" in current_value or len(current_value) > 6:
        formatted_current = f"{current_value} (~{format_time(current_ms)})"
    else:
        formatted_current = current_value

    if "ms" in default_value or len(default_value) > 6:
        formatted_default = f"{default_value} (~{format_time(default_ms)})"
    else:
        formatted_default = default_value

    # If we get here, the values are different
    return f"{setting_key}: {formatted_current} (default: {formatted_default})"


def compare_memory_settings(
    setting_key,
    current_value,
    default_value,
    heap_size_bytes,
    tolerance_percent_large=2.9,
    tolerance_percent_small=1,
    threshold_percent=20,
):
    """Compare memory-based settings and return formatted output if different."""
    current_bytes = to_bytes(str(current_value), heap_size_bytes)
    default_bytes = to_bytes(default_value, heap_size_bytes)

    if current_bytes is None or default_bytes is None:
        return None

    if not heap_size_bytes:
        raise ValueError("Heap size must be provided to compare memory settings.")

    # Calculate percentage of heap
    current_percent = (current_bytes / heap_size_bytes) * 100
    default_percent = (default_bytes / heap_size_bytes) * 100

    # Choose tolerance based on the size of default value
    tolerance = tolerance_percent_large if default_percent >= threshold_percent else tolerance_percent_small

    # Check if values differ by more than tolerance
    if abs(current_percent - default_percent) <= tolerance:
        return None

    # Format for display with calculated percentage
    formatted_current = f"{current_value}"
    formatted_default = f"{default_value}"

    is_percent_format = "%" in str(default_value)
    current_is_percent = "%" in str(current_value)

    # Add percentage if absolute value
    if not current_is_percent:
        formatted_current += f" (~{current_percent:.1f}% of heap)"
    # Add byte equivalent if value is percentage
    else:
        formatted_current += f" (~{current_bytes:,} bytes)"

    # Add percentage/bytes for default value
    if is_percent_format:
        formatted_default += f" (~{default_bytes:,} bytes)"
    else:
        formatted_default += f" (~{default_percent:.1f}% of heap)"

    return f"{setting_key}: {formatted_current} (default: {formatted_default})"


class Color:
    HEADER = "\033[95m"
    BOLD = "\033[1m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    BLUE = "\033[94m"
    RESET = "\033[0m"
    PURPLE = "\033[95m"  # Define purple color


def report_comparison(color: Color, default_settings, non_default_settings):
    BOLD, GREEN, YELLOW, RESET, PURPLE = (
        color.BOLD,
        color.GREEN,
        color.YELLOW,
        color.RESET,
        color.PURPLE,
    )

    # Group settings by category
    categorized_settings = defaultdict(list)

    for setting in sorted(non_default_settings):
        # Extract the top-level category from the setting key
        category = setting.split(":", 1)[0].split(".")[0]
        categorized_settings[category].append(setting)

    # Print settings by category
    if categorized_settings:
        for category, settings in sorted(categorized_settings.items()):
            print(f"{BOLD}{GREEN}{category.upper()}{RESET}")
            print(f"{GREEN}{'=' * len(category)}{RESET}")

            # Print each setting in the category without blank lines between them
            for setting in settings:
                # Split into parts for colored output
                key_part, value_parts = setting.split(":", 1)

                # Format in a single line with appropriate wrapping
                full_line = f"{BOLD}{key_part}:{RESET} {value_parts.strip()}"

                # Add yellow color to the default value part
                full_line = full_line.replace(" (default: ", f" (default: {YELLOW}").replace(")", f"{RESET})")

                # Wrap long lines, preserving the setting name at the start of the first line
                wrapped_text = textwrap.fill(
                    full_line,
                    width=120,  # Increased width
                    subsequent_indent="  ",
                    break_on_hyphens=False,
                    break_long_words=False,  # Prevent breaking words like large byte counts
                )

                # Print the wrapped text for the setting
                print(wrapped_text)

                # Add the statement in purple as a separate line if available
                setting_key_clean = key_part.strip()
                if setting_key_clean in default_settings and "stmt" in default_settings[setting_key_clean]:
                    stmt = default_settings[setting_key_clean]["stmt"]
                    print(f"  {PURPLE}{stmt}{RESET}")

            # Add blank line after each category
            print()
    else:
        print(f"{GREEN}No non-default settings found.{RESET}")

    print(f"\n{BOLD}Total non-default settings: {len(non_default_settings)}{RESET}")


@click.command()
@option_cluster_url
@click.option(
    "--large-tolerance",
    type=float,
    default=2.9,
    help="Tolerance percentage for large memory settings (default: 2.9)",
)
@click.option(
    "--small-tolerance",
    type=float,
    default=1.0,
    help="Tolerance percentage for small memory settings (default: 1.0)",
)
@click.option(
    "--threshold",
    type=float,
    default=20.0,
    help="Threshold percentage to distinguish large from small settings (default: 20.0)",
)
@click.option(
    "--no-color",
    is_flag=True,
    help="Disable colored output",
)
def compare_cluster_settings(
    cluster_url: str,
    large_tolerance=2.9,
    small_tolerance=1.0,
    threshold=20.0,
    no_color=False,
):
    """Compare cluster settings against defaults and print differences."""
    from cratedb_toolkit.docs.settings import SettingsExtractor

    # ANSI color palette – use *local* variables to avoid shadowing globals.
    color = Color()
    if no_color:
        color.HEADER = color.BOLD = color.GREEN = color.YELLOW = color.RED = color.BLUE = color.RESET = color.PURPLE = (
            ""
        )

    HEADER, BOLD, GREEN, YELLOW, RED, BLUE, RESET = (
        color.HEADER,
        color.BOLD,
        color.GREEN,
        color.YELLOW,
        color.RED,
        color.BLUE,
        color.RESET,
    )

    print(f"{BOLD}Comparing settings in {BLUE}{cluster_url}{RESET}{BOLD} against default settings{RESET}")
    adapter = DatabaseAdapter(dburi=cluster_url)

    # Acquire default settings from documentation.
    try:
        extractor = SettingsExtractor()
        extractor.acquire()
        default_settings = extractor.thing["settings"]
    except Exception as e:
        msg = f"{RED}Failed to extract settings: {e}{RESET}"
        logger.exception(msg)
        raise click.ClickException(msg) from e

    # Acquire cluster heap size.
    heap_size_bytes = adapter.get_heap_size()
    if heap_size_bytes:
        formatted_heap = f"{heap_size_bytes:,}".replace(",", "_")
        print(f"{BOLD}Heap Size: {GREEN}{formatted_heap} bytes{RESET}")
    else:
        print(f"{YELLOW}No heap size provided{RESET}")

    # Acquire cluster runtime settings.
    cluster_settings = adapter.get_settings()
    if not cluster_settings:
        print(f"{RED}Error: Could not extract cluster settings{RESET}")
        return

    # Flatten settings for easier comparison.
    flat_settings = flatten_dict(cluster_settings)

    # Find and print non-default settings
    print(f"\n{HEADER}=== Non-default CrateDB settings ==={RESET}")
    print(
        f"{BLUE}(Using {large_tolerance}% tolerance for settings > {threshold}% of heap, "
        f"{small_tolerance}% for smaller settings){RESET}"
    )
    print(f"{BLUE}Default settings loaded{RESET}\n")

    non_default_settings = set()  # Changed from list to set to prevent duplicates

    for setting_key, current_value in sorted(flat_settings.items()):
        if setting_key not in default_settings:
            continue

        # Get default value
        default_value = default_settings[setting_key].get("default_value", "")
        if not default_value:
            continue

        result = None  # Initialize the result to None for each setting

        # Check if this is a time-related setting
        is_time_setting = any(
            time_term in setting_key.lower()
            for time_term in {
                "timeout",
                "interval",
                "delay",
                "expiration",
                "time",
                "duration",
            }
        )
        if is_time_setting and isinstance(current_value, str) and isinstance(default_value, str):
            result = compare_time_settings(setting_key, current_value, default_value)

        # Check if this is a memory-related setting
        elif heap_size_bytes and any(
            mem_term in setting_key for mem_term in {"memory", "heap", "breaker", "limit", "size"}
        ):
            result = compare_memory_settings(
                setting_key,
                current_value,
                default_value,
                heap_size_bytes,
                large_tolerance,
                small_tolerance,
                threshold,
            )

        # Standard comparison for other settings - only if not already handled
        elif result is None:  # Only process if not already handled
            norm_current = normalize_value(current_value)
            norm_default = normalize_value(default_value)
            if norm_current != norm_default:
                result = f"{setting_key}: {current_value} (default: {default_value})"

        # Add to results if different and not already in the set
        if result:
            non_default_settings.add(result)  # Using add() for a set instead of append()

    report_comparison(color, default_settings, non_default_settings)
