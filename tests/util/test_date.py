# Copyright (c) 2021-2025, Crate.io Inc.
# Distributed under the terms of the AGPLv3 license, see LICENSE.
"""
Tests for date/duration utility functions.
"""

from datetime import timedelta

import pytest

from cratedb_toolkit.util.date import parse_duration


def test_parse_duration_days():
    assert parse_duration("7d") == timedelta(days=7)


def test_parse_duration_hours():
    assert parse_duration("24h") == timedelta(hours=24)


def test_parse_duration_minutes():
    assert parse_duration("30m") == timedelta(minutes=30)


def test_parse_duration_seconds():
    assert parse_duration("90s") == timedelta(seconds=90)


def test_parse_duration_weeks():
    assert parse_duration("2w") == timedelta(weeks=2)


def test_parse_duration_combined():
    assert parse_duration("1d12h") == timedelta(days=1, hours=12)


def test_parse_duration_full_combination():
    assert parse_duration("1w2d3h15m30s") == timedelta(weeks=1, days=2, hours=3, minutes=15, seconds=30)


def test_parse_duration_whitespace_stripped():
    assert parse_duration("  7d  ") == timedelta(days=7)


def test_parse_duration_invalid_format():
    with pytest.raises(ValueError, match="Invalid duration"):
        parse_duration("seven days")


def test_parse_duration_empty_string():
    with pytest.raises(ValueError, match="Invalid duration"):
        parse_duration("")


def test_parse_duration_zero():
    with pytest.raises(ValueError, match="must be positive"):
        parse_duration("0d")
