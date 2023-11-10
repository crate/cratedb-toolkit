import os


def test_version():
    """
    CLI test: Invoke `migr8 --version`.
    """
    exitcode = os.system("migr8 --version")  # noqa: S605,S607
    assert exitcode == 0
