from pathlib import Path

import pytest
from testbook import testbook

pytestmark = pytest.mark.python


ROOT = Path(__file__).parent.parent.parent


def test_managed_cluster_app(cloud_environment):
    """
    Verify that the program `examples/python/cloud_cluster.py` works.
    """

    from examples.python.cloud_cluster import main

    main()


def test_managed_import_app(cloud_environment):
    """
    Verify that the program `examples/python/cloud_import.py` works.
    """

    from examples.python.cloud_import import main

    main()


def test_managed_import_notebook(cloud_environment):
    """
    Verify the Jupyter notebook `examples/notebook/cloud_import.py` works.
    """

    # Execute the notebook.
    notebook = Path("examples") / "notebook" / "cloud_import.ipynb"
    if not notebook.exists():
        pytest.fail(f"Notebook not found: {notebook}")
    with testbook(notebook, timeout=180) as tb:
        tb.execute()
