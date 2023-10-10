def test_example_materialized_view(store):
    """
    Verify that the program `examples/materialized_view.py` works.
    """
    from examples.materialized_view import main

    # Run the example.
    main(dburi=store.database.dburi)

    # Verify materialized view was created and populated.
    count = store.database.count_records("examples.raw_metrics_view")
    assert count == 3
