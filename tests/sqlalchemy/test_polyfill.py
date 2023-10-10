import re

import pytest
import sqlalchemy as sa

from cratedb_toolkit.sqlalchemy import check_uniqueness_factory, polyfill_autoincrement, polyfill_refresh_after_dml


def get_autoincrement_model():
    """
    Provide a minimal SQLAlchemy model including an AUTOINCREMENT primary key.
    """
    Base = sa.orm.declarative_base()

    class FooBar(Base):
        """
        Minimal SQLAlchemy model with autoincrement primary key.
        """

        __tablename__ = "foobar"
        identifier = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True)
        foo = sa.Column(sa.String)

    return FooBar


def get_unique_model():
    """
    Provide a minimal SQLAlchemy model including a column with UNIQUE constraint.
    """
    Base = sa.orm.declarative_base()

    class FooBar(Base):
        """
        Minimal SQLAlchemy model with UNIQUE constraint.
        """

        __tablename__ = "foobar"
        identifier = sa.Column(sa.BigInteger, primary_key=True, default=sa.func.now())
        name = sa.Column(sa.String, unique=True, nullable=False)

    return FooBar


def test_autoincrement_vanilla(database):
    """
    When using a model including an autoincrement column, and not assigning a value, CrateDB will fail.
    """
    FooBar = get_autoincrement_model()
    FooBar.metadata.create_all(database.engine)
    with sa.orm.Session(database.engine) as session:
        session.add(FooBar(foo="bar"))
        with pytest.raises(sa.exc.ProgrammingError) as ex:
            session.commit()
        assert ex.match(
            re.escape("SQLParseException[Column `identifier` is required but is missing from the insert statement]")
        )


def test_autoincrement_polyfill(database):
    """
    When using a model including an autoincrement column, and the corresponding polyfill
    is installed, the procedure will succeed.
    """
    polyfill_autoincrement()

    FooBar = get_autoincrement_model()
    FooBar.metadata.create_all(database.engine)
    with sa.orm.Session(database.engine) as session:
        session.add(FooBar(foo="bar"))
        session.commit()


def test_unique_patched(database):
    """
    When using a model including a column with UNIQUE constraint, the SQLAlchemy dialect will ignore it.
    """
    FooBar = get_unique_model()
    FooBar.metadata.create_all(database.engine)

    with sa.orm.Session(database.engine) as session:
        session.add(FooBar(name="name-1"))
        session.commit()
        session.add(FooBar(name="name-1"))
        session.commit()


def test_unique_patched_and_active(database):
    """
    When using a model including a column with UNIQUE constraint, enabling the patch,
    and activating the uniqueness check, SQLAlchemy will raise `DuplicateKeyException`
    errors if uniqueness constraints don't hold.
    """
    FooBar = get_unique_model()
    FooBar.metadata.create_all(database.engine)

    # For uniqueness checks to take place, installing an event handler is needed.
    # TODO: Maybe add to some helper function?
    # TODO: Maybe derive from the model definition itself?
    sa.event.listen(FooBar, "before_insert", check_uniqueness_factory(FooBar, "name"))

    with sa.orm.Session(database.engine) as session:
        polyfill_refresh_after_dml(session)
        session.add(FooBar(name="name-1"))
        session.commit()
        session.add(FooBar(name="name-1"))
        with pytest.raises(sa.exc.IntegrityError) as ex:
            session.commit()
        assert ex.match("DuplicateKeyException on column: foobar.name")
