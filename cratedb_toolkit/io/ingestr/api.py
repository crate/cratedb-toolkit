import logging
from unittest import mock

from yarl import URL

from cratedb_toolkit.model import DatabaseAddress

logger = logging.getLogger(__name__)


def _import_ingestr():
    """Import ingestr with telemetry disabled."""
    try:
        with mock.patch("ingestr.src.telemetry.event.track"), mock.patch(
            "dlt.common.runtime.telemetry._TELEMETRY_STARTED", True
        ):
            import ingestr.main
            import ingestr.src.factory
            from dlt.common.configuration import ConfigFieldMissingException

            return True, ingestr, ConfigFieldMissingException
    except ImportError:
        logger.error("Could not import ingestr")
        return False, None, None


ingestr_available, ingestr, ConfigFieldMissingException = _import_ingestr()


def ingestr_select(source_url: str) -> bool:
    """
    Whether to select `ingestr` for this data source.
    """
    if not ingestr_available:
        return False
    try:
        factory = ingestr.src.factory.SourceDestinationFactory(source_url, "csv:////tmp/foobar.csv")
        factory.get_source()
        scheme = ingestr.src.factory.parse_scheme_from_uri(source_url)
        logger.info(f"Selecting ingestr for source scheme: {scheme}")
        return True
    except Exception as ex:
        if "Unsupported source scheme" in str(ex):
            logger.debug(f"Failed to select ingestr for source url '{source_url}': {ex}")
        else:
            logger.exception("Unknown error with ingestr")
        return False


def ingestr_copy(source_url: str, target_address: DatabaseAddress, progress: bool = False):
    """
    Invoke data transfer to CrateDB from any source provided by `ingestr`.

    https://cratedb-toolkit.readthedocs.io/io/ingestr/

    Synopsis:

        ctk load table \
            "postgresql://pguser:secret11@postgresql.example.org:5432/postgres?table=public.diamonds" \
            --cluster-url="crate://crate:na@localhost:4200/testdrive/ibis_diamonds"
    """

    # Sanity checks.
    if not ingestr_available:
        raise ModuleNotFoundError("ingestr package not installed, use `uv pip install ingestr`")

    # Compute source and target URLs and table names.
    # Table names use dotted notation `<schema>.<table>`.

    source_url_obj = URL(source_url)
    source_table = source_url_obj.query.get("table")
    source_fragment = source_url_obj.fragment
    source_url_obj = source_url_obj.without_query_params("table").with_fragment("")

    target_uri, target_table_address = target_address.decode()
    target_table = target_table_address.fullname
    target_url = target_address.to_ingestr_url()

    if not source_table:
        raise ValueError("Source table is required")
    if not target_table:
        raise ValueError("Target table is required")

    if source_fragment:
        source_table += f"#{source_fragment}"

    logger.info("Invoking ingestr")
    logger.info(f"Source URL: {source_url_obj}")
    logger.info(f"Target URL: {target_url}")
    logger.info(f"Source Table: {source_table}")
    logger.info(f"Target Table: {target_table}")

    try:
        ingestr.main.ingest(
            source_uri=str(source_url_obj),
            dest_uri=str(target_url),
            source_table=source_table,
            dest_table=target_table,
            yes=True,
        )
        return True
    except ConfigFieldMissingException:
        logger.error(
            "A configuration field is missing. Please ensure all required credentials are provided. "
            "For example, if your account does not use a password, use a dummy password `na` like "
            "`export CRATEDB_CLUSTER_URL=crate://crate:na@localhost:4200/testdrive`"
        )
        raise
