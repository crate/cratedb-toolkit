import logging
from functools import lru_cache

from yarl import URL

from cratedb_toolkit.io.ingestr.boot import import_ingestr
from cratedb_toolkit.model import DatabaseAddress

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_ingestr():
    """
    Get an ingestr API handle, cached.
    """
    return import_ingestr()


def ingestr_select(source_url: str) -> bool:
    """
    Whether to select `ingestr` for this data source.
    """
    ingestr_available, ingestr, ConfigFieldMissingException = _get_ingestr()

    if not ingestr_available:
        logger.debug("ingestr is not installed")
        return False
    try:
        factory = ingestr.src.factory.SourceDestinationFactory(source_url, "csv:////tmp/foobar.csv")
        factory.get_source()
        scheme = ingestr.src.factory.parse_scheme_from_uri(source_url)
        logger.info(f"Selecting ingestr for source scheme: {scheme}")
        return True
    except (ImportError, ValueError, AttributeError) as ex:
        if "Unsupported source scheme" in str(ex):
            logger.debug(f"Failed to select ingestr for source url '{source_url}': {ex}")
        else:
            logger.exception(f"Unexpected error with ingestr for source url: {source_url}")
        return False
    except Exception:
        logger.exception(f"Unexpected error with ingestr for source url: {source_url}")
        return False


def ingestr_copy(source_url: str, target_address: DatabaseAddress, progress: bool = False) -> bool:
    """
    Invoke data transfer to CrateDB from any source provided by `ingestr`.

    https://cratedb-toolkit.readthedocs.io/io/ingestr/

    Synopsis:

        ctk load table \
            "frankfurter://?base=EUR&table=latest" \
            --cluster-url="crate://crate:na@localhost:4200/testdrive/exchange_latest"

        ctk load table \
            "frankfurter://?base=EUR&table=currencies" \
            --cluster-url="crate://crate:na@localhost:4200/testdrive/exchange_currencies"

        ctk load table \
            "postgresql://pguser:secret11@postgresql.example.org:5432/postgres?table=public.diamonds" \
            --cluster-url="crate://crate:na@localhost:4200/testdrive/ibis_diamonds"
    """
    ingestr_available, ingestr, ConfigFieldMissingException = _get_ingestr()

    # Sanity checks.
    if not ingestr_available:
        raise ModuleNotFoundError("ingestr subsystem not installed")

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
        target_table = source_table

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
    except Exception as ex:
        logger.exception(f"Failed to ingest data: {ex}")
        return False
