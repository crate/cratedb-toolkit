import logging
from functools import lru_cache

from yarl import URL

from cratedb_toolkit.io.omniload.boot import import_omniload
from cratedb_toolkit.model import DatabaseAddress

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _get_omniload():
    """
    Get an omniload API handle, cached.
    """
    return import_omniload()


def omniload_select(source_url: str) -> bool:
    """
    Whether to select `omniload` for this data source.
    """
    omniload_available, omniload, ConfigFieldMissingException = _get_omniload()

    if not omniload_available:
        logger.debug("omniload is not installed")
        return False
    try:
        factory = omniload.src.factory.SourceDestinationFactory(source_url, "csv:////tmp/foobar.csv")
        factory.get_source()
        scheme = omniload.src.factory.parse_scheme_from_uri(source_url)
        logger.info(f"Selecting omniload for source scheme: {scheme}")
        return True
    except (ImportError, ValueError, AttributeError) as ex:
        if "Unsupported source scheme" in str(ex):
            logger.debug(f"Failed to select omniload for source url '{source_url}': {ex}")
        else:
            logger.exception(f"Unexpected error with omniload for source url: {source_url}")
        return False
    except Exception:
        logger.exception(f"Unexpected error with omniload for source url: {source_url}")
        return False


def omniload_copy(source_url: str, target_address: DatabaseAddress, progress: bool = False) -> bool:
    """
    Invoke data transfer to CrateDB from any source provided by `omniload`.

    https://cratedb-toolkit.readthedocs.io/io/omniload/

    Synopsis:

        ctk load \
            "frankfurter://?base=EUR&table=latest" \
            "crate://crate:na@localhost:4200/testdrive/exchange_latest"

        ctk load \
            "frankfurter://?base=EUR&table=currencies" \
            "crate://crate:na@localhost:4200/testdrive/exchange_currencies"

        ctk load \
            "postgresql://pguser:secret11@postgresql.example.org:5432/postgres?table=public.diamonds" \
            "crate://crate:na@localhost:4200/testdrive/ibis_diamonds"
    """
    omniload_available, omniload, ConfigFieldMissingException = _get_omniload()

    # Sanity checks.
    if not omniload_available:
        raise ModuleNotFoundError("omniload subsystem not installed")

    # Compute source and target URLs and table names.
    # Table names use dotted notation `<schema>.<table>`.

    source_url_obj = URL(source_url)
    source_table = source_url_obj.query.get("table")
    source_fragment = source_url_obj.fragment

    batch_size_raw = source_url_obj.query.get("batch_size")
    batch_size = None
    if batch_size_raw is not None:
        try:
            batch_size = int(batch_size_raw)
        except ValueError as ex:
            raise ValueError("`batch_size` must be an integer") from ex
        if batch_size <= 0:
            raise ValueError("`batch_size` must be greater than 0")

    start_date = source_url_obj.query.get("start_date")
    source_url_obj = source_url_obj.without_query_params("table", "start_date", "batch_size").with_fragment("")

    target_uri, target_table_address = target_address.decode()
    target_table = target_table_address.fullname
    target_url = target_address.to_omniload_url()

    if not source_table:
        raise ValueError("Source table is required")
    if not target_table:
        target_table = source_table

    if source_fragment:
        source_table += f"#{source_fragment}"

    logger.info("Invoking omniload")
    logger.info(f"Source URL: {source_url_obj}")
    logger.info(f"Target URL: {target_url}")
    logger.info(f"Source Table: {source_table}")
    logger.info(f"Target Table: {target_table}")
    logger.info(f"Start Date: {start_date}")
    logger.info(f"Batch Size: {batch_size}")

    ingest_kwargs = dict(  # noqa: C408
        source_uri=str(source_url_obj),
        dest_uri=str(target_url),
        source_table=source_table,
        dest_table=target_table,
    )
    if start_date is not None:
        ingest_kwargs["interval_start"] = start_date
    if batch_size is not None:
        ingest_kwargs["page_size"] = batch_size

    try:
        omniload.main.ingest(**ingest_kwargs)
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
