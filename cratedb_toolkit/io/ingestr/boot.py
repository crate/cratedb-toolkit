import logging
from unittest import mock

from boltons.urlutils import URL

logger = logging.getLogger(__name__)


def import_ingestr():
    """Import ingestr with CrateDB destination adapter."""
    try:
        with mock.patch("ingestr.src.telemetry.event.track"), mock.patch(
            "dlt.common.runtime.telemetry._TELEMETRY_STARTED", True
        ):
            import dlt_cratedb  # noqa: F401
            import ingestr.main
            import ingestr.src.factory
            from dlt.common.configuration import ConfigFieldMissingException
            from ingestr.src.destinations import GenericSqlDestination

            class CrateDBDestination(GenericSqlDestination):
                def dlt_dest(self, uri: str, **kwargs):
                    uri = self._replace_url(uri)
                    import dlt_cratedb.impl.cratedb.factory

                    return dlt_cratedb.impl.cratedb.factory.cratedb(credentials=uri, **kwargs)

                @staticmethod
                def _replace_url(uri: str) -> str:
                    url_obj = URL(uri)
                    if url_obj.scheme == "cratedb":
                        url_obj.scheme = "postgres"
                    return str(url_obj)

            ingestr.src.factory.SourceDestinationFactory.destinations["cratedb"] = CrateDBDestination

            return True, ingestr, ConfigFieldMissingException
    except ImportError:
        logger.error("Could not import ingestr")
        return False, None, None
