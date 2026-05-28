import logging

from boltons.urlutils import URL

logger = logging.getLogger(__name__)


def import_omniload():
    """Import omniload with CrateDB destination adapter."""
    try:
        import dlt_cratedb  # noqa: F401
        import omniload.main
        import omniload.src.factory
        from dlt.common.configuration import ConfigFieldMissingException
        from omniload.src.destinations import GenericSqlDestination

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

        omniload.src.factory.SourceDestinationFactory.destinations["cratedb"] = CrateDBDestination

        return True, omniload, ConfigFieldMissingException
    except ImportError:
        logger.error("Could not import omniload")
        return False, None, None
