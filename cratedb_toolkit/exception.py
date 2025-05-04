from click import ClickException


class TableNotFound(Exception):
    pass


class OperationFailed(Exception):
    pass


class CroudException(ClickException):
    pass


class DatabaseAddressMissingError(ClickException):
    STANDARD_MESSAGE = (
        "Missing database address, please specify cluster identifier, "
        "cluster name, or database URI in SQLAlchemy or HTTP URL format"
    )

    EXTENDED_MESSAGE = (
        f"{STANDARD_MESSAGE}. "
        "Use --cluster-id / --cluster-name / --cluster-url CLI options "
        "or CRATEDB_CLUSTER_ID / CRATEDB_CLUSTER_NAME / CRATEDB_CLUSTER_URL "
        "environment variables."
    )

    def __init__(self, message: str = None):
        if not message:
            message = self.EXTENDED_MESSAGE
        super().__init__(message)


class DatabaseAddressDuplicateError(ClickException):
    STANDARD_MESSAGE = (
        "Duplicate database address, please specify only one of: cluster id, cluster name, or database URL"
    )

    def __init__(self, message: str = None):
        if not message:
            message = self.STANDARD_MESSAGE
        super().__init__(message)
