import io
import json
from contextlib import redirect_stdout


class PlatformInfo:
    @staticmethod
    def application():
        import platform

        from cratedb_toolkit import __appname__, __version__

        data = {}

        data["platform"] = platform.platform()
        data["version"] = __version__
        data["name"] = __appname__
        return data

    @staticmethod
    def libraries():
        data = {}

        # SQLAlchemy
        from importlib.metadata import entry_points

        try:
            import sqlalchemy.dialects.plugins
            import sqlalchemy.dialects.registry

            data["sqlalchemy"] = {
                "dialects_builtin": list(sqlalchemy.dialects.registry.impls.keys()),
                "dialects_3rdparty": [dialect.name for dialect in entry_points(group="sqlalchemy.dialects")],  # type: ignore[attr-defined,call-arg]
                "plugins": list(sqlalchemy.dialects.plugins.impls.keys()),
            }
        except Exception:  # noqa: S110
            pass

        # pandas
        try:
            import pandas

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                pandas.show_versions(as_json=True)
            buffer.seek(0)
            data["pandas"] = json.load(buffer)
        except Exception:  # noqa: S110
            pass

        # fsspec
        import fsspec

        data["fsspec"] = {"protocols": fsspec.available_protocols(), "compressions": fsspec.available_compressions()}

        return data
