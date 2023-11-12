from influxio.core import copy


def influxdb_copy(source_url, target_url, progress: bool = False):
    copy(source_url, target_url, progress=progress)
