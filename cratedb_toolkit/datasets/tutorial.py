from cratedb_toolkit.datasets.model import Dataset
from cratedb_toolkit.datasets.store import registry

devices_info = Dataset(
    reference="tutorial/devices-info",
    data_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/devices_info.json.gz",
    init_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/load_devices_info.sql",
    init_includes_loading=True,
)

devices_readings = Dataset(
    reference="tutorial/devices-readings",
    data_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/devices_readings.json.gz",
    init_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/load_devices_readings.sql",
    init_includes_loading=True,
)

marketing = Dataset(
    reference="tutorial/marketing",
    data_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_marketing.json.gz",
    init_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/load_marketing.sql",
    init_includes_loading=True,
)

netflix = Dataset(
    reference="tutorial/netflix",
    data_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_netflix.json.gz",
    init_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/load_netflix.sql",
    init_includes_loading=True,
)

weather = Dataset(
    reference="tutorial/weather-basic",
    data_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_weather.csv.gz",
    init_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/load_weather.sql",
    init_includes_loading=True,
)

registry.add(devices_info)
registry.add(devices_readings)
registry.add(marketing)
registry.add(netflix)
registry.add(weather)
