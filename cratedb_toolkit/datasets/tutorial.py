from cratedb_toolkit.datasets.model import Dataset
from cratedb_toolkit.datasets.store import registry

devices_info = Dataset(
    name="tutorial/devices-info",
    data_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/devices_info.json.gz",
    load_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/load_devices_info.sql",
)

devices_readings = Dataset(
    name="tutorial/devices-readings",
    data_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/devices_readings.json.gz",
    load_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/load_devices_readings.sql",
)

marketing = Dataset(
    name="tutorial/marketing",
    data_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_marketing.json.gz",
    load_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/load_marketing.sql",
)

netflix = Dataset(
    name="tutorial/netflix",
    data_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_netflix.json.gz",
    load_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/load_netflix.sql",
)

weather = Dataset(
    name="tutorial/weather-basic",
    data_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/data_weather.csv.gz",
    load_url="https://github.com/crate/cratedb-datasets/raw/main/cloud-tutorials/load_weather.sql",
)

registry.add(devices_info)
registry.add(devices_readings)
registry.add(marketing)
registry.add(netflix)
registry.add(weather)
