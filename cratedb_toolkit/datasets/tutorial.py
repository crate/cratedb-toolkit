from cratedb_toolkit.datasets.model import Dataset
from cratedb_toolkit.datasets.store import registry

CDN_BASEURL = "https://cdn.crate.io/downloads/datasets/cratedb-datasets"

chicago_311_records = Dataset(
    reference="tutorial/chicago-311-records",
    data_url=f"{CDN_BASEURL}/academy/chicago-data/311_records_apr_2024.json.gz",
    init_url=f"{CDN_BASEURL}/academy/chicago-data/311_records_load.sql",
    init_includes_loading=True,
)

chicago_community_areas = Dataset(
    reference="tutorial/chicago-community-areas",
    data_url=f"{CDN_BASEURL}/academy/chicago-data/chicago_community_areas_with_vectors.json",
    init_url=f"{CDN_BASEURL}/academy/chicago-data/chicago_community_areas_with_vectors_load.sql",
    init_includes_loading=True,
)

chicago_libraries = Dataset(
    reference="tutorial/chicago-libraries",
    data_url=f"{CDN_BASEURL}/academy/chicago-data/chicago_libraries.json",
    init_url=f"{CDN_BASEURL}/academy/chicago-data/chicago_libraries_load.sql",
    init_includes_loading=True,
)

chicago_taxi_vehicles = Dataset(
    reference="tutorial/chicago-taxi-vehicles",
    data_url=f"{CDN_BASEURL}/academy/chicago-data/taxi_details.csv",
    init_url=f"{CDN_BASEURL}/academy/chicago-data/taxi_details_load.sql",
    init_includes_loading=True,
)

chicago_taxi_rides = Dataset(
    reference="tutorial/chicago-taxi-rides",
    data_url=f"{CDN_BASEURL}/academy/chicago-data/taxi_rides_apr_2024.json.gz",
    init_url=f"{CDN_BASEURL}/academy/chicago-data/taxi_rides_load.sql",
    init_includes_loading=True,
)

chicago_weather_stations = Dataset(
    reference="tutorial/chicago-weather-stations",
    data_url=None,
    init_url=f"{CDN_BASEURL}/academy/chicago-data/beach_weather_stations_load.sql",
    init_includes_loading=True,
)

chicago_weather_data = Dataset(
    reference="tutorial/chicago-weather-data",
    data_url=f"{CDN_BASEURL}/academy/chicago-data/beach_weather_station_data.csv",
    init_url=f"{CDN_BASEURL}/academy/chicago-data/beach_weather_station_data_load.sql",
    init_includes_loading=True,
)

devices_info = Dataset(
    reference="tutorial/devices-info",
    data_url=f"{CDN_BASEURL}/cloud-tutorials/devices_info.json.gz",
    init_url=f"{CDN_BASEURL}/cloud-tutorials/load_devices_info.sql",
    init_includes_loading=True,
)

devices_readings = Dataset(
    reference="tutorial/devices-readings",
    data_url=f"{CDN_BASEURL}/cloud-tutorials/devices_readings.json.gz",
    init_url=f"{CDN_BASEURL}/cloud-tutorials/load_devices_readings.sql",
    init_includes_loading=True,
)

marketing = Dataset(
    reference="tutorial/marketing",
    data_url=f"{CDN_BASEURL}/cloud-tutorials/data_marketing.json.gz",
    init_url=f"{CDN_BASEURL}/cloud-tutorials/load_marketing.sql",
    init_includes_loading=True,
)

netflix = Dataset(
    reference="tutorial/netflix",
    data_url=f"{CDN_BASEURL}/cloud-tutorials/data_netflix.json.gz",
    init_url=f"{CDN_BASEURL}/cloud-tutorials/load_netflix.sql",
    init_includes_loading=True,
)

weather = Dataset(
    reference="tutorial/weather-basic",
    data_url=f"{CDN_BASEURL}/cloud-tutorials/data_weather.csv.gz",
    init_url=f"{CDN_BASEURL}/cloud-tutorials/load_weather.sql",
    init_includes_loading=True,
)

windfarm_uk_info = Dataset(
    reference="tutorial/windfarm-uk-info",
    data_url=f"{CDN_BASEURL}/devrel/uk-offshore-wind-farm-data/wind_farms.json",
    init_url=f"{CDN_BASEURL}/devrel/uk-offshore-wind-farm-data/wind_farms_load.sql",
    init_includes_loading=True,
)

windfarm_uk_data = Dataset(
    reference="tutorial/windfarm-uk-data",
    data_url=f"{CDN_BASEURL}/devrel/uk-offshore-wind-farm-data/wind_farm_output.json.gz",
    init_url=f"{CDN_BASEURL}/devrel/uk-offshore-wind-farm-data/wind_farm_output_load.sql",
    init_includes_loading=True,
)

registry.add(chicago_community_areas)
registry.add(chicago_311_records)
registry.add(chicago_libraries)
registry.add(chicago_taxi_vehicles)
registry.add(chicago_taxi_rides)
registry.add(chicago_weather_stations)
registry.add(chicago_weather_data)
registry.add(devices_info)
registry.add(devices_readings)
registry.add(marketing)
registry.add(netflix)
registry.add(weather)
registry.add(windfarm_uk_info)
registry.add(windfarm_uk_data)
