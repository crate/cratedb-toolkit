from cratedb_toolkit.datasets.model import Dataset
from cratedb_toolkit.datasets.store import registry

the_weather_dataset_daily = Dataset(
    title="The Weather Dataset: Daily Weather",
    reference="kaggle://guillemservera/global-daily-climate-data/daily_weather.parquet",
    documentation="https://www.kaggle.com/datasets/guillemservera/global-daily-climate-data",
    ddl="""
        CREATE TABLE IF NOT EXISTS {table} (
           "station_id" TEXT,
           "city_name" TEXT,
           "date" TIMESTAMP WITHOUT TIME ZONE,
           "season" TEXT,
           "avg_temp_c" REAL,
           "min_temp_c" REAL,
           "max_temp_c" REAL,
           "precipitation_mm" REAL,
           "snow_depth_mm" REAL,
           "avg_wind_dir_deg" REAL,
           "avg_wind_speed_kmh" REAL,
           "peak_wind_gust_kmh" REAL,
           "avg_sea_level_pres_hpa" REAL,
           "sunshine_total_min" REAL,
           INDEX city_name_ft using fulltext (city_name)
        )
        """,
)


the_weather_dataset_cities = Dataset(
    title="The Weather Dataset: Cities",
    reference="kaggle://guillemservera/global-daily-climate-data/cities.csv",
    documentation="https://www.kaggle.com/datasets/guillemservera/global-daily-climate-data",
    ddl="""
        CREATE TABLE IF NOT EXISTS {table} (
           "station_id" TEXT,
           "city_name" TEXT,
           "country" TEXT,
           "state" TEXT,
           "iso2" TEXT,
           "iso3" TEXT,
           "loc" GEO_POINT
        )
    """,
)


registry.add(the_weather_dataset_daily)
registry.add(the_weather_dataset_cities)
