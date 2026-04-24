CREATE TABLE "{schema}".climate_data
(
    "timestamp"    TIMESTAMP WITHOUT TIME ZONE,
    "geo_location" GEO_POINT,
    "data"         OBJECT(DYNAMIC) AS (
        "temperature"  DOUBLE PRECISION,
        "u10"          DOUBLE PRECISION,
        "v10"          DOUBLE PRECISION,
        "pressure"     DOUBLE PRECISION,
        "latitude"     DOUBLE PRECISION,
        "longitude"    DOUBLE PRECISION,
        "humidity"     DOUBLE PRECISION
    )
);
