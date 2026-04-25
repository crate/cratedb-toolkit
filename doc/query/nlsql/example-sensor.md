(nlsql-example-sensor)=

# NLSQL with sensor data

Let's use a single `time_series_data` database table
and populate it with a few records worth of time series data.

:::{rubric} Provision
:::

Create table and insert data.

```sql
CREATE TABLE IF NOT EXISTS time_series_data (
    timestamp TIMESTAMP,
    value DOUBLE,
    location STRING,
    sensor_id INT
);

INSERT INTO time_series_data (timestamp, value, location, sensor_id)
VALUES
    ('2023-09-14T00:00:00', 10.5, 'Sensor A', 1),
    ('2023-09-14T01:00:00', 15.2, 'Sensor A', 1),
    ('2023-09-14T02:00:00', 18.9, 'Sensor A', 1),
    ('2023-09-14T03:00:00', 12.7, 'Sensor B', 2),
    ('2023-09-14T04:00:00', 17.3, 'Sensor B', 2),
    ('2023-09-14T05:00:00', 20.1, 'Sensor B', 2),
    ('2023-09-14T06:00:00', 22.5, 'Sensor A', 1),
    ('2023-09-14T07:00:00', 18.3, 'Sensor A', 1),
    ('2023-09-14T08:00:00', 16.8, 'Sensor A', 1),
    ('2023-09-14T09:00:00', 14.6, 'Sensor B', 2),
    ('2023-09-14T10:00:00', 13.2, 'Sensor B', 2),
    ('2023-09-14T11:00:00', 11.7, 'Sensor B', 2);

REFRESH TABLE time_series_data;
```

:::{rubric} Query
:::

Submit a typical query in human language.

```shell
ctk query nlsql "What is the average value for sensor 1?"
```

:::{rubric} Response
:::

The model figures out the SQL statement, the engine runs it, and
uses the model again to come back with an answer in human language:
```text
The average value for sensor 1 is approximately 17.03.
```

The SQL statement was:
```sql
SELECT AVG(value) FROM time_series_data WHERE sensor_id = 1;
```

:::{rubric} Multiple languages
:::

The NLSQL conversation works well in multiple languages.

> Q: ¿Cuál es el valor medio del sensor 1?
>
> A: El valor medio del sensor 1 es 17.0333.

> Q: Quelle est la valeur moyenne du capteur 1 ?
>
> A: La valeur moyenne du capteur 1 est de 17,0333.

> Q: What is the average value for sensor 1?
>
> A: The average value for sensor 1 is approximately 17.03.

> Q: Wie lautet der Durchschnittswert für Sensor 1?
>
> A: Der Durchschnittswert für Sensor 1 beträgt 17,0333.

> Q: Qual è il valore medio del sensore 1?
>
> A: Il valore medio del sensore 1 è pari a 17,0333.
