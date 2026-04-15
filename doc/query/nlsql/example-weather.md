(nlsql-example-weather)=

# NLSQL with weather data

Let's use a basic database including weather observations.

:::{rubric} Provision
:::

Create table and insert data.

```sql
CREATE TABLE weather (zip_code VARCHAR, city VARCHAR, temperature_fahrenheit INTEGER, mean_visibility_miles INTEGER);

INSERT INTO weather (zip_code, city, temperature_fahrenheit, mean_visibility_miles) VALUES
('10001', 'New York', 85, 8),       -- visibility < 10
('90001', 'Los Angeles', 95, 12),   -- temp > 90
('60601', 'Chicago', 88, 9),        -- visibility < 10
('73301', 'Austin', 102, 15),       -- temp > 90
('94102', 'San Francisco', 65, 7),  -- visibility < 10
('85001', 'Phoenix', 110, 20),      -- temp > 90
('33101', 'Miami', 91, 11);         -- temp > 90
```

:::{rubric} Query
:::

Submit typical queries in human language.

```shell
ctk query nlsql "Find the zip code where the mean visibility is lower than 10."
ctk query nlsql "Find all cities with temperatures above 90°F."
```

:::{rubric} Response
:::

The model figures out the SQL statements, the engine runs it, and
uses the model again to come back with answers in human language:
```text
The zip codes with a mean visibility of less than 10 miles are 94102, 10001, and 60601.
```
```text
The cities with temperatures above 90°F are Miami, Austin, Phoenix, and Los Angeles.
```

The SQL statements were:
```sql
SELECT zip_code FROM weather WHERE mean_visibility_miles < 10;
```
```sql
SELECT city FROM weather WHERE temperature_fahrenheit > 90;
```
