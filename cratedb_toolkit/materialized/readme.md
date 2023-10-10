# Materialized Views Baseline Infrastructure

## About

This subsystem provides a foundation for emulating materialized views in CrateDB.
It addresses the need to optimize query performance by caching the results of
complex or resource-intensive SQL queries in regular tables that can be refreshed
on a scheduled basis.

The subsystem emulates materialized views so that queries that take a long
time to run can be cached, which is specifically useful when applied in
scenarios with high traffic in reads. This approach can significantly reduce
the database load and improve response times for frequently accessed data.

## Features

- Create and manage materialized view definitions
- Refresh materialized views on demand or on schedule
- Track metadata about materialized views for management purposes
- Support for different refresh strategies

## Prior Art

- https://github.com/nroi/elfenbein
- https://github.com/nroi/pg_materialized_views_refresh_topologically
- https://github.com/darkside/monocle
- https://github.com/maggregor/maggregor
- https://github.com/adamfoneil/ViewMaterializer
- https://github.com/jhollinger/activerecord-viewmatic
- https://github.com/q-m/metabase-matview
