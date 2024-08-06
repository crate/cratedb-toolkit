# AWS DMS Processor

## About
[AWS Database Migration Service] (AWS DMS) is a managed migration and replication
service that helps move your database and analytics workloads quickly, securely,
and with minimal downtime and zero data loss.

The data migration pipeline supports one-shot full-load operations, and continuous
replication based on change data capture (CDC).

## Details
A full-load-and-CDC pipeline using AWS DMS and CrateDB will use [Amazon Kinesis]
Data Streams [as a DMS target], combined with a CrateDB-specific downstream
processor element.

## Coverage
AWS DMS supports migration between 20-plus database and analytics engines, either
on-premises, or per EC2 instance databases.

- Amazon Aurora
- Amazon DocumentDB
- Amazon S3
- IBM Db2 for Linux, UNIX, and Windows versions 9.7 and higher
- IBM Db2 for z/OS version 12
- MariaDB versions 10.0 and higher
- Microsoft Azure SQL Database
- Microsoft SQL Server versions 2005 and higher
- MongoDB versions 3.x and higher
- MySQL versions 5.5 and higher
- Oracle versions 10.2 and higher
- PostgreSQL versions 9.4 and higher
- SAP Adaptive Server Enterprise (ASE) versions 12.5 and higher

AWS DMS also supports the MySQL/MariaDB and PostgreSQL variants on AWS RDS,
Microsoft Azure, and Google Cloud. [Sources for AWS DMS] displays all the
compatibility details on one page.

## Usage
Depending on your needs and requirements, CrateDB and CrateDB Cloud support
different ways to configure AMS DMS using CrateDB as a CDC consolidation
database.
```{toctree}
:maxdepth: 2

standalone
managed
```


[Amazon Kinesis]: https://aws.amazon.com/kinesis/
[as a DMS target]: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.Kinesis.html
[AWS Database Migration Service]: https://aws.amazon.com/dms/
[Sources for AWS DMS]: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Introduction.Sources.html
