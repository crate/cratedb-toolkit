CREATE TABLE IF NOT EXISTS "sys-operations" (
	id STRING, 
	job_id STRING, 
	name STRING, 
	node OBJECT, 
	started TIMESTAMP, 
	used_bytes LONG
);
