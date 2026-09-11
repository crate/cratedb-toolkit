CREATE TABLE IF NOT EXISTS "sys-segments" (
	attributes OBJECT, 
	committed BOOLEAN, 
	compound BOOLEAN, 
	deleted_docs INT, 
	fully_merged_docs INT, 
	generation LONG, 
	memory LONG, 
	merge_id VARCHAR, 
	node OBJECT, 
	num_docs INT, 
	partition_ident VARCHAR, 
	"primary" BOOLEAN, 
	search BOOLEAN, 
	segment_name VARCHAR, 
	shard_id INT, 
	size LONG, 
	table_name VARCHAR, 
	table_schema VARCHAR, 
	version VARCHAR
);

