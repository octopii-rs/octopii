so what should we provide vs what should be left for application to do ? 

we already have:
- auto dedup'ing transfer stuff
- CAS blob single node stuff

## sharded KV store

forget replication for now, just shard the darn data and call it a day



## object store 

so it would just store object metadata in the shardedKV store, then from that size, get the PG deterministically, 
and that's about it
(for now let's just say object metadata just has the size of object in bytes, no versioning or anything like that)

so a GET(object_path) would look like:

- get object metadata from shardedKV
- determine PG from that metadata
- get the actual chunks
- merge and return

and a PUT(object_path,blob) would look like:
(this is NOT thread safe right now btw)

- get PG based off path hash or something
- put the chunks in blobStore
- put object metadata in shardedKV
- done





 
