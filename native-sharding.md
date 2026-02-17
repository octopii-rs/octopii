so what should we provide vs what should be left for application to do ? 

we already have:
- auto dedup'ing transfer stuff
- CAS blob single node stuff


btw, I think its a VERY bad idea for PGs to change once they are initialized, 

yeah, so they are static, init at start, keep that in state, that's all

and the only thing we will keep are the placement groups, 

a placement group is just: a group of isolated nodes, that's it... and it should be static, so something like

get_placement_group(hash,size) should give you a deterministic PG 

so the thing is, octopii just stores the node ids in an array internally like, for n = 5, where n = no. of nodes

arr = [1,2,3,4,5]

so when a get_placement_group(hash,size) comes, we just randomly shuffle it taking hash as the seed (we want it to be deterministic)

then just collect result as arr[i%n], so if we get size = 7 in n = 5 (assume shuffling doesnt causes any change in this case), we just return:

[1,2,3,4,5,1,2] 

wdyt

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





 
