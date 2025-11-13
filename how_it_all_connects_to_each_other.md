walrus octopii communication

walrus needs to be able to communicate with peers, that's all it is

basically these are the APIs it needs from octopii:

get_peers()
set_shared_data()
get_shared_data()

(I don't think we have this part yet, but should be trivial to implement, just a crossbeam channel would do)
sending shit to some peer(s) via RPC
receiving shit coming from peer(s) via RPC


yeah, that's all it is needed, and you'll have distributed walrus, that's it yeah
