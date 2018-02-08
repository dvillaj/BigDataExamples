from kazoo.client import KazooClient
import logging

logging.basicConfig()
zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

counter = zk.Counter("/int", default = 100)
counter += 2
counter -= 1
print(counter.value)

zk.stop()