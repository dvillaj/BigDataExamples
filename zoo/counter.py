from kazoo.client import KazooClient
import logging
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('counter', help="Nombre del contador")
parser.add_argument('--value', help="Valor (Valor positivo o negativo)", default = "1")
args = parser.parse_args()

if args.counter is None:
    parser.error("Es necesario especificar el nombre del contador!")
    sys.exit(1)

if not isinstance(args.value, (int, long)):
    parser.error("El valor debe de ser un número!")
    sys.exit(1)

logging.basicConfig()
zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

counter = zk.Counter("/%s" % args.counter, default = 0)
counter += int(args.value)
print(counter.value)

zk.stop()