# encoding: utf-8

from random_words import RandomWords
import argparse, random
from datetime import datetime
import socket, time
import json

parser = argparse.ArgumentParser()
parser.add_argument("n", help="Número de envíos", default = "1")
parser.add_argument("--words", help="Número de palabras en cada envío", default = "1")
parser.add_argument("--seed", help="Semilla aleatoria")
parser.add_argument("--json", help="Mensaje en formato json", action='store_true', default = False)
parser.add_argument('--server', help="Servidor", default = "localhost")
parser.add_argument('--port', help="Puerto", default = "9999")
parser.add_argument('--letter', help="Letra por la que empieza la palabra")
parser.add_argument('--delay', help="Tiempo entre palabras", default = "0.1")

args = parser.parse_args()

if not args.seed is None and args.seed.isdigit():
    random.seed(int(args.seed))
else:
    random.seed(datetime.now())

rw = RandomWords()

print("Enviando información al host %s:%s" % (args.server, args.port))
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((args.server, int(args.port)))
    for n in range(1, int(args.n) + 1):
        words = rw.random_words(letter = args.letter, count=int(args.words))
        if args.json:
            json_message = { 'words' : words,
                             'number' : random.randrange(1000) , 
                             'timestamp' : datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f'),
                             'n' : n
                            }
            message = json.dumps(json_message).encode('utf8')
        else:
            phrase = ' '.join(words)
            message = phrase.encode('utf8')
            
        s.send("%s\n" %  message)
        print("Mensaje: %s" % message)
        time.sleep(float(args.delay))

    s.close()
    print("Enviados %s mensajes!" % args.n)

except socket.error:
    print("Conexión rechazada. Puerto cerrado!" )

