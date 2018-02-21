# encoding: utf-8

import socket
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("port", help="Número de puerto")
parser.add_argument("--mensaje", help="Mensaje")
args = parser.parse_args()

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', int(args.port)))
print(s.recv(1024))

if args.mensaje is None:
    inpt = raw_input('Escribe cualquier cosa y pulsa Enter... ')
    s.send(inpt
else:
    s.send (args.mensaje)

print "El mensaje ha sido enviado"
