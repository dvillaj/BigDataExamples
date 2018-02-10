# encoding: utf-8

import socket
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("port", help="Número de puerto")
parser.add_argument("--message", help="Mensaje")
args = parser.parse_args()

import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('localhost', int(args.port)))
print(s.recv(1024))

if args.message is None:
    inpt = raw_input('type anything and click enter... ')
    s.send(inpt)
else:
    s.send (args.message)

print "The message has been sent"
